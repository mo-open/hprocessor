package org.mds.hprocessor.memcache;

import com.google.common.base.Preconditions;
import org.mds.hprocessor.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Randall.mo
 */
public class MemcacheSetProcessor extends MemcacheProcessor {
    private static final Logger log = LoggerFactory.getLogger(MemcacheSetProcessor.class);
    protected final static int DEFAULT_BATCH_SIZE = 256;
    protected final static int MIN_BATCH_SIZE = 16;
    private Processor<SetObject> setterProcessor;
    protected Processor<Object> callbackProcessor;
    private MemCache memcacheSetter;

    protected static class CallbackObject {
        List<SetObject> objects;
        boolean exception = false;

        public CallbackObject(List<SetObject> objects, boolean exception) {
            this.objects = objects;
            this.exception = exception;
        }

        public void release() {
            if (this.objects != null) {
                this.objects.clear();
                this.objects = null;
            }
        }
    }

    private class SetObject {
        public SetObject(String key, int exp, Object value) {
            this(key, exp, value, null);
        }

        public SetObject(String key, int exp, Object value, SetCallback callback) {
            this.key = key;
            this.value = value;
            this.exp = exp;
            this.callback = callback;
        }

        String key;
        int exp;
        Object value;
        boolean completed = false;
        long submitTime = System.currentTimeMillis();
        SetCallback callback;

        public boolean isTimeout() {
            return getTimeUnit.convert(System.currentTimeMillis() - submitTime,
                    TimeUnit.MILLISECONDS) > getTimeout;
        }

        public void release() {
            this.key = null;
            this.value = null;
            this.callback = null;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends ProcessorBuilder<SetObject, SetObject, Builder, MemcacheSetProcessor> {
        int batchSize = DEFAULT_BATCH_SIZE;
        MemCache[] setters;
        boolean async = true;


        public Builder setSetters(MemCache[] setters) {
            return this.setSetters(0, setters);
        }

        public Builder setAsync(boolean async) {
            this.async = async;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            if (batchSize > MIN_BATCH_SIZE)
                this.batchSize = batchSize;
            return this;
        }

        public Builder setSetters(int workerCount, MemCache[] setters) {
            Preconditions.checkArgument(setters != null && setters.length > 0, "Setters can not be empty.");
            if (setters.length > workerCount) {
                this.setters = setters;
            } else {
                this.setters = new MemCache[workerCount];
                Random random = new Random();
                for (int i = 0; i < setters.length; i++) {
                    this.setters[i] = setters[i];
                }
                for (int i = setters.length; i < workerCount; i++) {
                    this.setters[i] = setters[random.nextInt(setters.length)];
                }
            }
            this.setters = setters;
            return this;
        }

        public MemcacheSetProcessor build() {
            Preconditions.checkArgument(this.setters != null && this.setters.length > 0, "Setters must be set.");
            MemcacheSetProcessor memcacheSetProcessor = new MemcacheSetProcessor();
            memcacheSetProcessor.memcacheSetter = setters[0];
            this.setTimeout(memcacheSetProcessor);
            Processor.BatchBuilder batchBuilder = this.builder();
            Processor.SingleBuilder callbackProcessorBuilder = this.callbackBuilder();

            if (batchBuilder != null) {
                if (!memcacheSetProcessor.memcacheSetter.supportMultiSet()) {
                    ProcessorHandler<SetObject>[] handlers = new ProcessorHandler[setters.length];
                    for (int i = 0; i < setters.length; i++) {
                        handlers[i] = new SetProcessorHandler(setters[i], memcacheSetProcessor, this.async);
                    }
                    memcacheSetProcessor.setterProcessor = batchBuilder.addNext(handlers).build();
                    memcacheSetProcessor.callbackProcessor = callbackProcessorBuilder
                            .addNext(this.callbackWorkers, new CallbackProcessorHandler(memcacheSetProcessor))
                            .build();
                } else {
                    ProcessorBatchHandler<SetObject>[] handlers = new SetMultiProcessorHandler[setters.length];
                    for (int i = 0; i < setters.length; i++) {
                        handlers[i] = new SetMultiProcessorHandler(setters[i], memcacheSetProcessor);
                    }
                    memcacheSetProcessor.setterProcessor = batchBuilder.addNext(this.batchSize, handlers).build();
                    memcacheSetProcessor.callbackProcessor = callbackProcessorBuilder
                            .addNext(this.callbackWorkers, new CallbackSetMultiProcessorHandler(memcacheSetProcessor))
                            .build();
                }
            }

            return memcacheSetProcessor;
        }
    }

    private MemcacheSetProcessor() {
    }

    private static class CallbackProcessorHandler implements ProcessorHandler<SetObject> {
        private MemcacheSetProcessor memcacheSetProcessor;

        public CallbackProcessorHandler(MemcacheSetProcessor memcacheSetProcessor) {
            this.memcacheSetProcessor = memcacheSetProcessor;
        }

        @Override
        public void process(SetObject object) {
            if (object.callback == null) {
                object.release();
                return;
            }
            if (object.completed) {
                object.callback.complete(object.key, object.value);
            } else {
                if (!object.isTimeout()) {
                    this.memcacheSetProcessor.submit(object);
                    return;
                }
                object.callback.timeout(object.key, object.value);
            }
            object.release();
        }
    }

    private static class CallbackSetMultiProcessorHandler implements ProcessorHandler<CallbackObject> {
        private MemcacheSetProcessor memcacheSetProcessor;

        public CallbackSetMultiProcessorHandler(MemcacheSetProcessor memcacheSetProcessor) {
            this.memcacheSetProcessor = memcacheSetProcessor;
        }

        @Override
        public void process(CallbackObject object) {
            for (SetObject setObject : object.objects) {
                try {
                    if (object.exception && !setObject.isTimeout()) {
                        this.memcacheSetProcessor.submit(setObject);
                        continue;
                    }
                    if (setObject.callback != null) {
                        if (setObject.completed) {
                            setObject.callback.complete(setObject.key, setObject.value);
                        } else {
                            setObject.callback.timeout(setObject.key, setObject.value);
                        }
                    }
                    setObject.release();
                } catch (Exception ex) {
                    log.warn("Failed to handle the result of key:{}:{} ", setObject.key, ex);
                }
            }
            object.release();
        }
    }

    private static class SetMultiProcessorHandler implements ProcessorBatchHandler<SetObject> {
        private MemCache memcacheSetter;
        private MemcacheSetProcessor memcacheSetProcessor;

        public SetMultiProcessorHandler(MemCache memcacheSetter, MemcacheSetProcessor memcacheSetProcessor) {
            this.memcacheSetter = memcacheSetter;
            this.memcacheSetProcessor = memcacheSetProcessor;
        }

        @Override
        public void process(List<SetObject> objects) {
            if (objects.isEmpty()) return;
            int exp = objects.get(0).exp;
            Map<String, Object> values = new HashMap<>();
            for (SetObject setObject : objects) {
                values.put(setObject.key, setObject.value);
            }
            Map<String, Object> result = null;
            boolean exception = false;
            try {
                this.memcacheSetter.setMulti(values, exp);
            } catch (Throwable ex) {
                log.warn("Exception in getMulti from memcache:" + ex);
                exception = true;
            } finally {
                if (result != null) {
                    this.memcacheSetProcessor.callbackProcessor.submit(new CallbackObject(objects, exception));
                } else {
                    objects.clear();
                }
            }
        }
    }

    private static class SetProcessorHandler implements ProcessorHandler<SetObject> {
        private MemCache memcacheSetter;
        private MemcacheSetProcessor memcacheSetProcessor;
        private boolean async = true;

        public SetProcessorHandler(MemCache memcacheSetter,
                                   MemcacheSetProcessor memcacheSetProcessor,
                                   boolean async) {
            this.memcacheSetProcessor = memcacheSetProcessor;
            this.memcacheSetter = memcacheSetter;
            this.async = async;
        }

        @Override
        public void process(SetObject object) {
            try {
                if (this.async) {
                    this.memcacheSetter.set(object.key, object.exp, object.value);
                } else {
                    this.memcacheSetter.syncSet(object.key, object.exp, object.value);
                }
                object.completed = true;
            } catch (Throwable ex) {
                log.warn("Failed to set memcache, key: {}, error:{}", object.key, ex);
            } finally {
                if (object.callback != null) {
                    this.memcacheSetProcessor.callbackProcessor.submit(object);
                } else {
                    object.release();
                }
            }
        }
    }

    private void submit(SetObject setObject) {
        this.setterProcessor.submit(setObject);
    }

    public boolean set(String key, int exp, Object value) {
        return this.setterProcessor.trySubmit(new SetObject(key, exp, value),
                this.submitTimeout, this.submitTimeUnit);
    }

    public void syncSet(String key, int exp, Object value) {
        this.memcacheSetter.syncSet(key, exp, value);
    }

    public void set(String key, int exp, Object value, SetCallback callback) {
        if (!this.setterProcessor.trySubmit(new SetObject(key, exp, value, callback)
                , this.submitTimeout, this.submitTimeUnit)) {
            if (callback != null) {
                callback.timeout(key, value);
            }
        }
    }

    public interface SetCallback<T> {
        public void complete(String key, T value);

        public void timeout(String key, T value);
    }
}
