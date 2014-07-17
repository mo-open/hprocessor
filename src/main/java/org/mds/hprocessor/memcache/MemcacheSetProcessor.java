package org.mds.hprocessor.memcache;

import com.google.common.base.Preconditions;
import org.mds.hprocessor.processor.*;

import java.util.Random;

/**
 * @author Randall.mo
 */
public class MemcacheSetProcessor extends MemcacheProcessor {

    private Processor<SetObject> setterProcessor;
    private MemcacheSetter memcacheSetter;

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
        SetCallback callback;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends ProcessorBuilder<SetObject, Builder, MemcacheSetProcessor> {
        MemcacheSetter[] setters;
        boolean async = true;

        public Builder setSetters(MemcacheSetter[] setters) {
            return this.setSetters(0, setters);
        }

        public Builder setAsync(boolean async) {
            this.async = async;
            return this;
        }

        public Builder setSetters(int workerCount, MemcacheSetter[] setters) {
            Preconditions.checkArgument(setters != null && setters.length > 0, "Setters can not be empty.");
            if (setters.length > workerCount) {
                this.setters = setters;
            } else {
                this.setters = new MemcacheSetter[workerCount];
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
            ProcessorHandler<SetObject>[] handlers = new ProcessorHandler[setters.length];
            for (int i = 0; i < setters.length; i++) {
                handlers[i] = new SetProcessorHandler(setters[i],this.async);
            }
            memcacheSetProcessor.memcacheSetter = setters[0];

            this.setTimeout(memcacheSetProcessor);
            Processor.BatchBuilder batchBuilder = this.builder();
            if (batchBuilder != null) {
                memcacheSetProcessor.setterProcessor = batchBuilder.addNext(handlers).build();
            }

            return memcacheSetProcessor;
        }
    }

    private MemcacheSetProcessor() {
    }

    private static class SetProcessorHandler implements ProcessorHandler<SetObject> {
        private MemcacheSetter memcacheSetter;
        private boolean async = true;

        public SetProcessorHandler(MemcacheSetter memcacheSetter, boolean async) {
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
                if (object.callback != null) {
                    object.callback.complete(object.key);
                }
            } catch (Exception ex) {
                if (object.callback != null) {
                    object.callback.fail(object.key);
                }
            }
        }
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
                callback.timeout(key);
            }
        }
    }

    public interface SetCallback {
        public void complete(String key);

        public void fail(String key);

        public void timeout(String key);
    }
}
