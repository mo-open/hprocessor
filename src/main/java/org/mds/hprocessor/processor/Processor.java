package org.mds.hprocessor.processor;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * Created by Randall.mo on 14-4-22.
 */
public interface Processor<T> extends Closeable {
    public int bufferSize();
    public void submit(T object);

    public boolean trySubmit(T object);

    public boolean trySubmit(T object, long timeout, TimeUnit timeUnit);

    public interface IBaseBuilder<T extends Processor> {
        public T build();
    }

    public interface ISingleBuilder<T0 extends Object, T1 extends ISingleBuilder<T0, T1, ?>, T2 extends Processor<T0>>
            extends IBaseBuilder<T2> {
        public T1 addNext(int workerCount, ProcessorHandler<T0> handler);

        public T1 addNext(ProcessorHandler<T0>[] handlers);
    }

    public interface IBatchBuilder<T0 extends Object, T1 extends IBatchBuilder<T0, T1, ?>, T2 extends Processor<T0>>
            extends IBaseBuilder<T2> {
        public T1 addNext(int workerCount, int batchSize, ProcessorBatchHandler<T0> batchHandler);

        public T1 addNext(int batchSize, ProcessorBatchHandler<T0>[] batchHandlers);
    }

    public static abstract class SingleBuilder<T0, T1 extends SingleBuilder<T0, T1, ? extends Processor<T0>>,
            T2 extends Processor<T0>> implements ISingleBuilder<T0, T1, T2> {
        protected int bufferSize = 1024 * 8;

        public T1 setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return (T1) this;
        }
    }

    public static abstract class BatchBuilder<T0, T1 extends BatchBuilder<T0, T1, ? extends Processor<T0>>,
            T2 extends Processor<T0>> extends SingleBuilder<T0, T1, T2> implements
            IBatchBuilder<T0, T1, T2> {
    }
}
