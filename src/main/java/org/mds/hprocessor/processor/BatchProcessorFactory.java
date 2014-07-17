package org.mds.hprocessor.processor;

/**
 * Created by Randall.mo on 14-6-9.
 */
public interface BatchProcessorFactory<T> {
    public Processor<T> newProcessor(ProcessorBatchHandler<T> processorBatchHandler);
}
