package org.mds.hprocessor.processor;

/**
 * Created by Randall.mo on 14-6-9.
 */
public interface ProcessorFactory<T> {
    public Processor<T> newProcessor(ProcessorHandler<T> processorHandler);
}
