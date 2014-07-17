package org.mds.hprocessor.processor;

/**
 * Created by Randall.mo on 14-4-22.
 */
public interface ProcessorHandler<T> {
    public void process(T object);
}
