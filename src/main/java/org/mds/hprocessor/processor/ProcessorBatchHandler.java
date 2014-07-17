package org.mds.hprocessor.processor;

import java.util.List;

/**
 * @author Randall.mo
 */
public interface ProcessorBatchHandler<T> {
    public void process(List<T> objects);
}
