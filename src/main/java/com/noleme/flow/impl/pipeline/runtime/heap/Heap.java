package com.noleme.flow.impl.pipeline.runtime.heap;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingKey;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingNode;
import com.noleme.flow.io.output.Output;
import com.noleme.flow.stream.StreamGenerator;

import java.util.Collection;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/07/2015.
 */
public interface Heap
{
    /**
     *
     * @param key
     * @param returnValue
     * @param counter
     */
    Heap push(WorkingKey key, Object returnValue, int counter);

    /**
     *
     * @param key
     * @return
     */
    boolean has(WorkingKey key);

    /**
     *
     * @param key
     * @return
     */
    Object peek(WorkingKey key);

    /**
     *
     * @param key
     * @return
     */
    Object consume(WorkingKey key);

    /* Stream related methods */

    /**
     *
     * @param node
     * @return
     */
    @SuppressWarnings("rawtypes")
    Generator getStreamGenerator(WorkingNode<StreamGenerator> node);

    /**
     *
     * @param node
     * @return
     */
    @SuppressWarnings("rawtypes")
    long getNextStreamOffset(WorkingNode<StreamGenerator> node);

    /**
     *
     * @param key
     * @return
     */
    Collection<Object> consumeAll(WorkingKey key);

    /**
     *
     * @param identifier
     * @return
     */
    boolean hasInput(String identifier);

    /**
     *
     * @param identifier
     * @return
     */
    Object getInput(String identifier);

    /**
     *
     * @param identifier
     * @param value
     * @return
     */
    Heap setOutput(String identifier, Object value);

    /**
     *
     * @return
     */
    Output getOutput();

    default String dump() {
        return this.toString();
    }
}
