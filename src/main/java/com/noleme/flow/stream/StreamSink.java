package com.noleme.flow.stream;

import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.node.SimpleNode;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public class StreamSink<I> extends SimpleNode<Loader<I>> implements StreamIn<I>, StreamNode
{
    /**
     *
     * @param actor
     * @param depth
     */
    public StreamSink(Loader<I> actor, int depth)
    {
        super(actor);
        this.setDepth(depth);
    }

    /**
     *
     * @param name
     * @return
     */
    public StreamSink<I> name(String name)
    {
        this.name = name;
        return this;
    }
}
