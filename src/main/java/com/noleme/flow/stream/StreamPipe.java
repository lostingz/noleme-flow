package com.noleme.flow.stream;

import com.noleme.flow.FlowOut;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.interruption.Interruption;
import com.noleme.flow.node.SimpleNode;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public class StreamPipe<I, O> extends SimpleNode<Transformer<I, O>> implements StreamIn<I>, StreamOut<O>, StreamNode
{
    /**
     *
     * @param transformer
     * @param depth
     */
    public StreamPipe(Transformer<I, O> transformer, int depth)
    {
        super(transformer);
        this.setDepth(depth);
    }

    @Override
    public <NO> StreamPipe<O, NO> into(Transformer<O, NO> transformer)
    {
        var pipe = new StreamPipe<>(transformer, this.depth);
        this.bind(pipe);
        return pipe;
    }

    @Override
    public StreamSink<O> into(Loader<O> loader)
    {
        var sink = new StreamSink<>(loader, this.depth);
        this.bind(sink);
        return sink;
    }

    @Override
    public <JI, JO> StreamJoin<O, JI, JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer)
    {
        return new StreamJoin<>(this, input, transformer, this.depth);
    }

    @Override
    public <N> StreamGenerator<O, N> stream(Function<O, Generator<N>> generatorSupplier)
    {
        var pipe = new StreamGenerator<>(generatorSupplier, this.depth + 1);
        this.bind(pipe);
        return pipe;
    }

    @Override
    public <N> StreamAccumulator<O, N> accumulate(Accumulator<O, N> accumulator)
    {
        var acc = new StreamAccumulator<>(accumulator, this.depth - 1);
        this.bind(acc);
        return acc;
    }

    /**
     *
     * @param loader
     * @return
     */
    public StreamPipe<I, O> driftSink(Loader<O> loader)
    {
        this.into(loader);
        return this;
    }


    /**
     *
     * @return
     */
    public StreamPipe<O, O> interrupt()
    {
        return this.into(new Interruption<>());
    }

    /**
     *
     * @param predicate
     * @return
     */
    public StreamPipe<O, O> interruptIf(Predicate<O> predicate)
    {
        return this.into(new Interruption<>(predicate));
    }

    /**
     *
     * @param name
     * @return
     */
    public StreamPipe<I, O> name(String name)
    {
        this.name = name;
        return this;
    }
}
