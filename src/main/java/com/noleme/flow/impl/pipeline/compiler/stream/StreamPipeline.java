package com.noleme.flow.impl.pipeline.compiler.stream;

import com.noleme.flow.compiler.NotImplementedException;
import com.noleme.flow.node.AbstractNode;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamGenerator;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamPipeline extends AbstractNode
{
    private final StreamGenerator<?, ?> generatorNode;
    private final List<Node> nodes;
    private StreamPipeline topParent;
    private String pivot;
    private Set<String> potentialPivots;

    /**
     *
     * @param generatorNode
     */
    public StreamPipeline(StreamGenerator<?, ?> generatorNode, StreamPipeline topParent)
    {
        super();
        this.generatorNode = generatorNode;
        this.topParent = topParent;
        this.nodes = new ArrayList<>();
        this.setDepth(generatorNode.getDepth());
    }

    public StreamPipeline(StreamGenerator<?, ?> generatorNode)
    {
        this(generatorNode, null);
        this.topParent = this;
    }

    @Override
    public List<Node> getUpstream()
    {
        return this.generatorNode.getUpstream();
    }

    @Override
    public List<Node> getRequirements()
    {
        return this.generatorNode.getRequirements();
    }

    @Override
    public Node after(Node other)
    {
        throw new NotImplementedException();
    }

    @Override
    public Node after(Collection<Node> others)
    {
        throw new NotImplementedException();
    }

    public StreamGenerator getGeneratorNode()
    {
        return this.generatorNode;
    }

    public StreamPipeline add(Node node)
    {
        this.nodes.add(node);
        return this;
    }

    public List<Node> getNodes()
    {
        return this.nodes;
    }

    public StreamPipeline getTopParent()
    {
        return this.topParent;
    }

    public StreamPipeline setPotentialPivots(Set<String> pivots)
    {
        this.potentialPivots = pivots;
        return this;
    }

    public Set<String> getPotentialPivots()
    {
        return this.potentialPivots;
    }

    public StreamPipeline setPivot(String pivot)
    {
        this.pivot = pivot;
        return this;
    }

    public String getPivot()
    {
        return this.pivot;
    }
}
