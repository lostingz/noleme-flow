package com.noleme.flow.impl.pipeline.compiler.pass;

import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.impl.pipeline.compiler.stream.StreamPipeline;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamAccumulator;
import com.noleme.flow.stream.StreamGenerator;
import com.noleme.flow.stream.StreamNode;
import com.noleme.flow.stream.StreamOut;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/13
 */
public class StreamAggregationPass implements PipelineCompilerPass
{
    @Override
    public Collection<Node> run(Collection<Node> nodes) throws CompilationException
    {
        Registry registry = new Registry();

        LinkedList<Node> queue = new LinkedList<>(nodes);

        while (!queue.isEmpty())
        {
            Node node = queue.poll();

            if (node instanceof StreamGenerator && !registry.contains(node))
            {
                var pipeline = new StreamPipeline((StreamGenerator<?, ?>) node);
                this.compileStream(node, pipeline, null, registry);
                queue.push(pipeline);
            }
            else if (registry.contains(node))
                continue;
            else if (node instanceof StreamPipeline && registry.isPipelineUpstreamSatisfied((StreamPipeline) node))
                registry.add(node);
            else if (!(node instanceof StreamPipeline) && registry.isUpstreamSatisfied(node))
                registry.add(node);
            else
                queue.add(node);
        }

        return registry.nodes();
    }

    private void compileStream(Node node, StreamPipeline pipeline, StreamPipeline parent, Registry registry)
    {
        for (Node usn : node.getUpstream())
        {
            if (usn instanceof StreamOut)
                continue;
            registry.registerPivot(pipeline.getTopParent(), usn);
        }
        for (Node dsn : node.getDownstream())
        {
            /* If we find a cascading stream, we initialize a sub-stream pipeline and crawl it */
            if (dsn instanceof StreamGenerator)
            {
                var sub = new StreamPipeline((StreamGenerator<?, ?>) dsn, parent);
                pipeline.add(sub);
                this.compileStream(dsn, sub, pipeline, registry);
            }
            /* If we find stream nodes, we push them to the pipeline */
            else if (dsn instanceof StreamNode)
            {
                pipeline.add(dsn);
                this.compileStream(dsn, pipeline, parent, registry);
            }
            else if (dsn instanceof StreamAccumulator && parent != null)
                parent.add(dsn);
        }
    }

    private static class Registry
    {
        private final Set<Node> registry = new HashSet<>();
        private final List<Node> nodes = new ArrayList<>();
        private final Map<StreamPipeline, Set<Node>> pivots = new HashMap<>();

        public Registry add(Node node)
        {
            this.register(node);
            this.nodes.add(node);
            return this;
        }

        public Registry register(Node node)
        {
            this.registry.add(node);

            if (node instanceof StreamPipeline)
            {
                this.register(((StreamPipeline) node).getGeneratorNode());
                for (Node dsn : ((StreamPipeline)node).getNodes())
                    this.register(dsn);
            }

            return this;
        }

        public Registry registerPivot(StreamPipeline pipeline, Node node)
        {
            if (!this.pivots.containsKey(pipeline))
                this.pivots.put(pipeline, new HashSet<>());
            this.pivots.get(pipeline).add(node);
            return this;
        }

        public boolean contains(Node node)
        {
            return this.registry.contains(node);
        }

        public boolean isPipelineUpstreamSatisfied(StreamPipeline pipeline)
        {
            /* If the pipeline cannot be found in the registry, as far as the registry is concerned its upstream is satisfied */
            if (!this.pivots.containsKey(pipeline))
                return true;

            long pivotNotSatisfied = this.pivots.get(pipeline).stream()
                .filter(pn -> !this.registry.contains(pn))
                .count()
            ;

            long upstreamNotSatisfied = pipeline.getUpstream().stream()
                .filter(usn -> !this.registry.contains(usn))
                .count()
            ;

            return pivotNotSatisfied + upstreamNotSatisfied == 0;
        }

        public boolean isUpstreamSatisfied(Node node)
        {
            if (node.getUpstream().isEmpty())
                return true;

            long notSatisfied = node.getUpstream().stream()
                .filter(usn -> !this.registry.contains(usn))
                .count()
            ;

            return notSatisfied == 0;
        }

        public List<Node> nodes()
        {
            return this.nodes;
        }
    }
}
