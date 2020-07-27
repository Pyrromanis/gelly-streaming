package HyperBall;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.WindowTriangles;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class WindowGraph implements ProgramDescription{
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (!parseParameters(args, env)) {
            return;
        }

        SimpleEdgeStream<Long, NullValue> edges = getGraphStream(env);

        DataStream<Tuple2<Integer, Long>> immGraph =
                edges.slice(windowTime, EdgeDirection.ALL)
                        .applyOnNeighbors(new WindowTriangles.GenerateCandidateEdges())
                        .keyBy(0, 1).timeWindow(windowTime)
                        .apply(new WindowTriangles.CountTriangles())
                        .timeWindowAll(windowTime).sum(0);

        if (fileOutput) {
            immGraph.writeAsText(outputPath);
        }
        else {
            immGraph.print();
        }
        env.execute("Window Maker");
    }
    private static boolean fileOutput = false;
    private static String edgeInputPath = null;
    private static String outputPath = null;
    private static Time windowTime = Time.of(300, TimeUnit.MILLISECONDS);

    private static boolean parseParameters(String[] args, StreamExecutionEnvironment env) {

        if(args.length > 0) {
            if(args.length < 3) {
                System.err.println("Usage: WindowGraph <input edges path> <output path>"
                        + " <window time (ms)> <parallelism (optional)>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            windowTime = Time.of(Long.parseLong(args[2]), TimeUnit.MILLISECONDS);
            if (args.length > 3) {
                env.setParallelism(Integer.parseInt(args[3]));
            }

        } else {
            System.out.println("Executing WindowGraph with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: WindowGraph <input edges path> <output path>"
                    + " <window time (ms)> <parallelism (optional)>");
        }
        return true;
    }


    @SuppressWarnings("serial")
    private static SimpleEdgeStream<Long, NullValue> getGraphStream(StreamExecutionEnvironment env) {

        if (fileOutput) {
            return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
                    .map(new MapFunction<String, Edge<Long, Long>>() {
                        @Override
                        public Edge<Long, Long> map(String s) {
                            String[] fields = s.split("\\s");
                            long src = Long.parseLong(fields[0]);
                            long trg = Long.parseLong(fields[1]);
                            long timestamp = Long.parseLong(fields[2]);
                            return new Edge<>(src, trg, timestamp);
                        }
                    }), new WindowTriangles.EdgeValueTimestampExtractor(), env).mapEdges(new WindowTriangles.RemoveEdgeValue());
        }

        return new SimpleEdgeStream<>(env.generateSequence(1, 10).flatMap(
                new FlatMapFunction<Long, Edge<Long, Long>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, Long>> out) throws Exception {
                        for (int i = 1; i < 3; i++) {
                            long target = key + i;
                            out.collect(new Edge<>(key, target, key*100 + (i-1)*50));
                        }
                    }
                }), new WindowTriangles.EdgeValueTimestampExtractor(), env).mapEdges(new WindowTriangles.RemoveEdgeValue());
    }


    @SuppressWarnings("serial")
    public static final class EdgeValueTimestampExtractor extends AscendingTimestampExtractor<Edge<Long, Long>> {
        @Override
        public long extractAscendingTimestamp(Edge<Long, Long> element) {
            return element.getValue();
        }
    }

    @SuppressWarnings("serial")
    public static final class RemoveEdgeValue implements MapFunction<Edge<Long,Long>, NullValue> {
        @Override
        public NullValue map(Edge<Long, Long> edge) {
            return NullValue.getInstance();
        }
    }

    @Override
    public String getDescription() {
        return "GraphWindowStream to ImmutableGraph";
    }
}
