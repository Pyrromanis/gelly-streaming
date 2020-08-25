package HyperBall;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowGraph implements ProgramDescription{
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Edge<Long, Long>> edges = env
                .readTextFile("/home/pyrromanis/Downloads/graphs/follow-2018-02-01.txt")
                .map((MapFunction<String, Edge<Long, Long>>) s -> {
                    String[] ds_args = s.split(" ");
                    long src = Long.parseLong(ds_args[0]);
                    long trg = Long.parseLong(ds_args[1]);
                    return new Edge<>(src, trg, null);
                });

        SimpleEdgeStream<Long,Long> ses = new SimpleEdgeStream<>(edges, env);
        ses.slice(windowTime);
        ses.getEdges().print();

        JobExecutionResult res = env.execute("Slice the Stream");
        long runtime = res.getNetRuntime();
        System.out.println("Runtime: " + runtime);
    }

    private static final Time windowTime = Time.of(1, TimeUnit.DAYS);

    @Override
    public String getDescription() {
        return "GraphWindowStream to ImmutableGraph";
    }
}
