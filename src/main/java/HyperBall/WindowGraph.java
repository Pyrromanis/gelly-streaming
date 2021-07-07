package HyperBall;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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

        deleteFiles();

        List<Centr> centr   = new ArrayList<>();
        List<Long>  dupl    = new ArrayList<>();
        //FileWriter writer= new FileWriter("r3sults.txt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Edge<Long, Long>> edges = env
                .readTextFile("/home/pyrromanis/Downloads/graphs/follow-2018-02-01.txt")
                .map(new MapFunction<String, Edge<Long, Long>>() {
                    @Override
                    public Edge<Long, Long> map(String s) throws Exception {
                       String[] ds_args = s.split(" ");
                       long src = Long.parseLong(ds_args[0]);
                       long trg = Long.parseLong(ds_args[1]);
                       if (!dupl.contains(trg)){
                           centr.add(new Centr(trg));
                           dupl.add(trg);
                           //writer.write(trg + System.lineSeparator());
                           //System.out.println(trg);
                           //return new Edge<>(src, trg, Math.abs(trg-src));
                       }else{
                           for(Centr j : centr){
                                if (j.getName()==trg){
                                    j.incNodes();
                                    j.addDistance(Math.abs(trg-src));
                                    j.setResult();
                                    //return new Edge<>((long)0,j.getName(),(long)j.getResult());
                                    //System.out.println(centr.size());
                                }
                                //System.out.println("EXISTS");
                           }
                       }

                       return new Edge<>(src, trg, Math.abs(trg-src));                     //Follower,the one being followed and their "distance"
                    }
                });

        SimpleEdgeStream<Long,Long> ses = new SimpleEdgeStream<>(edges, env);
        ses.slice(windowTime);                                                            //Reminder that the slice makes sure that all edges of a vertex are in the same window
        ses.getEdges().writeAsText("/home/pyrromanis/Downloads/graphs/results");
        //ses.getEdges().print();

        //for(Centr i: centr){
        //    writer.write(i.toString() + System.lineSeparator());
        //}
        //writer.close();

        JobExecutionResult res = env.execute("Slice the Stream");

        long runtime = res.getNetRuntime();
        System.out.println("Runtime: " + runtime);
        System.out.println(centr.size());
    }

    private static final Time windowTime = Time.of(24, TimeUnit.HOURS);

    public static void deleteFiles(){
        StringBuilder stringBuilder = new StringBuilder();
        for(int i=1;i<5;i++) {
            stringBuilder.append("/home/pyrromanis/Downloads/graphs/results/");
            stringBuilder.append(i);
            //stringBuilder.append(".txt");                                                    //not needed
            String finalString = stringBuilder.toString();
            Path fileToDeletePath = Paths.get(finalString);
            try {
                Files.deleteIfExists(fileToDeletePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            stringBuilder.setLength(0);
        }
    }

    public static class Centr{
        private long name;
        private long num_of_nodes;
        private long total_dist;
        private double result;

        public Centr(long name) {
            this.name = name;
            this.num_of_nodes = 1;
            this.total_dist = 0;
            this.result=0;
        }

        public void setName(long name) {
            this.name = name;
        }

        public long getName() {
            return name;
        }
        public void incNodes() {
            this.num_of_nodes++;
        }

        public long getNum_of_nodes() {
            return num_of_nodes;
        }

        public void addDistance(long dist) {
            this.total_dist+=dist;
        }

        public long getTotal_dist() {
            return total_dist;
        }

        public double getResult(){
            return this.result;
        }

        public void setResult(){
            this.result=(double)num_of_nodes/total_dist;
        }

        @Override
        public String toString() {
            return "Centr{" +
                    "name=" + name +
                    ", num_of_nodes=" + num_of_nodes +
                    ", total_dist=" + total_dist +
                    ", result=" + result +
                    '}';
        }
    }

    @Override
    public String getDescription() {
        return "GraphWindowStream to ImmutableGraph";
    }
}

/*
    Implementation
    --------------
    Step:           Use Gelly to get the Data Stream and slice it in windows
    Problem:        None
    Improvements:   None

    Step:           Pass the windows to HyperBall as ImmutableGraph
    Problem:        None (so far)
    Improvements:   Since ImmutableGraphs loads a graph from disk, I'm printing the windows in files and then load them to HyperBall. Obviously immediate loading would be better
                    in every aspect, but that would mean i would have to change Immutable class so i won't do it for now

    Step:           Run HyperBall
    Problem:        None
    Improvements:   None

    Issues Encountered
    ------------------

    Problem:        NoClassDefFoundError
    Solution:       Project structure all modules from provided to compile
    Description:    Dependencies should be set to provided or else best case is huge jar. In IntelliJ this means that they are not included in the classpath
                    and in-IDE execution will fail with this error. For this purpose a profile that selectively activates that promotes the dependencies to
                    compile, without affecting the whole package. That being said some modules still didn't work so i swapped them in Project structure
                    to compile, hopefully nothing will go sour.

    Problem:        Return type of function could not be determined automatically due to type erasure
    Solution:       Lambda -> Anonymous func
    Description:    Flink needs to know the type of the values that are processed because it needs to serialize and deserialize them.
                    Flink's type system is based on TypeInformation which describes a data type. When you specify a function, Flink tries to infer the return type of that function.
                    Unfortunately, some Lambda functions lose this information due to type erasure such that Flink cannot automatically infer the type.
                    Therefore, you have to explicitly declare the return type.
*/