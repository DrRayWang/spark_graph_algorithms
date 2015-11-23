import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
public class GraphOps {
    
    
    public static void main(String[] args) {
        String master;
        if(args.length > 0)
            master = args[0];
        else
            master = "local";
        SparkConf conf = new SparkConf().setAppName("graphops").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        String dir;
        if(args.length > 0)
            dir = args[1];
        else
            dir = "<path to your N-Triple database>";
        JavaRDD<String> lines = sc.textFile(dir);
        
        // summup total number of triples in datasets
        JavaRDD<Integer> numElem = lines.map(new Function<String, Integer>() {
            public Integer call(String s) {
                return 1;
            }
        });
        
        int tripleTotal = numElem.reduce(new Function2<Integer,Integer,Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        
        
        lines.cache();
        
        JavaRDD<String> resources = lines.flatMap(
                new FlatMapFunction<String,String>() {
                    public Iterable<String> call(String s) {
                        String new_s = s.substring(0, s.lastIndexOf('.')-1);
                        return Arrays.asList(new_s.split(" "));
                    }
        });
        
        JavaRDD<String> uniResources = resources.distinct();
        long resourceTotal = uniResources.count();    
        

        JavaRDD<String> nodes = lines.flatMap(
                new FlatMapFunction<String,String>() {
                    public Iterable<String> call(String s) {
                        String[] temp = s.split(" ");
                        String[] nodes = {temp[0], temp[2]};
                        return Arrays.asList(nodes);
                    }
        });
        
        JavaRDD<String> uniNodes = nodes.distinct();
        
        String outputDir;
        if(args.length > 0)
            outputDir = args[2];
        else
            outputDir = "<path to your N-Triple database>";
        String output1 = outputDir + "/nodes";
        uniNodes.saveAsTextFile(output1);
        long nodeTotal = uniNodes.count();
        
        
        JavaRDD<String> edges = lines.map(
                new Function<String,String>() {
                    public String call(String s) {
                        String[] temp = s.split(" ");
                        return temp[1];
                    }
        });
        
        JavaRDD<String> uniEdges = edges.distinct();
        String output2 = outputDir + "/edges";
        uniEdges.saveAsTextFile(output2);
        long edgeTotal = uniEdges.count();
        
        
        JavaPairRDD<String,String> neighbors = lines.mapToPair(
                new PairFunction<String,String,String>() {
                    public Tuple2<String,String> call(String s) {
                        String[] temp = s.split(" ");
                        return new Tuple2<String,String>(temp[0], temp[2]);
                    }
        });
        JavaPairRDD<String,String> uniNeighbors = neighbors.distinct();
        JavaPairRDD<String, Iterable<String>> ADJ_list = uniNeighbors.groupByKey();
        
        String output3 = outputDir + "/graph";
        ADJ_list.saveAsTextFile(output3);
        
        System.out.println("Number of Triples: " + tripleTotal);
        System.out.println("Number of Resources: " + resourceTotal);
        System.out.println("Number of Nodes: " + nodeTotal);
        System.out.println("Number of Edges: " + edgeTotal);
        
        FileOutputStream out = null;
        
        try {
            out = new FileOutputStream(outputDir + "graph_brief.txt");
            out.write(tripleTotal);
            out.write(String.valueOf(resourceTotal).getBytes());
            out.write(String.valueOf(nodeTotal).getBytes());
            out.write(String.valueOf(edgeTotal).getBytes());
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
