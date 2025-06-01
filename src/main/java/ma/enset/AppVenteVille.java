//package ma.enset;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//public class AppVenteVille {
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("Ventes Par Ville").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> ventes = sc.textFile("file:///opt/data/ventes.txt");
//
//        JavaPairRDD<String, Double> ventesParVille = ventes
//                .mapToPair(line -> {
//                    String[] parts = line.split(" ");
//                    String ville = parts[1];
//                    double prix = Double.parseDouble(parts[3]);
//                    return new Tuple2<>(ville, prix);
//                })
//                .reduceByKey(Double::sum);
//
//        ventesParVille.foreach(tuple -> System.out.println(tuple._1 + " => " + tuple._2));
//
//        sc.close();
//    }
//}
