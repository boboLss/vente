package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AppVenteVilleAnnee {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Ventes Par Ville et Année").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ventes = sc.textFile("file:///opt/data/ventes.txt");

        JavaPairRDD<String, Double> ventesParVilleAnnee = ventes
                .mapToPair(line -> {
                    String[] parts = line.split(" ");
                    String date = parts[0];             // ex: 2023-01-10
                    String annee = date.split("-")[0];  // ex: 2023
                    String ville = parts[1];
                    double prix = Double.parseDouble(parts[3]);
                    String cle = ville + "-" + annee;
                    return new Tuple2<>(cle, prix);
                })
                .reduceByKey(Double::sum);

        ventesParVilleAnnee.foreach(tuple -> System.out.println(tuple._1 + " => " + tuple._2));

        sc.close();
    }
}
