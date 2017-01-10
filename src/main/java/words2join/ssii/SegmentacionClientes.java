package words2join.ssii;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SegmentacionClientes {
	private static final String NAME = "SegmentacionClientes";
	private static final long SEED = 1;
	private static final int K = 5;
	private static final int MAX_ITER = 10;

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:\\winutil\\");
		System.setProperty("spark.sql.warehouse.dir",
				"file:///${System.getProperty(\"user.dir\")}/spark-warehouse".replaceAll("\\\\", "/"));
		// 0. Quitar log innecesario
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// 1. Definir un SparkContext
		SparkConf sconf = new SparkConf().setAppName(NAME);
		sconf.setMaster("local[2]");
		sconf.set("spark.sql.crossJoin.enabled", "true");
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());

		// 2. Creación de los conjuntos frecuentes
		Dataset<Row> users = sql.read().option("header", true).option("inferSchema", true)
				.csv("u.user");

//		List<Integer> works = users.select("occupation").distinct().toJavaRDD().collect().stream().map(x ->
//
//		x.get(0).hashCode()).collect(Collectors.toList());
//		
//		System.out.println(works.toString());
//
//		List<Integer> zips = users.select("zip").distinct().toJavaRDD().collect().stream().map(x ->
//
//		x.getString(0).substring(0, 2).hashCode()).collect(Collectors.toList());
//
//		List<Integer> ages = users.select("age").distinct().toJavaRDD().collect().stream().map(x ->
//
//		x.getInt(0)).collect(Collectors.toList());


		JavaRDD<Vector> jrdd = new JavaRDD<Row>(users.rdd(),

				users.org$apache$spark$sql$Dataset$$classTag()).

						map(x -> Vectors.dense(new double[]{x.getInt(1)}));

		// Cluster the data into two classes using KMeans

		KMeansModel clusters = new KMeans().setSeed(SEED).setK(K).

		setMaxIterations(MAX_ITER).run(jrdd.rdd());
		
		System.out.println(Arrays.toString(clusters.clusterCenters()));

		// 3. Liberar Recursos

		ctx.stop();
		ctx.close();

	}

}
