package words2join.ssii;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class SistemaRecomendacion {

	private static final String NAME = "SistemaRecomendacion";

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
		sconf.set("spark.sql.crossJoin.enabled","true");
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		
		// 2. Creación de la puntuación de recomendación
		Dataset<Row> dataset = sql.read().option("header", true).option("inferSchema", true).csv("Rec_User_Item_Base.csv");

		JavaRDD<RatioCB> aux = dataset.select(dataset.col("user_id").as("user1"),dataset.col("film_id").as("film1"))

		.join(dataset.select(dataset.col("user_id").as("user2"), dataset.col("film_id").as("film2"))).where("user1 = user2")

		.where("film1 <> film2").select("film1", "film2").javaRDD().mapToPair(x -> new Tuple2<Row, Integer>(x, 1))

		.reduceByKey((x, y) -> x + y).map(x->new RatioCB(x._1,x._2));
		
		aux.saveAsTextFile("pruebaRecomendacion");

		ctx.stop();

		ctx.close();
		
	}

}
