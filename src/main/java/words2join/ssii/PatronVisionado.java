package words2join.ssii;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class PatronVisionado {
	private static final String NAME = "PatronVisionado";
	private static final double MIN_SUPPORT = 0.3;
	private static final int NUM_PARTITIONS = 10;
	private static final double MIN_CONFIDENCE = 0.95;

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
		Dataset<Row> dataset = sql.read().option("header", true).option("inferSchema", true)
				.csv("Rec_User_Item_Base.csv");

		Dataset<Row> completa = dataset.select(dataset.col("user_id"), dataset.col("film_id"));
		JavaRDD<Iterable<String>> aux = completa.javaRDD()
				.mapToPair(x -> new Tuple2<String, String>("" + x.getInt(0), "" + x.getInt(1)))
				.reduceByKey((z, y) -> (String) (z + " " + y)).map(x -> Arrays.asList(x._2.split(" ")));

		FPGrowth fp = new FPGrowth().setMinSupport(MIN_SUPPORT).setNumPartitions(NUM_PARTITIONS);

		FPGrowthModel<String> model = fp.run(aux);

		for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());

		}

		AssociationRules arules = new AssociationRules().setMinConfidence(MIN_CONFIDENCE);

		JavaRDD<AssociationRules.Rule<String>> results = arules.run(
				new JavaRDD<>(model.freqItemsets(), model.freqItemsets().org$apache$spark$rdd$RDD$$evidence$1));

		for (AssociationRules.Rule<String> rule : results.collect()) {
			System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " +
					rule.confidence());
		}

		// 3. Liberar Recursos

		ctx.stop();
		ctx.close();

	}

}
