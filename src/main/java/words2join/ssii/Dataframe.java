package words2join.ssii;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class Dataframe {
	private static final String NAME = "Dataframe";

	public static void main(String[] args) {
		// 1. Definir un SparkContext
		SparkConf sconf = new SparkConf().setAppName(NAME);
		sconf.setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		// 2. Resolver nuestro problema
		Dataset<Row> dataset = sql.read().option("header", true).option("inferSchema", true).csv("prueba1");
		Dataset<Row> manufacturas = dataset.select(dataset.col("Valor1")).distinct();
		dataset.printSchema();
		manufacturas.show();
		// 3. Liberar recursos
		ctx.stop();
		ctx.close();


	}

}
