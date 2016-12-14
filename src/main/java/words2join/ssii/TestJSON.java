package words2join.ssii;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class TestJSON {
	private static final String NAME = "TestJSON";

	public static void main(String[] args) {
		// 0. Quitar log innecesario
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// 1. Definir un SparkContext
		SparkConf sconf = new SparkConf().setAppName(NAME);
		sconf.setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		// 2. Resolver nuestro problema
		Dataset<Row> dataset1 = sql.read().option("header", true).option("inferSchema", true).json("C://Users//JM//Desktop//fork//words2Join//app//db//users.json");
		dataset1.printSchema();
		Dataset<Row> dataset2 = sql.read().option("header", true).option("inferSchema", true).json("C://Users//JM//Desktop//fork//words2Join//app//db//friends.json");
		dataset2.printSchema();
		
		Dataset<Row> username = dataset1.select(dataset1.col("username")).distinct();	
		username.show();
		Dataset<Row> id = dataset1.select(dataset1.col("_id")).distinct();	
		id.show();
		Dataset<Row> dataset3 =username.join(id);
		dataset3.show();
		
		// 3. Liberar recursos
		ctx.stop();
		ctx.close();

	}

}
