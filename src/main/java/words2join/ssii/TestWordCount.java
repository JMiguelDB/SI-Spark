package words2join.ssii;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public final class TestWordCount {
	private static final String NAME = "JavaWordCount";
	private static final Pattern SPACE = Pattern.compile(" ");
	
	//String master = System.getProperty("spark.master");

	public static void main(String[] args) throws Exception{
		SparkConf sconf = new SparkConf().setAppName(NAME);
		sconf.setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		
		// 2. Resolver nuestro problema.
	 	JavaRDD<String> lines = ctx.textFile("prueba");

	 	JavaRDD<String> words = lines.flatMap(x -> ((List<String>) (Arrays.asList(SPACE.split(x)))).iterator());

	 	JavaPairRDD<String, Integer> ones = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

	 	JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

	 	System.out.println(counts.collectAsMap());

	 	// 3. Liberar Recursos

	 	ctx.stop();
	 	ctx.close();



	}

}
