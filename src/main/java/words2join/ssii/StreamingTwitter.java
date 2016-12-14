package words2join.ssii;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.Authorization;
import twitter4j.conf.ConfigurationBuilder;

public class StreamingTwitter {
	private static final String NAME = "StreamingTwitter";
	private static final String TWITTER_PROPS_FILE = "twitter.properties";
	private static Properties props;

	public static void configureTwitterCredentials() throws ClassNotFoundException, IOException {
		props = new Properties();
		InputStream input = StreamingTwitter.class.getClassLoader()
				.getResourceAsStream(TWITTER_PROPS_FILE);
		props.load(input);

	}

	public static Authorization getAuthority() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(props.getProperty("consumerKey").trim())
				.setOAuthConsumerSecret(props.getProperty("consumerSecret").trim())
				.setOAuthAccessToken(props.getProperty("accessToken").trim())
				.setOAuthAccessTokenSecret(props.getProperty("accessTokenSecret").trim());
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		return twitter.getAuthorization();
	}

	public static String[] getKeys() {
		return new String[] { "#HawkersxHuaweiMate9", "Jorge Cremades", "#QuedadaMeritxellista",
				"#PropuestaEnMovimiento", "El FMI" };
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "c:\\winutil\\");
		System.setProperty("spark.sql.warehouse.dir",
				"file:///${System.getProperty(\"user.dir\")}/spark-warehouse".replaceAll("\\\\", "/"));
		// 0. Quitar log innecesario
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// 1. Definir un SparkContext
		SparkConf sconf = new SparkConf().setAppName(NAME);
		sconf.setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		ctx.setLogLevel("WARN");
		// 2. Twitter credentials from twitter.properties
		StreamingTwitter.configureTwitterCredentials();
		Authorization twitter = StreamingTwitter.getAuthority();
		String[] filters = StreamingTwitter.getKeys();
		JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(10000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, twitter, filters);
		JavaDStream<String> filtered = stream.map(status -> "" + status.getText());
		filtered.foreachRDD(rdd -> {
			LocalDateTime lc = LocalDateTime.now();
			//System.out.println(rdd.collect());
			rdd.saveAsTextFile("C://Users//JM//Desktop//clase1SI//ssii//Twitter" + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));
		});
		// 3. Abrir canal de datos
		ssc.start();
		ssc.awaitTermination();

	}

}
