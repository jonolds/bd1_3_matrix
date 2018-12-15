import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class SparkJon {
	
	static SparkSession settings(String... appname) throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName(appname.length > 0 ? appname[0] : "").config("spark.master", "local").config("spark.eventlog.enabled","true").config("spark.executor.cores", "2").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		return spark;
	}
	
	static JavaRDD<String> read(SparkSession ss, String file) {
		return ss.read().textFile(file).javaRDD();
	}
	
	static JavaRDD<String[]> read(SparkSession ss, String file, String regex) {
		return ss.read().textFile(file).javaRDD().map(ln->ln.split(regex));
	}
	
	static int parseInt(String num) {
		return Integer.parseInt(num);
	}
	
	static int toIntExact(long val) {
		return Math.toIntExact(val);
	}
	
	
	/* DEEP COPY R */
	static List<Double> copy(List<Double> orig) {
		return IntStream.range(0, orig.size()).mapToDouble(x->orig.get(x)).boxed().collect(Collectors.toList());
	}
}
