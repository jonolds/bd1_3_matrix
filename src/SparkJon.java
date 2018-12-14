import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

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
	
	static <K, V> Tuple2<V, K> swap(Tuple2<K, V> t) {return new Tuple2<>(t._2, t._1);}
	
	static int Lng2Int(Long L) { return Math.toIntExact(L); }
	static Double[] strArrToDblArr(String[] sArr) { return Arrays.stream(sArr).mapToDouble(Double::parseDouble).boxed().toArray(Double[]::new); }
	static void printMat(JavaRDD<Double[]> mat) { mat.foreach(ln->println(toStr(ln))); println();}
	static <K, VALS>void printMat(JavaPairRDD<K, VALS[]> mat) { mat.foreach(ln->println(ln._1 + ":  " + toStr(ln._2))); println();}
	static <K, VALS>void printMatRev(JavaPairRDD<VALS[], K> mat) { printMat(mat.mapToPair(e->new Tuple2<>(e._2, e._1))); }
	
	static <T>String toStr(T[] arr) { return "[" + strs(arr).collect(Collectors.joining(", ")) + "]"; }
	static <T>Stream<String> strs(T[] arr) { return Arrays.stream(arr).map(t->String.valueOf(t)); }
	
	static <T>void print(T t) { System.out.print(t); }
	static <T>void println(T t) { System.out.println(t); }

	static <T>void println() { System.out.println(); }
}
