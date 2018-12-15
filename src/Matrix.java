import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

@SuppressWarnings("unused")
public class Matrix extends SparkJon{
	
	public static void main(String[] args) throws IOException, InterruptedException {
		SparkSession ss = settings("Matrix");
		final JavaRDD<Double[]> M1 = ss.read().textFile("M.txt").javaRDD().map(x->strArrToDblArr(x.split(",")));
		JavaRDD<Double[]> N1 = ss.read().textFile("N.txt").javaRDD().map(x->strArrToDblArr(x.split(",")));
		
		JavaPairRDD<Integer, Double[]>  M = M1.zipWithIndex().mapToPair(e->new Tuple2<>(Lng2Int(e._2), e._1));
		JavaPairRDD<Integer, Double[]>  N = N1.zipWithIndex().mapToPair(e->new Tuple2<>(Lng2Int(e._2), e._1));
		
		final int I = Math.toIntExact(M1.count()), J = M1.first().length, K = N1.first().length;
		
/*	
	M: |I| x |J| matrix.  M = {m_i,j}
	N: |J| x |K| matrix.  N = {n_j,k}
	P = M • N  = |I| x |K| matrix. P = {p_i,k}
*/
		
		//For each m_i,j - produce Pair( (i,k), (M,j,m_i,j) ) for all k in K.
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> mij_pairs = M.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Double[]>, Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {
			public Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>> call(Tuple2<Integer, Double[]> t){
				List<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>> list = new ArrayList<>();
				ints(0, t._2.length).forEach(c->ints(0, K).forEach(k->list.add(new Tuple2<>(new Tuple2<>(t._1, k), new Tuple3<>(0, c, t._2[c]) ))));
				return list.iterator();
			}
		});
		
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> njk_pairs = N.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Double[]>, Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {
			public Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>> call(Tuple2<Integer, Double[]> t){
				List<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>> list = new ArrayList<>();
				ints(0, t._2.length).forEach(c->ints(0, I).forEach(i->list.add(new Tuple2<>(new Tuple2<>(i, c), new Tuple3<>(1, t._1, t._2[c])))));
				return list.iterator();
			}
		});
		
		
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> mij_sort = mij_pairs.sortByKey(new SortP()).mapToPair(x->swap(x)).sortByKey(new SortByJ()).mapToPair(x->swap(x));
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> njk_sort = njk_pairs.sortByKey(new SortP()).mapToPair(x->swap(x)).sortByKey(new SortByJ()).mapToPair(x->swap(x));
		
		
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>>> mnjoined = mij_sort.fullOuterJoin(njk_sort).mapToPair(x->new Tuple2<>(x._1, new Tuple2<>(x._2._1.get(), x._2._2.get())));
		
		JavaPairRDD<Tuple2<Integer, Integer>, Double> multi = mnjoined.filter(x->x._2._1._2().equals(x._2._2._2())).mapToPair(e->new Tuple2<>(e._1, e._2._1._3() * e._2._2._3()));

		JavaPairRDD<Tuple2<Integer, Integer>, Double> summed = multi.reduceByKey((v1, v2) -> v1 + v2).sortByKey(new SortP());
		
	
	
		
//		mij_pairs.saveAsTextFile("output/out1");
//		njk_pairs.saveAsTextFile("output/out2");
//		mij_sort.saveAsTextFile("output/out3");
//		njk_sort.saveAsTextFile("output/out4");
//		mnjoined.saveAsTextFile("output/out5");
//		multi.saveAsTextFile("output/out6");
		
		
		summed.saveAsTextFile("output/out7");
					
		
		println("\n M:");
		printMat(M);
		println("\n N:");
		printMat(N);
		println("\n P:");
		summed.foreach(x->print((x._1._2 % 4 == 0 ? "\n": " ") + " " + x._2()));
		
		
//		Thread.sleep(10000);
	}
	
	static boolean isJEqual(Integer a, Integer b) { return a.equals(b);
	}

	static JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> mapMij(JavaPairRDD<Integer, Double[]> m3, Integer K, Integer slot) {
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> jpr = m3.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Double[]>, Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {
			
			public Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>> call(Tuple2<Integer, Double[]> t) throws Exception {
				List<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>> list = new ArrayList<>();
				ints(0, t._2.length).forEach(c->ints(0, K).forEach(k->list.add(new Tuple2<>(new Tuple2<>(t._1, k), new Tuple3<>(slot, c, t._2[c]) ))));
				return list.iterator();
			}
		});
		return jpr;
	}
	
	static class SortByJ implements Comparator<Tuple3<Integer, Integer, Double>>, Serializable {
		public int compare(Tuple3<Integer, Integer, Double> a,
				Tuple3<Integer, Integer, Double> b) {
			if(a._2() > b._2())
				return 1;
			if(a._2() < b._2())
				return -1;
			return 0;
		}
	}
	
	static class SortP implements Comparator<Tuple2<Integer, Integer>>, Serializable {
		@Override
		public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
			if(a._1() > b._1())
				return 1;
			else if (a._1() < b._1())
				return -1;
			else if (a._2() > b._2())
				return 1;
			else if (a._2() < b._2())
				return -1;
			return 0;
		}
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
	static IntStream ints(int begin, int end) { return IntStream.range(begin, end); }
}