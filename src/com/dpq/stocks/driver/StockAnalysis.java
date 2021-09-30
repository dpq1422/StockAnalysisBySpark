package com.dpq.stocks.driver;



import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class StockAnalysis {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RDD_EXAMPLES").setMaster("local"); 
        
	    JavaSparkContext sc = new JavaSparkContext(conf); 
	    //Country,Reporting_purpuse,Quarter,Grain,pp0,pp1
		JavaRDD<String> fileRdd = sc.textFile("/Users/dpq/springbootWrokspace/StocksAnalysisBySpark/resources/Stocks.csv",4);
		System.out.println(fileRdd.collect());
		JavaRDD<Tuple2<String,Double>> stockWithPrice = 
				fileRdd.map(data -> new Tuple2<String, Double >(data.split(",")[1] ,Double.parseDouble(data.split(",")[6] )) );
		
		//displaying all content s
		System.out.println("stockWithPrice:"+stockWithPrice.collect());
		
		JavaPairRDD<String, Double> stocksPairRDD = stockWithPrice.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Double>, String, Double>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			List<Tuple2<String, Double>> tuplePairs = new LinkedList<>();
			@Override
			public Iterator<Tuple2<String, Double>> call(Tuple2<String, Double> tuple) throws Exception {
				tuplePairs.add(tuple);
				return tuplePairs.iterator();
			}
		});
		
		System.out.println("stocksPairRDD:"+stocksPairRDD.collect());
		

		JavaPairRDD<String, Double> stocksAnalysisRDD = stocksPairRDD.reduceByKey(new Function2<Double, Double, Double>() {
			
			@Override
			public Double call(Double arg0, Double arg1) throws Exception {
				
				return arg0>arg1?arg0:arg1;
			}
		});
		System.out.println("OUTPUT:"+stocksAnalysisRDD.glom().collect());
		
	}

}
