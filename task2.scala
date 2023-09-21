import org.apache.spark.sql.Row

import org.apache.spark.rdd.RDD

val df = spark.read.format("csv").option("header",true).option("sep","\t").load("C:/Users/user/Downloads/Hadoop/search_data.csv")

val lineRDD: RDD[String] = df.select("clickUrl").rdd.map(_.mkString(""))

val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split("\\."))

val kvRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1)) 

val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)

val exchangeRDD: RDD[(Int, String)] = wordCounts.map{case (k,v)=>(v,k)}

val sortRDD: RDD[(Int, String)] = exchangeRDD.sortByKey(false)

sortRDD.take(10).foreach(println)
