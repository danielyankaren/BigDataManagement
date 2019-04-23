package bdm_com.spark.scala.bdm_demo

import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object Main {
  
  def main(args: Array[String]) {
     System.setProperty("hadoop.home.dir", "C:\\hadoop") //IF SPARK DOESN'T WORK, YOU SHOULD USE THIS
     //More information about that what you should install:
    //https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html
    
     val conf = new SparkConf()
            .setMaster("local[2]")
             .setAppName("BDMProject")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
   
      val rdd= sqlContext.read.format("csv").option("header", "false").load("ionosphere.data").toDF() //read data in 
      var n=rdd.count().toInt
      val train_rdd = rdd.take(200) //take out first 200 rows for training set
      val test_rdd =  rdd.take(n).drop(200) //take out rest of the rows for testing set
                 
      rdd.collect().foreach(println)
      val data = new DataImport.Import("ionosphere.data")
      val train = data.train
      val test = data.test
      for (j <- 1 until train.size) { 
         val mapper = new Mapper.Distance(Map(train.keysIterator.toList(j)->train(j.toString)), test)
      }
   
//TODO: we need to use this somehow:
//   val trainRdd: RDD[(String, LabeledPoint)]  = sc
//  // Convert Map to Seq so it can passed to parallelize
//  .parallelize(train.toSeq)
//  .map{case (id, (labelInt, values)) => {
//
//      // Convert nested map to Seq so it can be passed to Vector
//      val features = Vectors.sparse(nFeatures, values.toSeq)
//
//      // Convert label to Double so it can be used for LabeledPoint
//      val label = labelInt.toDouble 
//
//      (id, LabeledPoint(label, features))
// }}

  }
}