package bdm_com.spark.scala.bdm_demo

import org.apache.spark.SparkContext
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import scala.util.Random
import org.apache.spark.sql.types._ 
import scala.reflect._
import org.apache.spark.rdd.RDD
import scala.math.pow
import scala.math.sqrt

object Main {
  
  def main(args: Array[String]) {
     System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //IF SPARK DOESN'T WORK, YOU SHOULD USE THIS
    //More information about that what you should install:
    //https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html
     val conf = new SparkConf()
            .setMaster("local[2]")
             .setAppName("BDMProject")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
   
     //read data in
     val rdd = sqlContext.read.format("csv").option("header", "false").load("ionosphere.data").toDF().rdd

     // Randomly splitting the data into train/test sets.
     val Array(data1, data2) = rdd.randomSplit(Array(0.8, 0.2))
 
     //The input of the Mapper function is a <Key, Value> vector
     //key : the record id of the training instance
     //value: the set of feature values of the training instance
     val train = data1.zipWithIndex.map(_.swap)
     val test = data2.zipWithIndex.map(_.swap)
          
     //Make training data set splits:
     var splits = 20 //number of splits
     val trainSplits = splitSample(train, splits)
     var classCol: Integer = 34 //class value column id
     
     var allMappers = sc.emptyRDD[Map[String,List[Any]]] //Value for all the Mapper's output
     
     allMappers = new Mapper.Distance(trainSplits, test, classCol, allMappers).result
     //print(allMappers.take(1).head)
     
  }
  
  def splitSample[T :ClassTag](rdd: RDD[T], n: Int, seed: Long = 42): Seq[RDD[T]] = {
  Vector.tabulate(n) { j =>
    rdd.mapPartitions { data =>
      Random.setSeed(seed)
      data.filter { unused => Random.nextInt(n) == j }
    }
  }
}
  
   
}