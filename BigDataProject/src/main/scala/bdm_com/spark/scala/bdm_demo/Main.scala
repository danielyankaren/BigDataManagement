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
     
     var allMappers = sc.emptyRDD[(String, Row)] //Value for all the Mapper's output
    var result: RDD[(String, Row)] = allMappers
    for (n <- 0 to trainSplits.size-1) {
      val trainSplit = trainSplits.apply(n) //Training Data Split n
      val combinations = trainSplit.cartesian(test) //1:Training Data Split n, 2:Test Data Set - All combinations
      val trainInstance = combinations.take(1).head._1
      val testInstance = combinations.take(1).head._2
      val class_tr = trainInstance._2(classCol)
      //combinations.collect().foreach(println) //to print out the result
      val mapper_n = combinations.map{case (a: (Long, Row), b: (Long, Row)) => DistanceFunction(a,b,classCol)}
      //mapper_n.collect().foreach(println) //to print out the n-th mapper output

      result = result.union(mapper_n.take(1).head)
    }

//     allMappers = new Mapper.Distance(trainSplits, test, classCol, allMappers).result

    //print(allMappers.take(1).head)
     
  }
  private def DistanceFunction(trainInstance:(Long, Row), testInstance:(Long, Row), classCol: Int) : RDD[(String, Row)]  = {
    val class_tr = trainInstance._2(classCol)
    var sum = 0.0
    //    val ex = "notNone"
    for (k <- 0 until testInstance._2.size-1) {
      if(k!=classCol){
        //      try{val trj: Double = trainInstance._2.getString(5).toDouble}catch{case e: NumberFormatException => ex=="None"}
        //      try{val tsi: Double = testInstance._2.getString(5).toDouble}catch{case e: NumberFormatException => ex=="None"}
        val trj: Double = trainInstance._2.getString(k).toDouble
        val tsi: Double = testInstance._2.getString(k).toDouble

        //      if(trj != None && tsi != None){
        val diff: Double = trj - tsi
        val square: Double = (diff * diff)
        sum +=  square
        //      }
      }
    }
    val dist_ij: Double = sqrt(sum)
    val lst = Row(trainInstance._1,dist_ij,class_tr)
    val ts = testInstance._1.toString
    val dist_map = sc.parallelize(List(lst))
    val mapper_n = dist_map.map(a => (ts,a))
    return mapper_n
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