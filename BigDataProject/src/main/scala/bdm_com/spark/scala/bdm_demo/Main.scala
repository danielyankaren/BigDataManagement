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
     var classCol = 34 //class value column id
           
     var allMappers = sc.emptyRDD[List[Map[String,List[Any]]]]
     
     for (n <- 0 to trainSplits.size-1) {
       val trainSplit = trainSplits.apply(n) //Training Data Split n
       val combinations = trainSplit.cartesian(test) //1:Training Data Split n, 2:Test Data Set - All combinations
       //combinations.collect().foreach(println) //to print out the result
       val mapper_n = combinations.map{case (a: (Long, Row), b: (Long, Row)) => DistanceFunction(a,b,classCol)}
       //mapper_n.collect().foreach(println) //to print out the n-th mapper output
       allMappers = allMappers.map(_.toList).union(mapper_n)
     }
    //print(allMappers.take(1).head)
     //print(trainSplits.head.collect.foreach(println)) //print out first split
          
     
//     train.collect().foreach(println)
    
    
      //val data = new DataImport.Import("ionosphere.data")
      //val train = data.train
      //val test = data.test
      //var splits_n = 20 //how many training data splits need
      //val train_split = Map[String, List[String]]()
      //for (j <- 1 until train.size) {
      //  train_split += (j.toString -> train(j.toString))
      //   if(j % splits_n == 0){
      //     val mapper = new Mapper.Distance(train_split, test, j)
      //     train_split.clear
      //   }
      //}
      //if(!train_split.isEmpty){
      // val mapper = new Mapper.Distance(train_split, test, train.size)
      // train_split.clear
      //}
   
      //TODO: we need to use this somehow:
      //   val trainRdd: RDD[(String, LabeledPoint)]  = sc
      // Convert Map to Seq so it can passed to parallelize
      //  .parallelize(train.toSeq)
      //  .map{case (id, (labelInt, values)) => {

      // Convert nested map to Seq so it can be passed to Vector
      //   val features = Vectors.sparse(nFeatures, values.toSeq)

      // Convert label to Double so it can be used for LabeledPoint
      //   val label = labelInt.toDouble

      //  (id, LabeledPoint(label, features))
      // }

  }
  
  def splitSample[T :ClassTag](rdd: RDD[T], n: Int, seed: Long = 42): Seq[RDD[T]] = {
  Vector.tabulate(n) { j =>
    rdd.mapPartitions { data =>
      Random.setSeed(seed)
      data.filter { unused => Random.nextInt(n) == j }
    }
  }
}
  
  def DistanceFunction(trainInstance:(Long, Row), testInstance:(Long, Row), classCol: Int) : List[Map[String,List[Any]]]  = {
    val class_tr = trainInstance._2(classCol)
    var sum = 0.0
    var dist_map = Map[String, List[Any]]()
    var dist_map_list = List[Map[String, List[Any]]]()
//    val ex = "notNone"
    for (k <- 0 until testInstance._2.size-1) {
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
    val dist_ij: Double = sqrt(sum)
    var lst = List(trainInstance._1,dist_ij,class_tr)
    dist_map = Map(testInstance._1.toString -> lst)
    dist_map_list ::= dist_map
    
    return dist_map_list
}
   
}