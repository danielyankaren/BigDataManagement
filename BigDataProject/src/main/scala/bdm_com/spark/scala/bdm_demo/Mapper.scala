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

object Mapper{
  class Distance(trainSplits: Seq[RDD[(Long, Row)]]  , test: RDD[(Long, org.apache.spark.sql.Row)] , classCol: Integer, allMappers: RDD[Map[String,List[Any]]]) extends Serializable{
     var result: RDD[Map[String,List[Any]]] = allMappers
     for (n <- 0 to trainSplits.size-1) {
       val trainSplit = trainSplits.apply(n) //Training Data Split n
       val combinations = trainSplit.cartesian(test) //1:Training Data Split n, 2:Test Data Set - All combinations
       //combinations.collect().foreach(println) //to print out the result
       val mapper_n = combinations.map{case (a: (Long, Row), b: (Long, Row)) => DistanceFunction(a,b,classCol)}
       //mapper_n.collect().foreach(println) //to print out the n-th mapper output
       result = result.union(mapper_n)
     }
     //print(result.take(1).head)
     //print(trainSplits.head.collect.foreach(println)) //print out first split
  }
  
  private def DistanceFunction(trainInstance:(Long, Row), testInstance:(Long, Row), classCol: Int) : Map[String,List[Any]]  = {
    val class_tr = trainInstance._2(classCol)
    var sum = 0.0
    var dist_map = Map[String, List[Any]]()
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
    val lst = List(trainInstance._1,dist_ij,class_tr)
    dist_map = Map(testInstance._1.toString -> lst)    
    return dist_map
}
}

  