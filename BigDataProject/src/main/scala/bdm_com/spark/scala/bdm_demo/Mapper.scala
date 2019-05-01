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
  class Distance(sc: SparkContext, trainSplits: Seq[RDD[(Long, Row)]]  , test: RDD[(Long, Row)] , classCol: Integer) extends Serializable{

     val result = trainSplits.map{case (a: RDD[(Long,Row)]) => MapperN(a)}
     
  def MapperN(trainSplit: RDD[(Long, Row)]) : RDD[(String, Row)]  = {
       val combinations = trainSplit.cartesian(test) //1:Training Data Split n, 2:Test Data Set - All combinations
       //combinations.collect().foreach(println) //to print out the result
       val mapper_n = combinations.map{case (a: (Long, Row), b: (Long, Row)) => DistanceFunction(a,b,classCol)}
       return mapper_n
     }
     
     
  def DistanceFunction(trainInstance:(Long, Row), testInstance:(Long, Row), classCol: Int) : (String, Row)  = {
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
    val row = Row(trainInstance._1,dist_ij,class_tr)
    val ts = testInstance._1.toString
    val dist_map = Main.sc.parallelize(List(row))
    val mapper_n = dist_map.map(a => (ts,a))
    return mapper_n.take(1).head
}
  }
}

  