package bdm_com.spark.scala.bdm_demo

import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object Main {
//  var sc: SparkContext = null
  
  def main(args: Array[String]) {
    
//    val conf = new SparkConf()
//             .setMaster("local[2]")
//             .setAppName("BDMProject")
//    val sc = new SparkContext(conf)
            
//    val train2 = sc.textFile("BigDataProject\\ionosphere.data": String).map(line => converter.parserToLabeledPoint(line)).toDF().persist
      val data = new DataImport.Import("ionosphere.data")
      val train = data.train
      val test = data.test
      for (j <- 1 until train.size) {
         val mapper = new Mapper.Distance(Map(train.keysIterator.toList(j)->train(j.toString)), test)
      }
  }
}