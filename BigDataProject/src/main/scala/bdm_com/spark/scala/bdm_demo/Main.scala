
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {
  var sc: SparkContext = null
  
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("CoolApp")
      sc = new SparkContext(conf)
      
      val data = new DataImport.Import("BigDataProject\\ionosphere.data")
      val train = data.train
      val test = data.test
      for (j <- 1 until train.size) {
         val mapper = new Mapper.Distance(Map(train.keysIterator.toList(j)->train(j.toString)), test)
      }
  }
}