
import scala.collection.mutable.Map

object Main {
  def main(args: Array[String]) {
      val data = new DataImport.Import("BigDataProject\\ionosphere.data")
      val train = data.train
      val test = data.test
      for (j <- 1 until train.size) {
         val mapper = new Mapper.Distance(Map(train.keysIterator.toList(j)->train(j.toString)), test)
      }
  }
}