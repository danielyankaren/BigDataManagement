

object Main {
  def main(args: Array[String]) {
      val data = new DataImport.Import("ionosphere.data")
      val train = data.train
      val test = data.test
      for (j <- 1 until train.size) {
         val mapper = new Mapper.Distance(train(j.toString), test)
      }

  }
}