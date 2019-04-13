import scala.collection.mutable.Map
import scala.math.sqrt

object Mapper {
  class Distance(val trainingInstance: List[String], val test: Map[String, List[String]]) {
    val size = test.size
    val class_tr = trainingInstance.last //Class is the last element in the list
    val tr_j = trainingInstance.dropRight(1) //Remove class element from training features
    for (i <- 1 until size) { //for instance of the testing set
      var dist = DistanceFunction(tr_j, test(i.toString))
      println(dist)
      //TODO: put result in the Map
    }
  }
  
  def DistanceFunction(tr_j: List[String], testInstance: List[String]) : Double = {
    val ts_i = testInstance.dropRight(1) //Remove class element from testing features
    var sum = 0
    for (f <- 0 until tr_j.length) {
      val diff: Double = (tr_j(f).toDouble - ts_i(f).toDouble)
      val square: Double = (diff * diff)
      sum +=  square.toInt //TODO: control if converting change anything
    }
    return sqrt(sum)
  }
}