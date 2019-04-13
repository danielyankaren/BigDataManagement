import scala.collection.mutable.Map

object Mapper {
  class Distance(val trainingInstance: List[String], val test: Map[String, List[String]]) {
    val size = test.size
    val class_tr = trainingInstance.last //Class is the last element in the list
    val tr_j = trainingInstance.dropRight(1) //Remove class element from training features
    for (i <- 1 until size) { //for instance of the testing set
      //var dist: Float = DistanceFunction(trainingInstance, test(i.toString))
      //TODO: put result in the Map
    }
  }
  
  def DistanceFunction(tr_j: List[String], testInstance: List[String]){
    val tr_i = testInstance.dropRight(1) //Remove class element from testing features
    //TODO: Calculate distance according to the formula in the article
  }
}