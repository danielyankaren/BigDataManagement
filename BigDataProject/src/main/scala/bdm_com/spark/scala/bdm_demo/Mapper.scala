package bdm_com.spark.scala.bdm_demo

import scala.collection.mutable.Map
import scala.math.sqrt

object Mapper {
  class Distance(val train: Map[String, List[String]], val test: Map[String, List[String]], val t_stop: Int){
    val size = test.size
    var t_start = t_stop-19   
    var dist_map = Map[String, List[Any]]()
    var dist_map_list = List[Map[String, List[Any]]]()
    for (i <- 1 until size) { //for instance of the testing set
      for (j <- t_start to t_stop){ //for instance of the training set
        val class_tr = train(j.toString).last //Class is the last element in the list
        val tr_j = train(j.toString).dropRight(1) //Remove class element from training features
        var dist = DistanceFunction(tr_j, test(i.toString))
        var lst = List(train.keys.head,dist,class_tr)
        dist_map = Map(test.keysIterator.toList(i) -> lst)
        dist_map_list ::= dist_map
      }
    }
  }
  
  def DistanceFunction(tr_j: List[String], testInstance: List[String]) : Double = {
    val ts_i = testInstance.dropRight(1) //Remove class element from testing features
    var sum = 0.0
    for (f <- 0 until tr_j.length) {
      val diff: Double = (tr_j(f).toDouble - ts_i(f).toDouble)
      val square: Double = (diff * diff)
      sum +=  square
    }
    return sqrt(sum)
  }
  
}