package bdm_com.spark.scala.bdm_demo

import org.apache.spark.rdd.RDD

object Reducer{

  class Reduce(input: (Long, Iterable[(Long, List[Double])])) extends Serializable{
    val key1 = input._1 // the record id of the test instance
    val allLists = input._2.map{ case (a: (Long, List[Double])) =>   a._2} // <trj , distij , class_trj>
    //Sort_ascending by distances
    val sortedLists = allLists.toList.sortWith{(leftE,rightE) => 
     leftE.apply(1) < rightE.apply(1)
}
    //println(sortedLists)
    sortedLists
  }
  
}