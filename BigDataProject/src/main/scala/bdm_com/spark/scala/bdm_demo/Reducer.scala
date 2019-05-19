package bdm_com.spark.scala.bdm_demo

import org.apache.spark.rdd.RDD


object Reducer{

  class Reduce(dist: (Long, List[List[Double]]),
               K: Int) extends Serializable {

    // TODO:Implement the reducer part
    // possible solution may be the following
    // 1.separate the ids of the train instances
    //   per label
    // 2.calculate the centroids of the instances
    // 3.calculate the distance between the centroids
    // and the test instance
    // 4.choose label with the min distance

    }



}