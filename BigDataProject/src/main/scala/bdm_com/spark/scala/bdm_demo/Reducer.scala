package bdm_com.spark.scala.bdm_demo

import org.apache.spark.rdd.RDD


object Reducer{

  class Reduce(dist: RDD[(Long, List[List[Double]])] ,
               K: Int, Z: Int) extends Serializable {
    
    // keeping first K values
    val topK = dist.mapValues(Values =>
      Values.take(K))
      
    //Group values by traing data class values:
    val groupedTopK = topK.mapValues(Values => groupByClass(Values))

    groupedTopK.collect.foreach(println)
    
    
    def groupByClass(lists: List[List[Double]]): Map[Double,List[List[Double]]]   = {
      val grouped = lists.groupBy(list => list.apply(2))
      grouped
    }
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