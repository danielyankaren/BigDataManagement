package bdm_com.spark.scala.bdm_demo

import org.apache.spark.rdd.RDD


object Reducer{

  class Reduce(dist: RDD[(Long, List[List[Double]])] ,
               K: Int, Z: Int) extends Serializable {
    
    // keeping first K values
    val topK = dist.mapValues(Values =>
      Values.take(K))
        
    val groupedTopZ = topK.mapValues(Values => groupByClass(Values))
    
    //calculating distance means by class:
    val distanceMeansByClass = groupedTopZ.mapValues(Values => calcDistanceMean(Values))
    
    //topK.collect.foreach(println)
    distanceMeansByClass.foreach(println)
        
    def groupByClass(lists: List[List[Double]]): Map[Double,List[List[Double]]]   = { 
      
      //Group values by training data class values:
      val groupByClass = lists.groupBy(list => list.apply(2))
      //separating Z instances from each of the classes:
      val topZ = groupByClass.mapValues(Values => Values.take(Z))
      topZ
      
    }
    
    def calcDistanceMean(input: Map[Double,List[List[Double]]] ): Map[Double,Double]   = { 

      val distanceMeans = input.mapValues(Values => mean(Values)) 
      distanceMeans      
      
    }
    
    def mean(classBasedDistances: List[List[Double]]): Double ={
      
      //Put all the distance values in the one Map
      val distances = classBasedDistances.map(list => list.apply(1))
      var sum = distances.sum
      val mu = sum/distances.size
      mu
      
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