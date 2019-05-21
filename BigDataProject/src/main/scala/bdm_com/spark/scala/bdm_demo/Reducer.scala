package bdm_com.spark.scala.bdm_demo

import org.apache.spark.rdd.RDD



object Reducer{

  class Reduce(dist: RDD[(Long, List[List[Double]])] ,
               K: Int) extends Serializable {
    
    // keeping first K values
    val topK = dist.mapValues(Values =>
      Values.take(K))

    val groupedTopK = topK.mapValues(Values => groupByClass(Values))

    
    def groupByClass(lists: List[List[Double]]): Map[Double,List[Double]]   = {

      // Grouping the train ids per label:
        val groupByClass = lists.map(
          list => (list.apply(2),list.head) )
          // groupByKey alternative
          .groupBy(_._1)
          .map { case (k, v) => k -> v.map { _._2}}

      groupByClass

      }


    //val groupedTopZ = topK.mapValues(Values => groupByClass(Values))

    //calculating distance means by class:
    //val distanceMeansByClass = groupedTopZ.mapValues(Values => calcDistanceMean(Values))
    
    //choosing label with the min distance
    //val output = distanceMeansByClass.mapValues(Values => chooseMinDist(Values))
    
    //topK.collect.foreach(println)
    //output.foreach(println)
        
    //def groupByClass(lists: List[List[Double]]): Map[Double,List[List[Double]]]   = {
      
      //Group values by training data class values:
    //  val groupByClass = lists.groupBy(list => list.apply(2))
      //separating Z instances from each of the classes:
    //  val topZ = groupByClass.mapValues(Values => Values.take(Z))
    //  topZ
      
    //}
    
    //def calcDistanceMean(input: Map[Double,List[List[Double]]] ): Map[Double,Double]   = {

    //  val distanceMeans = input.mapValues(Values => mean(Values))
    //  distanceMeans
      
    //}
    
    //def mean(classBasedDistances: List[List[Double]]): Double ={
      
      //Put all the distance values in the one Map
    //  val distances = classBasedDistances.map(list => list.apply(1))
    // val sum = distances.sum
    //  val mu = sum/distances.size
    //  mu
      
    // }
    
    //def chooseMinDist(meanDist: Map[Double,Double]): Double ={
      //meanDist.filter{case (k,v) => v == meanDist.values.min}
    //  val comb = meanDist.minBy{ case (key, value) => value }
    //  comb._1
    // }
        
    }



}