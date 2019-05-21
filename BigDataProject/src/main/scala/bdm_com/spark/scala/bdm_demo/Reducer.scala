package bdm_com.spark.scala.bdm_demo

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.LabeledPoint
//import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector



object Reducer{

  class Reduce(//train_data: scala.collection.Map[Long,LabeledPoint],
               dist: RDD[(Long, List[List[Double]])] ,
               K: Int,
               trainRDD: RDD[(Long,LabeledPoint)],
               testRDD: RDD[(Long,LabeledPoint)]
              ) extends Serializable {
    
    // keeping first K values
    val topK = dist.mapValues(Values =>
      Values.take(K))

    val groupedTopK = topK.mapValues(Values => groupByClass(Values))
    
    //val centroids = groupedTopK.mapValues(Values => findCentroids(Values))


    def groupByClass(lists: List[List[Double]]): Map[Double,List[Double]]     = {

      // Grouping the train ids per label:
      val groupByClass = lists.map(
        list => (list.apply(2), list.head))
        // groupByKey alternative
        .groupBy(_._1)
        .map { case (k, v) => k -> v.map {
          _._2
        }
        }
      

      groupByClass

    }

    // example train_ids
    val train_ids: List[Int] = List.range(1, 10)

    def centroid(vec: List[Int]): Array[Double] = {

      val centre = trainRDD.filter(
        row => vec.contains(row._1)
      ).map(par => (par._2.features, 1))
        .reduce((a, b) => FeatureSum(a, b))

      val mu = Vectors.dense(centre._1.toArray).toArray.map(_ / centre._2)

      mu

    }

    val mu = centroid(train_ids)

    def FeatureSum(tuple: (Vector, Int),
                   tuple1: (Vector, Int)): (Vector, Int) = {

      val sum = Vectors.dense((tuple._1.toArray zip tuple1._1.toArray).map {
        case (x, y) => x + y
      }
      )

      val Z = tuple._2 + tuple1._2

      (sum, Z)

    }


    //    def findCentroids(train_ids: Map[Double,List[Double]] ): Map[Double,List[org.apache.spark.ml.linalg.Vector]]       = {
//
//      //find test instances by class:
//      val testInstancesByClass = train_ids.mapValues(Values => Values.map(tr_id => train_data.get(tr_id.toLong).get.features)).map(identity)
//      testInstancesByClass
//
//      //TODO: sum the test instances and divide by Z
//
//
//
//    }










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