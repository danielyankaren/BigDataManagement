package bdm_com.spark.scala.bdm_demo


import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import scala.math.pow
import scala.math.sqrt

object Mapper{

  class Distance(trainSplits: Seq[RDD[(Long, LabeledPoint)]],
                 testRDD: RDD[(Long, LabeledPoint)]) extends Serializable {

    val result = trainSplits.map {features: RDD[(Long, LabeledPoint)] => MapperN(features) }

    def MapperN(trainSplit: RDD[(Long, LabeledPoint)]): RDD[(Long, List[Double])] = {

      // Defining all combinations between
      // partitioned training set
      // and the test set

      val combinations = trainSplit.cartesian(testRDD)

      val mapper_n = combinations.map {
        case (a: (Long, LabeledPoint), b: (Long, LabeledPoint)) =>
          DistanceFunction(a, b)
      }
      mapper_n
    }


    def distance(xs: Array[Double],
                 ys: Array[Double]) = {
      sqrt((xs zip ys).map {
        case (x, y) => pow(y - x, 2) }.sum)
    }

    def DistanceFunction(trainInstance: (Long, LabeledPoint),
                         testInstance: (Long, LabeledPoint)): (Long, List[Double]) = {

      // setting the label of the training instance
      val class_tr = trainInstance._2.label

      // training instance feature vector
      val tr_j = trainInstance._2.features.toArray

      // test instance feature vector
      val ts_i = testInstance._2.features.toArray

      // distance between two instances
      val dist_ij = distance(tr_j, ts_i)

      val key = testInstance._1

      val value = List(trainInstance._1, dist_ij, class_tr)

      (key, value)
    }

  }
}

  