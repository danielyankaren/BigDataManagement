package bdm_com.spark.scala.bdm_demo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.Random
import org.apache.spark.SparkConf


object Main {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("BDMProject")

  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
     System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val RawData: RDD[String] = sc.textFile("ionosphere.data")

    val str_to_num = Map("g" -> "1.0", "b" -> "0.0")

    val dataRaw = RawData.map(_.split(",")).map { csv =>
      val label = str_to_num(csv.last).toDouble
      val point = csv.init.map(_.toDouble)
      (label, point)
    }


    val data: RDD[LabeledPoint] = dataRaw
      .map { case (label, point) =>
        LabeledPoint(label, Vectors.dense(point))
      }

    val Array(training: RDD[LabeledPoint],
    test: RDD[LabeledPoint]) = data.randomSplit(Array(0.8, 0.2), seed = 1234L)

    val trainRDD = training.zipWithIndex.map(_.swap)
    val testRDD = test.zipWithIndex.map(_.swap)

    val splits = 20

    val trainSplits = splitSample(trainRDD, splits)

    val allMappers = new Mapper.Distance(trainSplits, testRDD).result
    val listMappers = allMappers.reduce(_ union _)
    var grouped = listMappers.groupBy {case (k, v) => k}
    val reduceOutput = grouped.map { case (a) =>  new Reducer.Reduce(a)  }
    
    println(reduceOutput.collect().apply(0).sortedLists)

    //allMappers.take(1).head.collect().foreach(println) //print the first Mapper output
   
  }
  
  def splitSample[T :ClassTag](rdd: RDD[T], n: Int, seed: Long = 42): Seq[RDD[T]] = {
  Vector.tabulate(n) { j =>
    rdd.mapPartitions { data =>
      Random.setSeed(seed)
      data.filter { unused => Random.nextInt(n) == j }
    }
  }
}
  
   
}