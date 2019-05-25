package bdm_com.spark.scala.bdm_demo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

object Main {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("BDMProject")

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    var timeBeg: Long = 0l
    var timeEnd: Long = 0l
    timeBeg = System.nanoTime
    val RawData: RDD[String] = sc.textFile("wine.data")
    
    // taking distinct classes and transforming it to Map
    val str_to_num = RawData.map(_.split(",").last).distinct().zipWithIndex.collect().toMap

    val dataRaw = RawData.map(_.split(",")).map { csv =>
      val label = str_to_num(csv.last).toDouble
      val point = csv.init.map(_.toDouble)
      (label, point)

    }

    val data: RDD[LabeledPoint] = dataRaw
      .map {
        case (label, point) =>
          LabeledPoint(label, Vectors.dense(point))
      }
    
    val suffledData = data.distinct()

    val Array(training: RDD[LabeledPoint],
      test: RDD[LabeledPoint]) =
      suffledData.randomSplit(Array(0.8, 0.2), seed = 1234L)
    //test.saveAsTextFile("test")
    val trainRDD = training.zipWithIndex.map(_.swap)

    val testRDD = test.zipWithIndex.map(_.swap)

    val splits = args(0).toInt 

    // Array of 'splits' number of ones
    val ones = Array.fill(splits)(1.0)

    // randomly splitting the trainRDD into 'splits' parts
    val trainSplits = trainRDD.randomSplit(ones).toSeq

    //val trainSplits2 = sc.broadcast(trainRDD.randomSplit(ones))

    //val trainSplits3 = sc.parallelize(trainRDD.randomSplit(ones))
    // TODO: implement the same with sc.parallelize
    // to parallelize the data splits - In my opinion we can't parallelize RDD because it is not allowed to use the
    // Mapper class function for the RDD inside the RDD

    val allMappers = new Mapper.Distance(trainSplits, testRDD).result

    // combining the partitioned RDDs
    val listMappers = allMappers.reduce(_ union _)

    // grouping values by key
    val grouped = listMappers.groupByKey

    // ordering the values based on distance
    val ordered = grouped.map(KeyValues => {
      val sorting = KeyValues._2.toList
        .sortBy(value => value.apply(1))
      (KeyValues._1, sorting)
    })

    val K = args(1).toInt // nearest neighbours, argument passed from the run config.

    val reducerOutput = new Reducer.Reduce(
      trainRDD.collectAsMap(),
      testRDD.collectAsMap(),
      ordered, K)

    reducerOutput.result.collect.foreach(println)
    str_to_num.foreach(println)

    timeEnd = System.nanoTime
    val timeDiff = (timeEnd - timeBeg) / 1e9
    print("Time taken: " + timeDiff + " seconds" + "\n")


    val reals=test.map(_.label).collect
    val results = reducerOutput.result.map(_._2).collect
    val diffs=difference(reals, results)
    val accu=diffs.filter(i => i==0.0).length.toDouble/reals.length.toDouble
    print("Accuracy: " + accu + "\n")
    str_to_num.foreach(println)
  }
  def difference(xs: Array[Double],
               ys: Array[Double]) = {
    (xs zip ys).map {
      case (x, y) => (y - x) }
  }
}