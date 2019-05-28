package bdm_com.spark.scala.bdm_demo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

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
    // Reading in data
    val RawData: RDD[String] = sc.textFile("wine.data")

    // taking distinct classes and transforming it to Map
    val str_to_num = RawData.map(_.split(",").last).distinct().zipWithIndex.collect().toMap

    // converting featurea and label to double with a help of str_to_num map
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

    //suffling data before making train and test sets
    val suffledData = data.mapPartitions(iter => {
      val rng = new scala.util.Random(seed=1234L)
      iter.map((rng.nextInt, _))
    }).partitionBy(new HashPartitioner(data.partitions.size)).values

    // spliting data: 80% training, 20% testing
    val Array(training: RDD[LabeledPoint],
      test: RDD[LabeledPoint]) =
      suffledData.randomSplit(Array(0.8, 0.2), seed = 1234L)


    val trainRDD = training.zipWithIndex.map(_.swap)

    val testRDD = test.zipWithIndex.map(_.swap)

    // Get number of splits from program arguments
    val splits = args(0).toInt 

    // Array of 'splits' number of ones
    val ones = Array.fill(splits)(1.0)

    // randomly splitting the trainRDD into 'splits' parts
    val trainSplits = trainRDD.randomSplit(ones).toSeq


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

    //reducer takes grouped mapper output, train and test dataset and K as a argument
    val reducerOutput = new Reducer.Reduce(
      trainRDD.collectAsMap(),
      testRDD.collectAsMap(),
      ordered, K)

    // output of result used in calculating accuracy
    val reduceresult=reducerOutput.result

    //calculating program working time
    timeEnd = System.nanoTime
    val timeDiff = (timeEnd - timeBeg) / 1e9
    print("Time taken: " + timeDiff + " seconds" + "\n")

    //evaluating value for calculating accuracy
    val metrics = new MulticlassMetrics(reduceresult.map{case line => (line._2,line._3)})

    //calculating accuracy
    val precision = metrics.accuracy

    print("Accuracy: " + precision + "\n")

  }
}