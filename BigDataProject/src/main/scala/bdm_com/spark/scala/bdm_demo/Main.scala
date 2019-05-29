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
    val str_to_num = RawData.map(_.split(",").last)
                            .distinct().zipWithIndex
                            .collect().toMap

    // defining the (label,feature) pairs with str_to_num map
    val dataRaw = RawData.map(_.split(",")).map { csv =>
      val label = str_to_num(csv.last).toDouble
      val feature = csv.init.map(_.toDouble)
      (label, feature)

    }

    val data: RDD[LabeledPoint] = dataRaw
      .map {
        case (label, feature) =>
          LabeledPoint(label, Vectors.dense(feature))
      }

    // shuffling data before making train and test RDDs
    val shuffledData = data.mapPartitions(iter => {
      val random = new scala.util.Random(seed=1234L)
      iter.map((random.nextInt, _))
    }).partitionBy(new HashPartitioner(data.partitions.length)).values

    // splitting data: 80% training, 20% testing
    val Array(training: RDD[LabeledPoint],
      test: RDD[LabeledPoint]) =
      shuffledData.randomSplit(Array(0.8, 0.2),
                               seed = 1234L)

    // indexing the rows
    val trainRDD = training.zipWithIndex.map(_.swap)

    val testRDD = test.zipWithIndex.map(_.swap)

    // Get number of splits from program arguments
    val splits = args(0).toInt 

    // Array of 'splits' number of ones
    val ones = Array.fill(splits)(1.0)

    // randomly splitting the trainRDD into 'splits' parts
    val trainSplits = trainRDD.randomSplit(ones).toSeq


    val allMappers = new Mapper.Distance(trainSplits,
                                         testRDD).result

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

    // number of nearest neighbours
    // passed from the run config.
    val K = args(1).toInt

    // reducer takes train, test RDDs,
    // the Mapper output and K as inputs
    val reducerOutput = new Reducer.Reduce(
      trainRDD.collectAsMap(),
      testRDD.collectAsMap(),
      ordered, K)

    // classification results
    val reduceResult = reducerOutput.result

    // calculating the execution time
    timeEnd = System.nanoTime
    val timeDiff = (timeEnd - timeBeg) / 1e9
    print("Time taken: " + timeDiff + " seconds" + "\n")

    // evaluating the classification
    val metrics = new MulticlassMetrics(reduceResult.map{
      case line => (line._2,line._3)
    })

    // calculating accuracy
    val accuracy = metrics.accuracy

    print("Accuracy: " + accuracy + "\n")


  }

}