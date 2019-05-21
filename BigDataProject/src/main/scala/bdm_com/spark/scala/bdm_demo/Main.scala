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

  def main(args: Array[String]) {
     System.setProperty("hadoop.home.dir", "C:\\hadoop")

    // TODO: change the mapping to
    // function generally, without explicitly
    // defining the labels
    val str_to_num = Map("g" -> "1.0", "b" -> "0.0")

    val RawData: RDD[String] = sc.textFile("ionosphere.data")


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
              test: RDD[LabeledPoint]) =
      data.randomSplit(Array(0.8, 0.2), seed = 1234L)

    val trainRDD = training.zipWithIndex.map(_.swap)
    val testRDD = test.zipWithIndex.map(_.swap)

    val splits = 20

    // Array of 'splits' number of ones
    val ones = Array.fill(splits)(1.0)

    // randomly splitting the trainRDD into 'splits' parts
    val trainSplits = trainRDD.randomSplit(ones).toSeq

    // TODO: implement the same with sc.parallelize
    // to parallelize the data splits - In my opinion we can't parallelize RDD because it is not allowed to use the 
    // Mapper class function for the RDD indside the RDD

    val allMappers = new Mapper.Distance(trainSplits, testRDD).result

    // combining the partitioned RDDs
    val listMappers = allMappers.reduce(_ union _)

    // grouping values by key
    val grouped = listMappers.groupByKey


    // ordering the values based on distance
    val ordered = grouped.map(KeyValues => {
      val sorting = KeyValues._2.toList
        .sortBy(value => value.apply(1))
      (KeyValues._1,sorting)
    })
    
    val K = 7 // nearest neighbours

    val reducerOutput = new Reducer.Reduce(trainRDD.collectAsMap(),ordered,K)

    reducerOutput.centroids.collect.foreach(println)


  }
  

}