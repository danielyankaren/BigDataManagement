package bdm_com.spark.scala.bdm_demo

import scala.io.Source
import java.io.{FileNotFoundException, IOException}
import scala.collection.immutable.ListMap
import scala.collection.mutable.Map

object DataImport {
  class Import(val filename: String) {
    val train = Map[String, List[String]]()
    val test = Map[String, List[String]]()
    try {
      var trainLineNumber = 0
      var testLineNumber = 0
      for (line <- Source.fromFile(filename).getLines) {
        val feat = line.split(",").toList
        if(trainLineNumber<200){
          trainLineNumber += 1
          train += (trainLineNumber.toString -> feat)
        }else{
          testLineNumber +=1
          test += (testLineNumber.toString -> feat)
        }
      }
    } catch {
        case e: FileNotFoundException => println("Couldn't find that file.")
        case e: IOException => println("Got an IOException!")
    }
  }
  
}