import scala.io.Source
import java.io.{FileNotFoundException, IOException}

object DataImport {
  def main(args: Array[String]) {
    val filename = "ionosphere.txt"
    try {
      for (line <- Source.fromFile(filename).getLines) {
        println(line)
      }
    } catch {
        case e: FileNotFoundException => println("Couldn't find that file.")
        case e: IOException => println("Got an IOException!")
    }
  }
}