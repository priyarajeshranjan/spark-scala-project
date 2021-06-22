
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object PopulateYesAndNo extends App {

  def parseLine (line : String )={
    val fields = line.split(",")
    if (fields(1).toInt>18)
      (fields(0),fields(1),fields(2),"YES")
    else
      (fields(0),fields(1),fields(2),"NO")
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","populateyesandno")
  val input = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week9spark/dataset.data")

  val mappedInput = input.map(parseLine)
  mappedInput.collect().foreach(println)




}
