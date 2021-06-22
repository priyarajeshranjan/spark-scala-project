
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min


object MinTemprature extends App {

  def parseLine (line : String )={
    val fields = line.split(",")
    val id = fields(0)
    val readingType = fields(2)
    val temperature = fields(3)
    (id,readingType,temperature)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","minimumtemperature")
  val input = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week9spark/tempdata.csv")

  val mappedInput = input.map(parseLine)
  val minTemp = mappedInput.filter(x => x._2 == "TMIN")
  val tempStat = minTemp.map(x => (x._1,x._3.toFloat))
  val minTempsStatn = tempStat.reduceByKey((x, y) => min(x, y))
  val results = minTempsStatn.collect()
  for (result <- results)
    {
      val station = result._1
      val temp = result._2
      val formatTemp = f"$temp%.2f F"
      println(s"$station minimum temperature : $formatTemp")
    }



}
