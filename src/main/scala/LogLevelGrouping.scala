
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object LogLevelGrouping extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","keywordamount")
  val baseRdd = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week10SparkDepth/dataset/bigLog.txt")

  val mappedRdd = baseRdd.map(x => {
    val fields = x.split(":")
    (fields(0),1)
  })
  mappedRdd.reduceByKey(_+_).collect().foreach(println)
  scala.io.StdIn.readLine()
}
