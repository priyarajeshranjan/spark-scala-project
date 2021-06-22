
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object KeyWordAmount extends App {

  def loadBoringWords(): Set[String]= {
    var boringWords:Set[String] = Set()
    val lines = Source.fromFile("/Users/rajesh/Downloads/trendy_tech_trainning/week10SparkDepth/dataset/boringwords.txt").getLines()
    for (line <- lines)
      {
        boringWords += line
      }
    boringWords
  }


  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","keywordamount")
  val nameSet = sc.broadcast(loadBoringWords())
  val input = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week10SparkDepth/dataset/bigdatacampaigndata.csv")

  val mappedInput = input.map(x => (x.split(",")(10).toFloat,(x.split(",")(0))))
  val words = mappedInput.flatMapValues(x => x.split(" "))
  val finalMapped = words.map(x => (x._2.toLowerCase(),x._1))
  val filterRdd = finalMapped.filter(x => !nameSet.value(x._1))
  val total = filterRdd.reduceByKey((x,y) => x+y)
  val sorted = total.sortBy(x => x._2,false)
  sorted.take(20).foreach(println)
  scala.io.StdIn.readLine()


}
