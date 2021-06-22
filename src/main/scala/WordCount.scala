
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","wordcount")
  val input = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week9spark/search_data.txt")
  val words = input.flatMap(_.split(" "))
  val wordsLower = words.map(_.toLowerCase())
  val wordcount = wordsLower.map((_,1))
  val finalcount = wordcount.reduceByKey(_+_)
  val sortedResult = finalcount.sortBy(x => x._2)
  //val sortedResult = reversedTupple.sortByKey(false).map(x => (x._2,x._1))
  val results = sortedResult.collect
  for (result <- results)
    {
      val word = result._1
      val count = result._2
      println(s"$word : $count")
    }

}
