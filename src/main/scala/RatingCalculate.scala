
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object RatingCalculate extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","totalspend")
  val input = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week9spark/moviedata.data")
  val mappedInput = input.map(x => x.split("\t")(2))
  val result = mappedInput.countByValue()
//  val rating = mappedInput.map(x=>(x,1))
//  val reduceRating = rating.reduceByKey((x,y)=>x+y)
//  val result = reduceRating.collect()
  result.foreach(println)



}
