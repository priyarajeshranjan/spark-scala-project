
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object TopMovies extends App {

 // Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","totalspend")
  val ratingsRdd = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/Week11Spark_Struct_API/dataset/ratings.dat")
  val mappedRdd = ratingsRdd.map(x => {
    val fields = x.split("::")
    (fields(1),fields(2))
  })
  val newMapped = mappedRdd.mapValues(x => (x.toFloat,1.0))
  val reduceRdd = newMapped.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  val filterRdd = reduceRdd.filter(x => x._2._2 > 10)
  val ratingProcessed = filterRdd.mapValues(x => x._1/x._2).filter(x => x._2 > 4.5)
  //ratingProcessed.collect().foreach(println)


  val moviesRdd = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/Week11Spark_Struct_API/dataset/movies.dat")
  val moviesMapped = moviesRdd.map(x => {
    val fields = x.split("::")
    (fields(0),fields(1))
  })

  val joinedRdd = moviesMapped.join(ratingProcessed)
  val topMovies = joinedRdd.map(x => x._2._1)
  topMovies.collect().foreach(println)

  scala.io.StdIn.readLine()





}
