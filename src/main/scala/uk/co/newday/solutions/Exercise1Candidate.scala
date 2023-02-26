package uk.co.newday.solutions

import org.apache.avro.SchemaBuilder.nullable
import org.apache.spark
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, Column}

object Exercise1Candidate {

  private case class Movie (movieId:Int, title:String, genre:String)
  private case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)

  def execute(): (DataFrame, DataFrame) =
    {

      // read in the movies.dat and ratings.dat files into a dataframe
      // Spark had to upgraded to 3.3.1 in order to support multiple character delimiter as found in the .dat files
      val spark = SparkSession
        .builder()
        .appName("Exercise")
        .config("spark.master", "local")
        .getOrCreate()

      val columnsMovies = "MovieID,null1,Title,null2,Genres"
      val fieldsMovies = columnsMovies.split(",").map(fieldName => StructField(fieldName, StringType,
        nullable = true))
      val customSchemaMovies = StructType(fieldsMovies)

      val moviesDf = spark.read.option("header", "false").schema(customSchemaMovies).option("delimiter","::")
        .csv("src/main/resources/movies.dat")

      val columnsRatings = "UserID,MovieID,Rating,Timestamp"
      val fieldsRatings = columnsRatings.split(",").map(fieldName => StructField(fieldName, StringType,
        nullable = true))
      val customSchemaRatings = StructType(fieldsRatings)

      val ratingsDf = spark.read.option("header", "false").schema(customSchemaRatings).option("delimiter", "::")
        .csv("src/main/resources/ratings.dat")

      (moviesDf, ratingsDf)
    }
}