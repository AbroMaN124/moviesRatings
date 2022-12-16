package uk.co.newday.solutions

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, functions}

object Exercise2Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    // Get the min, max and average rating of each movie, as well as their original movies fields (MovieId, Title, Genres)
    movies.join(ratings, movies.col("MovieID") === ratings.col("MovieID"), "left")
      .drop(ratings.col("MovieID")).groupBy("MovieID", "Title", "Genres")
      .agg(functions.min("Rating"), functions.max("Rating"), avg("Rating")
      )

  }
}
