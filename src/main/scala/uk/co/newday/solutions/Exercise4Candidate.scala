package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame

object Exercise4Candidate {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame) = {
    movieRatings.write.parquet("output/movieRatings")
  }
}
