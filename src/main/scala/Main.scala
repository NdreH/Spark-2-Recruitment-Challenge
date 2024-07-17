
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, col, collect_list, count, desc, explode, max, udf}
import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType, StructField, StructType}

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark RecruitmentChallenge")
      .master("local[*]")
      .getOrCreate()

    val df_reviews = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/googleplaystore_user_reviews.csv")

    //    df_reviews.show()
    //    df_reviews.printSchema()

    val df_apps = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/googleplaystore.csv")

    //    df_apps.show()
    //    df_apps.printSchema()
    //
        // Exercises

        // Part 1
        val df_1 = df_reviews.where(col("Sentiment_Polarity") =!= "nan").groupBy("App").agg(avg("Sentiment_Polarity").alias("Sentiment_Polarity_Avg"))

        df_1.show()
        df_1.printSchema()

        // Part 2
        val df_2 = df_apps.filter(col("Rating") >= 4.0).filter(!col("Rating").isNaN).orderBy(desc("Rating"))

        df_2.show()
        df_2.printSchema()

        df_2
          .write
          .option("header", value = true)
          .option("sep", value = "§")
          .mode(saveMode = "overwrite")
          .csv("data/best_apps")

    // Part 3

    import spark.implicits._

    def toMb(number: String): Double = {
      if (number.endsWith("M")) {
        number.dropRight(1).toDouble
      } else if (number.endsWith("k")) {
        number.dropRight(1).toDouble / 1024
      } else {
        Double.NaN
      }
    }

    def getGenres(genres_list: String): Array[String] = {
      genres_list.split(";")
    }

    def toDate(date_str: String): Date = {
      val inputFormat = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH)
      val outputFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy")

      val isValid = try {
        LocalDate.parse(date_str, inputFormat)
        true // Parsing successful
      } catch {
        case _: Exception => false // Parsing failed
      }

      if (isValid) {
        val localDate = LocalDate.parse(date_str, inputFormat)
        val formattedDate = localDate.format(outputFormat)

        Date.valueOf(LocalDate.parse(formattedDate, outputFormat))
      } else {
        null
      }

    }

    val toMb_udf = udf(toMb _)
    var df_3 = df_apps.withColumn("Size", toMb_udf(col("Size")))

    df_3 = df_3.withColumn("Price", col("Price") * 0.9)

    val getGenres_udf = udf(getGenres _)
    df_3 = df_3.withColumn("Genres", getGenres_udf(col("Genres")))

    val toDate_udf = udf(toDate _)
    df_3 = df_3.withColumn("Last Updated", toDate_udf(col("Last Updated")))

    val maxReviewsAndCategoriesDF = df_3.groupBy("App")
      .agg(
        max("Reviews").alias("max_Reviews"),
        collect_list("Category").alias("Categories")
      )

    df_3 = df_3.join(maxReviewsAndCategoriesDF, Seq("App"))
      .where($"Reviews" === $"max_Reviews").drop("Category").drop("max_Reviews")

    // Part 4

    val df_3plus1 = df_3.join(df_1, Seq("App"))
    df_3plus1.show()
    df_3plus1.write
      .option("compression", "gzip")
      .parquet("data/googleplaystore_cleaned")

    // Part 5

    // Explode the genres array into separate rows
    val df_4 = df_3plus1.withColumn("Genre", explode($"Genres"))

    // Group by the exploded genre column

    df_4.show()

    val groupedDF = df_4.groupBy("Genre")
      .agg(
        count("*").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Sentiment_Polarity_Avg").alias("Average_Sentiment_Polarity")
      )

    // Show the resulting DataFrame
    groupedDF.show(truncate = false)
  }
}