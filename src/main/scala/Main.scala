
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, collect_list, count, desc, explode, max, udf}

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale


object Main {
  def main(args: Array[String]): Unit = {

    // Build Spark Session
    val spark = SparkSession.builder()
      .appName("Spark RecruitmentChallenge")
      .master("local[*]")
      .getOrCreate()

    // Read csv files into dataframes

    val df_reviews = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/googleplaystore_user_reviews.csv")

    df_reviews.show()
    df_reviews.printSchema()

    val df_apps = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/googleplaystore.csv")

    df_apps.show()
    df_apps.printSchema()

    // Exercises

    // Part 1

    val df_1 = df_reviews
      .where(col("Sentiment_Polarity") =!= "nan")
      .groupBy("App")
      .agg(avg("Sentiment_Polarity")
      .alias("Sentiment_Polarity_Avg"))

    df_1.show()
    df_1.printSchema()

    // Part 2

    val df_2 = df_apps
      .filter(col("Rating") >= 4.0)
      .filter(!col("Rating").isNaN)
      .orderBy(desc("Rating"))

    df_2.show()
    df_2.printSchema()

    // Write dataframe to csv file
    df_2
      .write
      .option("header", value = true)
      .option("sep", value = "§")
      .mode(saveMode = "overwrite")
      .csv("data/best_apps")

    // Part 3

    import spark.implicits._

    // Function to convert a String that represents the size in bytes into a Double
    def toMb(number: String): Double = {
      if (number.endsWith("M")) {
        number.dropRight(1).toDouble
      } else if (number.endsWith("k")) {
        number.dropRight(1).toDouble / 1024
      } else {
        Double.NaN
      }
    }

    // Function to separate a String of genres into an array of Strings
    def getGenres(genres_list: String): Array[String] = {
      genres_list.split(";")
    }

    // Function to convert a string that holds a date in the format MMMM d, yyyy to a date object in the format dd/MM/yyyy
    def toDate(date_str: String): Date = {
      val inputFormat = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH)
      val outputFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy")

      val isValid = try {
        LocalDate.parse(date_str, inputFormat)
        true
      } catch {
        case _: Exception => false
      }

      if (isValid) {
        val localDate = LocalDate.parse(date_str, inputFormat)
        val formattedDate = localDate.format(outputFormat)

        Date.valueOf(LocalDate.parse(formattedDate, outputFormat))
      } else {
        null
      }

    }

    val toMb_udf = udf(toMb _) // toMb function udf
    var df_3 = df_apps.withColumn("Size", toMb_udf(col("Size"))) // Applying udf to the Size column

    df_3 = df_3.withColumn("Price", col("Price") * 0.9) // Converting Dollars to Euros on the Price column

    val getGenres_udf = udf(getGenres _) // getGenres function udf
    df_3 = df_3.withColumn("Genres", getGenres_udf(col("Genres"))) // Applying udf to the Genres column

    val toDate_udf = udf(toDate _) // toDate function udf
    df_3 = df_3.withColumn("Last Updated", toDate_udf(col("Last Updated"))) // Applying udf to the Last_Updated column

    // Group dataframe by App name and storing the maximum reviews for each app, as well as the list of all categories
    val maxReviewsAndCategoriesDF = df_3.groupBy("App")
      .agg(
        max("Reviews").alias("max_Reviews"),
        collect_list("Category").alias("Categories")
      )

    // Join dataframes on App name and drop obsolete columns
    df_3 = df_3.join(maxReviewsAndCategoriesDF, Seq("App"))
      .where($"Reviews" === $"max_Reviews").drop("Category").drop("max_Reviews")

    // Part 4

    // Join dataframes
    val df_3plus1 = df_3.join(df_1, Seq("App"))
    df_3plus1.show()

    // Save dataframe as a parquet file, using gzip compression
    df_3plus1.write
      .option("compression", "gzip")
      .parquet("data/googleplaystore_cleaned")

    // Part 5

    // Explode the genres array into separate rows
    var df_4 = df_3plus1.withColumn("Genre", explode($"Genres"))

    // Group dataframe by Genre, and store information about the count of apps, the average of rating and the average of
    // sentiment polarity among genres
    df_4 = df_4.groupBy("Genre")
      .agg(
        count("*").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Sentiment_Polarity_Avg").alias("Average_Sentiment_Polarity")
      )

    df_4.show()
  }
}