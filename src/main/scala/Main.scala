import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, avg, col, desc, when}
import org.apache.hadoop.fs._

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
    val df_3 = df_reviews
  }
}