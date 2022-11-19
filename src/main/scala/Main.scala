import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast._
import org.apache.spark.ml.recommendation._
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("MusicRecommender")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rawUserArtistData =
      spark.read.textFile("./data/user_artist_data_small.txt")

    val userArtistDF = rawUserArtistData
      .map { line =>
        val Array(user, artist, _*) = line.split(' ')
        (user.toInt, artist.toInt)
      }
      .toDF("user", "artist")

    userArtistDF
      .agg(
        min("user"),
        max("user"),
        min("artist"),
        max("artist")
      )

    val rawArtistData = spark.read.textFile("./data/artist_data_small.txt")

    rawArtistData
      .map { line =>
        val (id, name) = line.span(_ != '\t')
        (id.toInt, name.trim)
      }
      .count()

    val artistByID = rawArtistData
      .flatMap { line =>
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty()) {
          None
        } else {
          try {
            Some((id.toInt, name.trim))
          } catch {
            case _: NumberFormatException => None
          }
        }
      }
      .toDF("id", "name")

    val rawArtistAlias = spark.read.textFile("./data/artist_alias_small.txt")
    val artistAlias = rawArtistAlias
      .flatMap { line =>
        val Array(artist, alias) = line.split('\t')
        if (artist.isEmpty()) {
          None
        } else {
          Some((artist.toInt, alias.toInt))
        }
      }
      .collect()
      .toMap

    def buildCounts(
        rawUserArtistData: Dataset[String],
        bArtistAlias: Broadcast[Map[Int, Int]]
    ): DataFrame = {
      rawUserArtistData
        .map { line =>
          val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
          val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
          (userID, finalArtistID, count)
        }
        .toDF("user", "artist", "count")
    }

    val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
    val trainData = buildCounts(rawUserArtistData, bArtistAlias)
    trainData.cache()

    val model = new ALS()
      .setSeed(Random.nextLong())
      .setImplicitPrefs(true)
      .setRank(10)
      .setRegParam(0.01)
      .setAlpha(1.0)
      .setMaxIter(5)
      .setUserCol("user")
      .setItemCol("artist")
      .setRatingCol("count")
      .setPredictionCol("prediction")
      .fit(trainData)

    val userID = 2093760

    val existingArtistIDs = trainData.filter($"user" === userID).select("artist").as[Int].collect()

    artistByID.filter($"id" isin (existingArtistIDs:_*)).show()

    def makeRecommendations(
        model: ALSModel,
        userID: Int,
        howMany: Int
    ): DataFrame = {
      val toRecommend = model.itemFactors
        .select($"id".as("artist"))
        .withColumn("user", lit(userID))

      model
        .transform(toRecommend)
        .select("artist", "prediction")
        .orderBy($"prediction".desc)
        .limit(howMany)
    }

    // val topRecommendations = makeRecommendations(model, userID, 5)
    // topRecommendations.show()
  }
}
