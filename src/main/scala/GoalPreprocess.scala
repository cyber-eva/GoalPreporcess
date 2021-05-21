import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types
import org.apache.spark.sql.SparkSession
import GoalPreprocess_assist.{regDate}

object GoalPreprocess {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GoalPreprocess").getOrCreate()
    import spark.implicits._

    //REGISTRATION
    val regDate_udf  = spark.udf.register("regDate_udf",regDate)

    val input_path :String = args(0)
    val goal       :String = args(1)

    val colsToRename:Map[String,String] = Map(
      "ga_clientid"          -> "ClientID"
    )

    val colsToAdd:Map[String,String] = Map(
      "interaction_type" -> "goal",
      "src"              -> "ga",
      "utm_source"       -> "(not set)",
      "utm_medium"       -> "(not set)",
      "utm_campaign"     -> "(not set)",
      "utm_term"         -> "(not set)",
      "utm_content"      -> "(not set)",
      "campaign_id"      -> "(not set)",
      "profile_id"       -> "(not set)",
      "creative_id"      -> "(not set)",
      "ad_id"            -> "(not set)"
    )

    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("header","true").
      option("mergeSchema","true").
      load(input_path)

    val data_work = data.
      select(
        col("ga_sessioncount").cast(sql.types.StringType),
        col("ga_clientid").cast(sql.types.StringType),
        col("ga_datehourminute").cast(sql.types.StringType)
      )

    val data_rename = colsToRename.foldLeft(data_work){
      (acc,names) => acc.withColumnRenamed(names._1,names._2)
    }

    val data_add = colsToAdd.foldLeft(data_rename){
      (acc,name) => acc.withColumn(name._1,lit(name._2))
    }

    val data_datereg = data_add.
      withColumn(
        "datehourminute",
        regDate_udf(col("ga_datehourminute"),col("interaction_type"))
      )


    val data_utc = data_datereg.
      withColumn(
        "HitTimeStamp",
        unix_timestamp(col("datehourminute"),"yyyy-MM-dd HH:mm:ss") * 1000 // milli
      ).
      withColumn("goal",lit(goal))


    val data_select = data_utc.select(
      $"interaction_type",
      $"src",
      $"ClientID",
      $"goal",
      $"HitTimeStamp",
      $"ga_sessioncount",
      $"utm_source",
      $"utm_medium",
      $"utm_campaign",
      $"utm_term",
      $"utm_content",
      $"campaign_id",
      $"profile_id",
      $"creative_id",
      $"ad_id",
      $"datehourminute"
    )


    data_select.show(20)


  }



}
