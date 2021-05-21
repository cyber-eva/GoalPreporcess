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
      load(input_path)


//    val currentColumns = data.columns.toSeq
//
////    val currentColumns:List[String] = data.schema.names.map(_.toString).toList
//    println("------------",currentColumns)

    //Bad code. DEBUG!!!
//    val exploreGoalCol:String = currentColumns.filter(_.startsWith("ga_goal")).take(1)(0)
//    val goal:String = exploreGoalCol.split("ga_goal")(1).split("completions")(0)

    val data_work = data.
      select(
        col("ga_sessioncount").cast(sql.types.StringType),
        col("ga_clientid").cast(sql.types.StringType),
        col("ga_datehourminute").cast(sql.types.StringType)
      )

//    data_work.show(20)

    val data_rename = colsToRename.foldLeft(data_work){
      (acc,names) => acc.withColumnRenamed(names._1,names._2)
    }

    val data_add = colsToAdd.foldLeft(data_rename){
      (acc,name) => acc.withColumn(name._1,lit(name._2))
    }

    val data_datereg = data_add.
      withColumn(
        "datehoureminute",
        regDate_udf(col("ga_datehourminute"),col("interaction_type"))
      )

    data_datereg.show(20)

    val data_utc = data_datereg.
      withColumn(
        "HitTimeStamp",
        unix_timestamp(col("datehoureminute"),"yyyy-MM-dd HH:mm:ss") * 1000 // milli
      ).
      withColumn("goal",lit(goal))

    val added_cols:List[String] = colsToAdd.keySet.toList
    val loaded_cols:List[String] = List("ga_sessioncount",colsToRename("ga_clientid"))
    val created_cols:List[String] = List("datehoureminute","HitTimeStamp","goal")
    val selected_cols:List[String] = added_cols ++ loaded_cols ++ created_cols

    val data_select = data_utc.
      select(selected_cols.map(c => col(c)): _*)


    data_select.show(20)


  }



}
