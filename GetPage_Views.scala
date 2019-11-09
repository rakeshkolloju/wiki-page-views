package sample
import java.text.SimpleDateFormat
import java.util._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object GetPage_Views {

  def main(args: Array[String]): Unit = {
    //spark2-submit --master yarn --deploy-mode client  --class GetPage_Views /linuxPath/GetPage_Views.jar 20190101 01 5
    //https://dumps.wikimedia.org/other/pageviews/
   // https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
    //pageviews/2019/2019-01/pageviews-20190101-000000.gz

    println("cli-format  yyyymmdd time topN ")
    println("Ex: 20190101 01 5")
     if (args.length < 3) println("Please Check the format yyyymmdd time topN")
    val spark = SparkSession
      .builder()
      .appName("GetPage_Views")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    var dt="20190101" //args(0)
    var time="01"  //args(1)
    var topN=5  //args(2).toInt

    val hdfspath="dev/product/can_staging/test_rk/wiki/"
    //Construct the hdfs path based on the input date and time 
    var filepath= hdfspath+dt.substring(0,4)+"/"+dt.substring(0,4)+"-"+dt.substring(6,8)
                filepath+= "/pageviews-"+dt+"-0000"+time+".gz"
    //var filepath="hdfspath"+"/2019/2019-01/pageviews-20190101-000000.gz"
    //dev/product/can_staging/test_rk/wiki/2019/2019-01/pageviews-20190101-000000.gz
    var blacklistpath="hdfspath"+"/blacklist.txt"


   val df= spark.sparkContext.textFile(filepath)

   import spark.implicits._
   val pages= df.map(line=>{
         val fields= line.split(" ")
         (fields(0),fields(1),fields(2).toLong)
    }).toDF("domain","page","view_count")

    val blacklistdf = spark.read
                .option("header", "false")
                .option("delimiter", " ")
                .option("inferSchema", "true")
                .format("csv")
                .load(blacklistpath)
    val bdf=blacklistdf.toDF("domain","page")


    val page_views=pages.groupBy($"domain",$"page")
                .agg(sum($"view_count").as("view_count"))
                .select($"domain",$"page",$"view_count")
                .orderBy($"view_count".desc_nulls_last,$"domain".asc_nulls_last)

     //Take off the Black listed Pages
    val page_views_black=page_views
                        .join(bdf,Seq("domain","page"),"leftanti")
                        .limit(topN)

    val format = new SimpleDateFormat("dd_MM_yyyy_hh_mm_ss").format(Calendar.getInstance().getTime())

    // Saving the dataframe into the current runtime filename
    page_views_black.coalesce(1).write.mode(SaveMode.ErrorIfExists).csv(hdfspath+"/"+format+".csv")

    //output - Result
    //    en,Main_Page,211234
    //    en.m,Main_Page,85577
    //    en.m,Suggs_(singer),48654
    //    en.m,Madness_(band),42399
    //    en,-,38120

  }

}
