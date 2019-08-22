package org.anniejohnson.hack1
import org.apache.spark._;
import org.apache.spark.sql._;

object sfpdProgram {
case class sfpdClass(incidentnum:String, category:String, description:String, dayofweek:String, date:String, time:String, pddistrict:String, resolution:String, address:String, X:Double, Y:Double, pdid:String)
def main(args:Array[String])
{
val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Spark SQL learning")
val sc=new SparkContext(sparkconf)
val sqlc=new SQLContext(sc);
import sqlc.implicits._;//for allowing creation of dataframes

val sparksess=org.apache.spark.sql.SparkSession.builder().appName("This is a spark session application").master("local[*]").enableHiveSupport().getOrCreate()
sparksess.sparkContext.setLogLevel("ERROR")

val rdd = sparksess.sparkContext.textFile("hdfs://localhost:54310/user/hduser/hadoop/sfpd.csv")
System.out.println("rdd created")
val rddNew = rdd.map(_.split(",")).map(p => sfpdClass(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9).toDouble,p(10).toDouble,p(11)))

//make into df
val rddDF = sparksess.createDataFrame(rddNew)
System.out.println("Data frame created")
rddDF.distinct.show

//make into ds
val sfpdDS = rddDF.as[sfpdClass];
System.out.println("Dataset created")

sfpdDS.createOrReplaceTempView("sfpd");
val sfpdSelect = sparksess.sql("SELECT * FROM sfpd")
System.out.println("sfpd table created and select query is run on it: ")
sfpdSelect.show();

//7)five districts with the most number of incidents
val incByDist=sfpdDS.groupBy("incidentnum")
//or

val incByDistSQL=sparksess.sql("select incidentnum,pddistrict from sfpd sort by incidentnum desc")
incByDistSQL.show(5)

//8)top 10 resolutions of incidents
//val sql8=sparksess.sql("select count(1),resolution from sfpd group by resolution having count(1)>1 sort by resolution desc")
val sql8=sparksess.sql("select count(1),resolution from sfpd group by resolution having count(1)>1 order by 1 desc")
sql8.show(10)

//9)top three count of categories of incidents
val sql9=sparksess.sql("select count(1),category from sfpd group by category having count(1)>1 order by 1 desc")
sql9.show(3)

//10)Save the top 10 resolutions to a JSON file with proper alias name in the folder /user/hduser/.../...
sql8.write.mode("overwrite").json("file:/home/hduser/hackathon/hack1/sql8.json");

//11)Identify on the data which contains “WARRANTS” and load into HDFS /user/hduser/sfpd_parquet/ in parquet format.
val sql11=sparksess.sql("select * from sfpd where category='WARRANTS'")
sql11.show(3)
sql11.write.mode("overwrite").parquet("file:/home/hduser/hackathon/hack1/sql11.parquet");

//12)define function that extracts characters after the last ‘/’ from the string
System.out.println("12th one executed")
System.out.println(extractYear("12/12/12"))
def extractYear(date:String):String=
{
  var ct=0
  var y=""
  for(x<-0 until date.length)
  {
    if(ct==2)
      y=y+date.charAt(x)
    
    if (date.charAt(x)=='/')
      ct=ct+1;
    
  }
  
  return y
}

sparksess.udf.register("extractYear",extractYear _)

//13)Using the registered the UDF in a SQL query, find the count of incidents by year
val incyearSQL = sparksess.sql("SELECT extractYear(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY extractYear(date) ORDER BY countbyyear DESC")
incyearSQL.show(10)

//14)Find and display the category, address, and resolution of incidents reported in 2014
val inc2014 = sparksess.sql("SELECT category,address,resolution from sfpd where extractYear(date)='14'")
inc2014.show(10)

//15)Find and display the addresses and resolutions of vandalism incidents in 2015
val inc2015 = sparksess.sql("SELECT address,resolution from sfpd where extractYear(date)='15' AND category='VANDALISM'")
inc2015.show(10)

}

}


