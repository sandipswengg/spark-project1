import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


object sparkSql {
  def main(args : Array[String]) {
    
    val conf = new SparkConf()
    .setAppName("Simple SQL Application")
    .setMaster("local")
    
    val sc = new SparkContext(conf)
  
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
      
    val df = sparkSession.read.json("userData.json").cache()
    println(df.show())
    
    println(df.groupBy("County", "Sex").count().show())
    
    val sqlContext = new SQLContext(sc);
    val csvData = sqlContext.read.csv("export.csv").cache()
   
    val df0 = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .load("export.csv")

    println(csvData.printSchema())
    println(df0.printSchema())
    
    val df_1 = df0.withColumnRenamed("depth","OLD_depth")
    val df_2 = df_1.withColumn("depth",df_1.col("OLD_depth").cast("int")).drop("OLD_depth")
    
    println(df_2.printSchema())
    val grp_data = df_2.groupBy("cut").max("depth")
    println(grp_data.show())

  }
}