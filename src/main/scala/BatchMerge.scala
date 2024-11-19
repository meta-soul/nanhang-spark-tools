import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object BatchMerge {

  val modelToTables: Map[String, Array[String]] = Map(
    "10411" -> Array("hdfs://10.64.219.26:9000/tmp/ddf/dws_qara_10411", "czods.s_qara_10411_16hz", "czods.s_qara_10411_1hz", "czods.s_qara_10411_2hz", "czods.s_qara_10411_32hz", "czods.s_qara_10411_4hz", "czods.s_qara_10411_8hz", "czods.s_qara_10411_p25hz", "czods.s_qara_10411_p5hz"),
    "10578" -> Array("hdfs://10.64.219.26:9000/tmp/ddf/dws_qara_10578", "czods.s_qara_10578_1hz", "czods.s_qara_10578_2hz", "czods.s_qara_10578_32hz", "czods.s_qara_10578_4hz", "czods.s_qara_10578_8hz", "czods.s_qara_10578_p25hz", "czods.s_qara_10578_p5hz")
    //    "103251" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_103251", "czods.s_qara_103251_1hz", "czods.s_qara_103251_2hz", "czods.s_qara_103251_4hz", "czods.s_qara_103251_8hz", "czods.s_qara_103251_p25hz", "czods.s_qara_103251_p5hz"),
    //    "10410" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10410", "czods.s_qara_10410_16hz", "czods.s_qara_10410_1hz", "czods.s_qara_10410_2hz", "czods.s_qara_10410_32hz", "czods.s_qara_10410_4hz", "czods.s_qara_10410_8hz", "czods.s_qara_10410_p25hz", "czods.s_qara_10410_p5hz"),
    //    "10411" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10411", "czods.s_qara_10411_16hz", "czods.s_qara_10411_1hz", "czods.s_qara_10411_2hz", "czods.s_qara_10411_32hz", "czods.s_qara_10411_4hz", "czods.s_qara_10411_8hz", "czods.s_qara_10411_p25hz", "czods.s_qara_10411_p5hz"),
    //    "10412" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10412", "czods.s_qara_10412_16hz", "czods.s_qara_10412_1hz", "czods.s_qara_10412_2hz", "czods.s_qara_10412_32hz", "czods.s_qara_10412_4hz", "czods.s_qara_10412_8hz", "czods.s_qara_10412_p25hz", "czods.s_qara_10412_p5hz"),
    //    "10413" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10413", "czods.s_qara_10413_16hz", "czods.s_qara_10413_1hz", "czods.s_qara_10413_2hz", "czods.s_qara_10413_32hz", "czods.s_qara_10413_4hz", "czods.s_qara_10413_8hz", "czods.s_qara_10413_p25hz", "czods.s_qara_10413_p5hz"),
    //    "10576" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10576", "czods.s_qara_10576_1hz", "czods.s_qara_10576_2hz", "czods.s_qara_10576_32hz", "czods.s_qara_10576_4hz", "czods.s_qara_10576_8hz", "czods.s_qara_10576_p25hz", "czods.s_qara_10576_p5hz"),
    //    "10578" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10578", "czods.s_qara_10578_1hz", "czods.s_qara_10578_2hz", "czods.s_qara_10578_32hz", "czods.s_qara_10578_4hz", "czods.s_qara_10578_8hz", "czods.s_qara_10578_p25hz", "czods.s_qara_10578_p5hz"),
    //    "10813" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10813", "czods.s_qara_10813_1hz", "czods.s_qara_10813_2hz", "czods.s_qara_10813_4hz", "czods.s_qara_10813_8hz", "czods.s_qara_10813_p25hz", "czods.s_qara_10813_p5hz"),
    //    "10876" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10876", "czods.s_qara_10876_1hz", "czods.s_qara_10876_2hz", "czods.s_qara_10876_4hz", "czods.s_qara_10876_8hz", "czods.s_qara_10876_p25hz", "czods.s_qara_10876_p5hz"),
    //    "10886" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10886", "czods.s_qara_10886_1hz", "czods.s_qara_10886_2hz", "czods.s_qara_10886_4hz", "czods.s_qara_10886_8hz", "czods.s_qara_10886_p25hz", "czods.s_qara_10886_p5hz"),
    //    "320020" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_320020", "czods.s_qara_320020_1hz", "czods.s_qara_320020_2hz", "czods.s_qara_320020_4hz", "czods.s_qara_320020_8hz", "czods.s_qara_320020_p25hz", "czods.s_qara_320020_p5hz"),
    //    "320210" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_320210", "czods.s_qara_320210_1hz", "czods.s_qara_320210_2hz", "czods.s_qara_320210_32hz", "czods.s_qara_320210_4hz", "czods.s_qara_320210_8hz", "czods.s_qara_320210_p25hz", "czods.s_qara_320210_p5hz"),
    //    "320232" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_320232", "czods.s_qara_320232_1hz", "czods.s_qara_320232_2hz", "czods.s_qara_320232_4hz", "czods.s_qara_320232_8hz", "czods.s_qara_320232_p25hz", "czods.s_qara_320232_p5hz"),
    //    "40026" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_40026", "czods.s_qara_40026_1hz", "czods.s_qara_40026_2hz", "czods.s_qara_40026_32hz", "czods.s_qara_40026_4hz", "czods.s_qara_40026_8hz", "czods.s_qara_40026_p25hz", "czods.s_qara_40026_p5hz"),
    //    "40036" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_40036", "czods.s_qara_40036_1hz", "czods.s_qara_40036_2hz", "czods.s_qara_40036_32hz", "czods.s_qara_40036_4hz", "czods.s_qara_40036_8hz", "czods.s_qara_40036_p25hz", "czods.s_qara_40036_p5hz"),
    //    "4028" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4028", "czods.s_qara_4028_16hz", "czods.s_qara_4028_1hz", "czods.s_qara_4028_2hz", "czods.s_qara_4028_4hz", "czods.s_qara_4028_8hz", "czods.s_qara_4028_p25hz ", "czods.s_qara_4028_p5hz"),
    //    "4029" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4029", "czods.s_qara_4029_16hz", "czods.s_qara_4029_1hz", "czods.s_qara_4029_2hz", "czods.s_qara_4029_32hz", "czods.s_qara_4029_4hz", "czods.s_qara_4029_8hz", "czods.s_qara_4029_p25hz", "czods.s_qara_4029_p5hz"),
    //    "4030" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4030", "czods.s_qara_4030_1hz", "czods.s_qara_4030_2hz", "czods.s_qara_4030_32hz", "czods.s_qara_4030_4hz", "czods.s_qara_4030_8hz", "czods.s_qara_4030_p25hz", "czods.s_qara_4030_p5hz"),
    //    "4031" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4031", "czods.s_qara_4031_1hz", "czods.s_qara_4031_2hz", "czods.s_qara_4031_32hz", "czods.s_qara_4031_4hz", "czods.s_qara_4031_8hz", "czods.s_qara_4031_p25hz", "czods.s_qara_4031_p5hz"),
    //    "467" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_467", "czods.s_qara_467_16hz", "czods.s_qara_467_1hz", "czods.s_qara_467_2hz", "czods.s_qara_467_32hz", "czods.s_qara_467_4hz", "czods.s_qara_467_8hz", "czods.s_qara_467_p25hz", "czods.s_qara_467_p5hz"),
    //    "477" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_477", "czods.s_qara_477_16hz", "czods.s_qara_477_1hz", "czods.s_qara_477_2hz", "czods.s_qara_477_32hz", "czods.s_qara_477_4hz", "czods.s_qara_477_8hz", "czods.s_qara_477_p25hz", "czods.s_qara_477_p5hz"),
    //    "77712" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_77712", "czods.s_qara_77712_16hz", "czods.s_qara_77712_1hz", "czods.s_qara_77712_2hz", "czods.s_qara_77712_32hz", "czods.s_qara_77712_4hz", "czods.s_qara_77712_8hz", "czods.s_qara_77712_p25hz", "czods.s_qara_77712_p5hz"),
    //    "787151" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_787151", "czods.s_qara_787151_16hz", "czods.s_qara_787151_1hz", "czods.s_qara_787151_2hz", "czods.s_qara_787151_4hz", "czods.s_qara_787151_8hz", "czods.s_qara_787151_p25hz", "czods.s_qara_787151_p5hz"),
    //    "78727" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_78727", "czods.s_qara_78727_16hz", "czods.s_qara_78727_1hz", "czods.s_qara_78727_2hz", "czods.s_qara_78727_4hz", "czods.s_qara_78727_8hz", "czods.s_qara_78727_p25hz"),
    //    "919040" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_919040", "czods.s_qara_919040_1hz", "czods.s_qara_919040_2hz", "czods.s_qara_919040_32hz", "czods.s_qara_919040_4hz", "czods.s_qara_919040_8hz", "czods.s_qara_919040_p25hz", "czods.s_qara_919040_p5hz"),
    //    "919051" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_919051", "czods.s_qara_919051_16hz", "czods.s_qara_919051_1hz", "czods.s_qara_919051_2hz", "czods.s_qara_919051_4hz", "czods.s_qara_919051_8hz", "czods.s_qara_919051_p25hz", "czods.s_qara_919051_p5hz")
  )

  def main(args: Array[String]): Unit = {
    val timeZone = "Asia/Shanghai"
    val mysqlUrl = "jdbc:mysql://rm-7uq470g1358b7f9u7.mysql.rds.inner.y.csair.com:3306/hive?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + timeZone
    val mysqlUserName = "hive_app"
    val mysqlPassword = "hi#v2e&8APp"
    val tableName = "qarfile_extract_aj_log_detail"

//    val mysqlUrl = "jdbc:mysql://localhost:3306/ddf_2?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + timeZone
//    val mysqlUserName = "root"
//    val mysqlPassword = "123456"
//    val tableName = "qara_info"

    val builder = SparkSession.builder()
      .config("spark.sql.parquet.mergeSchema", value = true)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.dmetasoul.lakesoul.schema.autoMerge.enabled", "true")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val nowTime = dataFormat.format(new Date)
    val cal1 = Calendar.getInstance()
    cal1.setTime(dataFormat.parse(nowTime))
    cal1.add(Calendar.HOUR, -2)
    val beforeTime = dataFormat.format(cal1.getTime)
    println("search start time: " + beforeTime)
    println("search end time: " + nowTime)

    val mysqlDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", mysqlUrl)
      .option("dbtable", tableName)
      .option("user", mysqlUserName)
      .option("numPartitions", "16")
      .option("password", mysqlPassword)
      .load()

    val partition = mysqlDF
      .select("file_no", "tail_num", "flt_dt", "model_type", "parsing_time")
      .filter(s"parsing_time >= '$beforeTime' and parsing_time <= '$nowTime'")
      .filter(s"model_type = '10411' or model_type = '10578'")
    partition.show()

    val rows = partition.rdd.collect()
    rows.foreach(row => {
      val modelType = row.getString(3)
      val fileNo = row.getString(0)
      val tailNum = row.getString(1)
      val fltDt = row.getString(2)

      val tableArray = modelToTables(modelType)
      val tablePath = tableArray(0)
      val sinkTable = LakeSoulTable.forPath(tablePath)
      println(dataFormat.format(new Date) + ", start new partition")
      for (i <- 1 to tableArray.length - 1) {
        val tableName = tableArray(i)
        val ss = s"select * from $tableName where flt_dt= '$fltDt' and tail_num = '$tailNum' and file_no = '$fileNo'"
        println(ss)
        val data = spark.sql(ss).toDF()
        sinkTable.upsert(data)
      }
    })


    //    val data = spark.sparkContext.parallelize(rows, 8)
    //    data.foreachPartition(iter => {
    //      iter.foreach(row => {
    //        val modelType = row.getString(3)
    //        val fileNo = row.getString(0)
    //        val tailNum = row.getString(1)
    //        val fltDt = row.getString(2)
    //
    //        val tableArray = modelToTables(modelType)
    //        val tablePath = tableArray(0)
    ////        val sinkTable = LakeSoulTable.forPath(tablePath)
    //        val sinkTable = LakeSoulTable.forPath("file:/Users/dudongfeng/work/zehy/lakesoul/test_table")
    //        for (i <- 1 to tableArray.length - 1) {
    //          val tableName = tableArray(i)
    //          val ss = s"select * from $tableName where flt_dt= '$fltDt' and tail_num = '$tailNum' and file_no = '$fileNo'"
    //          println(ss)
    ////          val data = spark.sql(ss).toDF()
    ////          sinkTable.upsert(data)
    //        }
    //      })
    //    })
  }
}
