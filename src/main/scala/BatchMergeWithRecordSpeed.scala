import com.dmetasoul.lakesoul.meta.entity.PartitionInfo
import com.dmetasoul.lakesoul.meta.{DBManager, DBUtil}
import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.{Row, SparkSession}

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Date, Properties}

object BatchMergeWithRecordSpeed {
  private val MODEL_TYPE: String = "model_type"
  private val BEGIN_TIME: String = "begin_day"
  private val END_DAY: String = "end_day"
  private val BATCH_SIZE: String = "batch_size"

  private val dataFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private val modelToTables: Map[String, Array[String]] = Map(
    "103251" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_103251", "czods.s_qara_103251_1hz", "czods.s_qara_103251_2hz", "czods.s_qara_103251_4hz", "czods.s_qara_103251_8hz", "czods.s_qara_103251_p25hz", "czods.s_qara_103251_p5hz"),
    "10410" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10410", "czods.s_qara_10410_16hz", "czods.s_qara_10410_1hz", "czods.s_qara_10410_2hz", "czods.s_qara_10410_32hz", "czods.s_qara_10410_4hz", "czods.s_qara_10410_8hz", "czods.s_qara_10410_p25hz", "czods.s_qara_10410_p5hz"),
    "10411" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10411", "czods.s_qara_10411_16hz", "czods.s_qara_10411_1hz", "czods.s_qara_10411_2hz", "czods.s_qara_10411_32hz", "czods.s_qara_10411_4hz", "czods.s_qara_10411_8hz", "czods.s_qara_10411_p25hz", "czods.s_qara_10411_p5hz"),
    "10412" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10412", "czods.s_qara_10412_16hz", "czods.s_qara_10412_1hz", "czods.s_qara_10412_2hz", "czods.s_qara_10412_32hz", "czods.s_qara_10412_4hz", "czods.s_qara_10412_8hz", "czods.s_qara_10412_p25hz", "czods.s_qara_10412_p5hz"),
    "10413" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10413", "czods.s_qara_10413_16hz", "czods.s_qara_10413_1hz", "czods.s_qara_10413_2hz", "czods.s_qara_10413_32hz", "czods.s_qara_10413_4hz", "czods.s_qara_10413_8hz", "czods.s_qara_10413_p25hz", "czods.s_qara_10413_p5hz"),
    "10576" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10576", "czods.s_qara_10576_1hz", "czods.s_qara_10576_2hz", "czods.s_qara_10576_32hz", "czods.s_qara_10576_4hz", "czods.s_qara_10576_8hz", "czods.s_qara_10576_p25hz", "czods.s_qara_10576_p5hz"),
    "10578" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10578", "czods.s_qara_10578_1hz", "czods.s_qara_10578_2hz", "czods.s_qara_10578_32hz", "czods.s_qara_10578_4hz", "czods.s_qara_10578_8hz", "czods.s_qara_10578_p25hz", "czods.s_qara_10578_p5hz"),
    "10813" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10813", "czods.s_qara_10813_1hz", "czods.s_qara_10813_2hz", "czods.s_qara_10813_4hz", "czods.s_qara_10813_8hz", "czods.s_qara_10813_p25hz", "czods.s_qara_10813_p5hz"),
    "10876" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10876", "czods.s_qara_10876_1hz", "czods.s_qara_10876_2hz", "czods.s_qara_10876_4hz", "czods.s_qara_10876_8hz", "czods.s_qara_10876_p25hz", "czods.s_qara_10876_p5hz"),
    "10886" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_10886", "czods.s_qara_10886_1hz", "czods.s_qara_10886_2hz", "czods.s_qara_10886_4hz", "czods.s_qara_10886_8hz", "czods.s_qara_10886_p25hz", "czods.s_qara_10886_p5hz"),
    "320020" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_320020", "czods.s_qara_320020_1hz", "czods.s_qara_320020_2hz", "czods.s_qara_320020_4hz", "czods.s_qara_320020_8hz", "czods.s_qara_320020_p25hz", "czods.s_qara_320020_p5hz"),
    "320210" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_320210", "czods.s_qara_320210_1hz", "czods.s_qara_320210_2hz", "czods.s_qara_320210_32hz", "czods.s_qara_320210_4hz", "czods.s_qara_320210_8hz", "czods.s_qara_320210_p25hz", "czods.s_qara_320210_p5hz"),
    "320232" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_320232", "czods.s_qara_320232_1hz", "czods.s_qara_320232_2hz", "czods.s_qara_320232_4hz", "czods.s_qara_320232_8hz", "czods.s_qara_320232_p25hz", "czods.s_qara_320232_p5hz"),
    "40026" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_40026", "czods.s_qara_40026_1hz", "czods.s_qara_40026_2hz", "czods.s_qara_40026_32hz", "czods.s_qara_40026_4hz", "czods.s_qara_40026_8hz", "czods.s_qara_40026_p25hz", "czods.s_qara_40026_p5hz"),
    "40036" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_40036", "czods.s_qara_40036_1hz", "czods.s_qara_40036_2hz", "czods.s_qara_40036_32hz", "czods.s_qara_40036_4hz", "czods.s_qara_40036_8hz", "czods.s_qara_40036_p25hz", "czods.s_qara_40036_p5hz"),
    "4028" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4028", "czods.s_qara_4028_16hz", "czods.s_qara_4028_1hz", "czods.s_qara_4028_2hz", "czods.s_qara_4028_4hz", "czods.s_qara_4028_8hz", "czods.s_qara_4028_p25hz ", "czods.s_qara_4028_p5hz"),
    "4029" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4029", "czods.s_qara_4029_16hz", "czods.s_qara_4029_1hz", "czods.s_qara_4029_2hz", "czods.s_qara_4029_32hz", "czods.s_qara_4029_4hz", "czods.s_qara_4029_8hz", "czods.s_qara_4029_p25hz", "czods.s_qara_4029_p5hz"),
    "4030" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4030", "czods.s_qara_4030_1hz", "czods.s_qara_4030_2hz", "czods.s_qara_4030_32hz", "czods.s_qara_4030_4hz", "czods.s_qara_4030_8hz", "czods.s_qara_4030_p25hz", "czods.s_qara_4030_p5hz"),
    "4031" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_4031", "czods.s_qara_4031_1hz", "czods.s_qara_4031_2hz", "czods.s_qara_4031_32hz", "czods.s_qara_4031_4hz", "czods.s_qara_4031_8hz", "czods.s_qara_4031_p25hz", "czods.s_qara_4031_p5hz"),
    "467" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_467", "czods.s_qara_467_16hz", "czods.s_qara_467_1hz", "czods.s_qara_467_2hz", "czods.s_qara_467_32hz", "czods.s_qara_467_4hz", "czods.s_qara_467_8hz", "czods.s_qara_467_p25hz", "czods.s_qara_467_p5hz"),
    "477" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_477", "czods.s_qara_477_16hz", "czods.s_qara_477_1hz", "czods.s_qara_477_2hz", "czods.s_qara_477_32hz", "czods.s_qara_477_4hz", "czods.s_qara_477_8hz", "czods.s_qara_477_p25hz", "czods.s_qara_477_p5hz"),
    "77712" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_77712", "czods.s_qara_77712_16hz", "czods.s_qara_77712_1hz", "czods.s_qara_77712_2hz", "czods.s_qara_77712_32hz", "czods.s_qara_77712_4hz", "czods.s_qara_77712_8hz", "czods.s_qara_77712_p25hz", "czods.s_qara_77712_p5hz"),
    "787151" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_787151", "czods.s_qara_787151_16hz", "czods.s_qara_787151_1hz", "czods.s_qara_787151_2hz", "czods.s_qara_787151_4hz", "czods.s_qara_787151_8hz", "czods.s_qara_787151_p25hz", "czods.s_qara_787151_p5hz"),
    "78727" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_78727", "czods.s_qara_78727_16hz", "czods.s_qara_78727_1hz", "czods.s_qara_78727_2hz", "czods.s_qara_78727_4hz", "czods.s_qara_78727_8hz", "czods.s_qara_78727_p25hz"),
    "919040" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_919040", "czods.s_qara_919040_1hz", "czods.s_qara_919040_2hz", "czods.s_qara_919040_32hz", "czods.s_qara_919040_4hz", "czods.s_qara_919040_8hz", "czods.s_qara_919040_p25hz", "czods.s_qara_919040_p5hz"),
    "919051" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_919051", "czods.s_qara_919051_16hz", "czods.s_qara_919051_1hz", "czods.s_qara_919051_2hz", "czods.s_qara_919051_4hz", "czods.s_qara_919051_8hz", "czods.s_qara_919051_p25hz", "czods.s_qara_919051_p5hz")
  )

  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    val modelType = parameter.get(MODEL_TYPE)
    val beginDay = parameter.get(BEGIN_TIME)
    val endDay = parameter.get(END_DAY)
    val batchSize = parameter.getInt(BATCH_SIZE, 10)
    if (modelType == null) {
      println("=" * 50)
      println(MODEL_TYPE + " is request! ")
      println("=" * 50)
      System.exit(1)
    }
    if (!modelToTables.contains(modelType)) {
      println("*" * 50)
      println(MODEL_TYPE + " is not exists! ")
      println("*" * 50)
      System.exit(1)
    }
    if (beginDay == null) {
      println("=" * 50)
      println(BEGIN_TIME + " is request! ")
      System.exit(1)

    }
    if (endDay == null) {
      println("=" * 50)
      println(END_DAY + " is request! ")
      println("=" * 50)
      System.exit(1)
    }

    val timeZone = "Asia/Shanghai"

    val mysqlUrl = "jdbc:mysql://rm-7uq470g1358b7f9u7.mysql.rds.inner.y.csair.com:3306/hive?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + timeZone
    val mysqlUserName = "hive_app"
    val mysqlPassword = "hi#v2e&8APp"
    val tableName = "qarfile_merge_record"

    //    val mysqlUrl = "jdbc:mysql://localhost:3306/ddf_2?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + timeZone
    //    val mysqlUserName = "root"
    //    val mysqlPassword = "123456"
    //    val tableName = "qarfile_merge_record"

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

    val selectTimeRange = getListDay(beginDay, endDay)

    val mysqlConnectionProperties = new Properties()
    mysqlConnectionProperties.put("user", mysqlUserName)
    mysqlConnectionProperties.put("password", mysqlPassword)
    mysqlConnectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")
    val mysqlTableData = spark.read.jdbc(mysqlUrl, tableName, mysqlConnectionProperties)

    val sourceTable = modelToTables(modelType)(1)

    import scala.collection.JavaConverters._
    val needMergePartition = getPartitions(sourceTable, selectTimeRange).asScala.toList
    val filterPartitionRDD = spark.sparkContext.parallelize(needMergePartition)

    val mergedPartitions =
      mysqlTableData
        .select("flt_dt", "tail_num", "file_no")
        .filter(s"model_type = $modelType")
        .filter(s"flt_dt >= '$beginDay' and flt_dt <= '$endDay'")
    val needPartitions = filterPartitionRDD.subtract(mergedPartitions.rdd)

    val rows = needPartitions.collect()
    println("*" * 15 + " need merge partition size: " + rows.length)
    var size = 0
    var multiFltDt = ""
    var multiTailNum = ""
    var multiFileNo = ""
    var seqList: Seq[(String, String, String, String)] = Seq()
    rows.foreach(row => {
      val fltDt = row.getString(0)
      val tailNum = row.getString(1)
      val fileNo = row.getString(2)

      val tableArray = modelToTables(modelType)
      val tablePath = tableArray(0)
      val sinkTable = LakeSoulTable.forPath(tablePath)

      multiFltDt = if (multiFltDt == "") "'" + fltDt + "'" else multiFltDt + ", '" + fltDt + "'"
      multiTailNum = if (multiTailNum == "") "'" + tailNum + "'" else multiTailNum + ", '" + tailNum + "'"
      multiFileNo = if (multiFileNo == "") "'" + fileNo + "'" else multiFileNo + ", '" + fileNo + "'"
      size = size + 1
      seqList = seqList :+ (modelType, fltDt, tailNum, fileNo)
      if (size == batchSize) {
        println(dataFormat.format(new Date) + " ---------- start new partition, flt_dt: " + fltDt + ", tail_num: " + tailNum + ", file_no: " + fileNo)
        for (i <- 1 to tableArray.length - 1) {
          val tableName = tableArray(i)
          val sql = s"select * from $tableName where flt_dt in ($multiFltDt) and tail_num in ($multiTailNum) and file_no in ($multiFileNo)"
          println(sql)
          val data = spark.sql(sql).toDF()
          sinkTable.upsert(data)
        }
        import spark.implicits._
        val sqlInsert = seqList.toDF("model_type", "flt_dt", "tail_num", "file_no")
        sqlInsert.write.mode("append").jdbc(mysqlUrl, tableName, mysqlConnectionProperties)
        size = 0
        multiFltDt = ""
        multiTailNum = ""
        multiFileNo = ""
        seqList = Seq()
      }
    })

    if (size > 0) {
      val tableArray = modelToTables(modelType)
      val tablePath = tableArray(0)
      val sinkTable = LakeSoulTable.forPath(tablePath)
      for (i <- 1 to tableArray.length - 1) {
        val tableName = tableArray(i)
        val sql = s"select * from $tableName where flt_dt in ($multiFltDt) and tail_num in ($multiTailNum) and file_no in ($multiFileNo)"
        println(sql)
        val data = spark.sql(sql).toDF()
        sinkTable.upsert(data)
      }
      import spark.implicits._
      val sqlInsert = seqList.toDF("model_type", "flt_dt", "tail_num", "file_no")
      sqlInsert.write.mode("append").jdbc(mysqlUrl, tableName, mysqlConnectionProperties)
      size = 0
      multiFltDt = ""
      multiTailNum = ""
      multiFileNo = ""
      seqList = Seq()
    }

  }

  private def getPartitions(inputTableName: String, dayList: util.List[String]): util.ArrayList[Row] = {
    val partitionList = new util.ArrayList[Row]
    val dbManager = new DBManager
    val inputTableIdent = inputTableName.split("\\.")
    val inputTableInfo = dbManager.getTableInfoByNameAndNamespace(inputTableIdent(1), inputTableIdent(0))
    val inputTablePartitionInfo = dbManager.getAllPartitionInfo(inputTableInfo.getTableId)
    println("============= PartitionInfo size: " + inputTablePartitionInfo.size + " =============")
    val inputTablePartitionStrList = new util.ArrayList[String]
    println("============= day list is:" + dayList.toString + " ============= ")
    dayList.stream.forEach((day: String) => {
      inputTablePartitionInfo.stream
        .filter((p: PartitionInfo) => p.getPartitionDesc.contains(day))
        .forEach((partitionInfo: PartitionInfo) => inputTablePartitionStrList.add(partitionInfo.getPartitionDesc))
    })
    println("============= inputTablePartition list size filter days: " + inputTablePartitionStrList.size + " =============")
    inputTablePartitionStrList.stream.forEach(
      (info: String) => {
        val partitionKV = DBUtil.parsePartitionDesc(info)
        partitionList.add(Row(partitionKV.get("flt_dt"), partitionKV.get("tail_num"), partitionKV.get("file_no")))
      }
    )
    partitionList
  }

  private def getListDay(beginDayStr: String, endDayStr: String): util.ArrayList[String] = {
    val list = new util.ArrayList[String]
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val beginDay = LocalDate.parse(beginDayStr, formatter)
    val endDay = LocalDate.parse(endDayStr, formatter)
    var date = beginDay
    while (!date.isAfter(endDay)) {
      list.add(date.format(formatter))
      date = date.plusDays(1)
    }
    list
  }
}
