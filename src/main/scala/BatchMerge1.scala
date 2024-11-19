import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object BatchMerge1 {

  def main(args: Array[String]): Unit = {

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

    val df = spark.sql("select * from czods.s_qara_10411_16hz where flt_dt= '20241112' and tail_num = 'B-30A8' and file_no = '3581554'")
    val table = LakeSoulTable.forPath("hdfs://10.64.219.26:9000/tmp/ddf/dws_qara_10411")
    table.upsert(df.toDF)

  }
}




