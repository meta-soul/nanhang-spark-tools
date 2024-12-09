import com.dmetasoul.lakesoul.meta.{DBManager, DBUtil}
import com.dmetasoul.lakesoul.meta.entity.{PartitionInfo, TableInfo}
import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.concurrent.Executors
import java.text.SimpleDateFormat
import java.util.{Date, Properties}


object QarNullSupplementDemo {

  private val BEGIN_TIME: String = "begin_day"
  private val END_DAY: String = "end_day"
  private val BATCH_SIZE: String = "batch_size"

  private val dataFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private val modelToTables: Map[String, Array[String]] = Map(
    "320020" -> Array("hdfs://10.64.219.26:9000/flink/warehouse/czcdm/dws_qara_320020_fillup", "qara_mid.mid_qara_320020_p5hz_fillup", "qara_mid.mid_qara_320020_p25hz_fillup", "qara_mid.mid_qara_320020_1hz_fillup", "qara_mid.mid_qara_320020_2hz_fillup", "qara_mid.mid_qara_320020_4hz_fillupz", "qara_mid.mid_qara_320020_8hz_fillup"),
  )

  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    val beginDay = parameter.get(BEGIN_TIME)
    val endDay = parameter.get(END_DAY)
    val batchSize = parameter.getInt(BATCH_SIZE, 50)
    val modelType = "320020"

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
    val needMergePartition = getPartitions("czods.s_qara_320020_1hz", "czcdm.dws_qara_320020_fillup", selectTimeRange)
    import scala.collection.JavaConverters._
    val needMergePartitionAsScala = needMergePartition.asScala.toList
    spark.sql("use qara_mid")


    val pool = Executors.newFixedThreadPool(12)
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(pool)

    // 提交任务到线程池
    val future1: Future[Unit] = Future(dealP25hzTableData(spark, needMergePartition))
    val future2: Future[Unit] = Future(dealPH5HZ(spark, needMergePartition))
    val future3: Future[Unit] = Future(deal1hzTableData(spark, needMergePartition))
    val future4: Future[Unit] = Future(deal2hzTableData(spark, needMergePartition))
    val future5: Future[Unit] = Future(deal4hzTableData(spark, needMergePartition))
    val future6: Future[Unit] = Future(deal8hzTableData(spark, needMergePartition))

    // 等待所有任务完成
    val result = Await.ready(Future.sequence(Seq(future1, future2, future3, future4, future5, future6)), 36000.seconds)
    result.onComplete {
      case Success(_) => {
        println("All futures completed successfully")
        pool.shutdown()
        println("pool shutdown")
        println("start merge")

        println("*" * 15 + " need merge partition size: " + needMergePartitionAsScala.length)
        var size = 0
        var multiFltDt = ""
        var multiTailNum = ""
        var multiFileNo = ""
        needMergePartitionAsScala.foreach(row => {
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
          if (size == batchSize) {
            println(dataFormat.format(new Date) + " ---------- start new partition, flt_dt: " + fltDt + ", tail_num: " + tailNum + ", file_no: " + fileNo)
            for (i <- 1 to tableArray.length - 1) {
              val tableName = tableArray(i)
              val sql = s"select * from $tableName where flt_dt in ($multiFltDt) and tail_num in ($multiTailNum) and file_no in ($multiFileNo)"
              println(sql)
              val data = spark.sql(sql).toDF()
              sinkTable.upsert(data)
            }
            size = 0
            multiFltDt = ""
            multiTailNum = ""
            multiFileNo = ""
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
          size = 0
          multiFltDt = ""
          multiTailNum = ""
          multiFileNo = ""
        }
      }
      case Failure(ex) => println(s"Waiting for futures failed: ${ex.getMessage}")
    }

  }


  private def dealPH5HZ(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_p5hz_fillup;")
    spark.sql(
      """create table qara_mid.mid_qara_320020_p5hz_fillup (
        |`time` INT, `time_series` INT,
        |`aie1_rw` INT, `aie2_rw` INT, `aiwl` INT, `aiwr` INT, `ai_faul1` INT, `ai_faul2` INT, `flaprw` FLOAT,
        |`glide_dev1` FLOAT, `glide_dev2` FLOAT, `hyd_l_pres_b` INT, `hyd_l_pres_g` INT, `hyd_l_pres_y` INT,
        |`loc_dev1` FLOAT, `loc_dev2` FLOAT, `n1_epr_cmd1` INT, `n1_epr_cmd2` INT, `n1_mod_sel1` INT,
        |`n1_mod_sel2` INT, `pck_1_fl_ctl` INT, `pck_2_fl_ctl` INT, `ralt1` FLOAT, `ralt2` FLOAT, `tat` FLOAT,
        |`flt_dt` STRING, `tail_num` STRING,`file_no` STRING
        |) USING lakesoul PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)
        |LOCATION 'hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_p5hz_fillup'
        |""".stripMargin)
    spark.sql("alter table qara_mid.mid_qara_320020_p5hz_fillup alter column flt_dt drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_p5hz_fillup alter column tail_num drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_p5hz_fillup alter column file_no drop not null;")
    partitionList.forEach(partition => {
      println("dealP5hzTableData: begin deal with tuple: " + partition.toString)
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sql = s"""
                   |insert into  qara_mid.mid_qara_320020_p5hz_fillup
                   |select t0.`time`,t0.time_series
                   |,COALESCE(t0.aie1_rw,t1.aie1_rw) as aie1_rw
                   |,COALESCE(t0.aie2_rw,t1.aie2_rw) as aie2_rw
                   |,COALESCE(t0.aiwl,t1.aiwl) as aiwl
                   |,COALESCE(t0.aiwr,t1.aiwr) as aiwr
                   |,COALESCE(t0.ai_faul1,t1.ai_faul1) as ai_faul1
                   |,COALESCE(t0.ai_faul2,t1.ai_faul2) as ai_faul2
                   |,COALESCE(t0.flaprw,t1.flaprw) as flaprw
                   |,COALESCE(t0.glide_dev1,t1.glide_dev1) as glide_dev1
                   |,COALESCE(t0.glide_dev2,t1.glide_dev2) as glide_dev2
                   |,COALESCE(t0.hyd_l_pres_b,t1.hyd_l_pres_b) as hyd_l_pres_b
                   |,COALESCE(t0.hyd_l_pres_g,t1.hyd_l_pres_g) as hyd_l_pres_g
                   |,COALESCE(t0.hyd_l_pres_y,t1.hyd_l_pres_y) as hyd_l_pres_y
                   |,COALESCE(t0.loc_dev1,t1.loc_dev1) as loc_dev1
                   |,COALESCE(t0.loc_dev2,t1.loc_dev2) as loc_dev2
                   |,COALESCE(t0.n1_epr_cmd1,t1.n1_epr_cmd1) as n1_epr_cmd1
                   |,COALESCE(t0.n1_epr_cmd2,t1.n1_epr_cmd2) as n1_epr_cmd2
                   |,COALESCE(t0.n1_mod_sel1,t1.n1_mod_sel1) as n1_mod_sel1
                   |,COALESCE(t0.n1_mod_sel2,t1.n1_mod_sel2) as n1_mod_sel2
                   |,COALESCE(t0.pck_1_fl_ctl,t1.pck_1_fl_ctl) as pck_1_fl_ctl
                   |,COALESCE(t0.pck_2_fl_ctl,t1.pck_2_fl_ctl) as pck_2_fl_ctl
                   |,COALESCE(t0.ralt1,t1.ralt1) as ralt1
                   |,COALESCE(t0.ralt2,t1.ralt2) as ralt2
                   |,COALESCE(t0.tat,t1.tat) as tat
                   |,t0.flt_dt,t0.tail_num,t0.file_no
                   |from
                   |(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*
                   |    from  czods.s_qara_320020_p5hz
                   |    where  flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'      ) t0
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*
                   |    from  czods.s_qara_320020_p5hz
                   |    where  flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'     ) t1
                   |on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add
                   |""".stripMargin
      spark.sql(sql)

      for (i <- 1 to 31) {
        val sql1 =
          s"""insert into qara_mid.mid_qara_320020_p5hz_fillup
             |select
             |`time`
             |,$i
             |,`aie1_rw`
             |,`aie2_rw`
             |,`aiwl`
             |,`aiwr`
             |,`ai_faul1`
             |,`ai_faul2`
             |,`flaprw`
             |,`glide_dev1`
             |,`glide_dev2`
             |,`hyd_l_pres_b`
             |,`hyd_l_pres_g`
             |,`hyd_l_pres_y`
             |,`loc_dev1`
             |,`loc_dev2`
             |,`n1_epr_cmd1`
             |,`n1_epr_cmd2`
             |,`n1_mod_sel1`
             |,`n1_mod_sel2`
             |,`pck_1_fl_ctl`
             |,`pck_2_fl_ctl`
             |,`ralt1`
             |,`ralt2`
             |,`tat`
             |,`flt_dt`
             |,`tail_num`
             |,`file_no`
             |from  qara_mid.mid_qara_320020_p5hz_fillup
             |where  flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo' and time_series=0 """.stripMargin
        spark.sql(sql1)
      }

    })
  }

  private def dealP25hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_p25hz_fillup;")
    spark.sql(
      """create table if not exists qara_mid.mid_qara_320020_p25hz_fillup (
        |`time` INT, `time_series` INT, `acms_fp` INT,
        |`acv_b_on2` INT, `ac_iden` INT,
        |`ac_tail123` STRING,
        |`ac_tail4` STRING,
        |`ac_tail456` STRING,
        | `ac_tail7` STRING, `ac_typ` INT, `ail_lh_avl_b` INT, `ail_lh_avl_g` INT,
        | `ail_rh_avl_b` INT, `ail_rh_avl_g` INT, `airline_id` STRING,
        | `airl_idt` STRING, `air_ground` FLOAT, `aiw_rw` INT, `alt_cpt` FLOAT,
        | `alt_fo` FLOAT, `alt_swc` INT, `apr_ctl` INT, `apubld_vv_op` INT, `aspd_ctl` INT, `aud_tr_fail` INT,
        | `bld_vlv1` INT, `bld_vlv2` INT, `bp1` FLOAT, `bp2` FLOAT, `cent_curr` INT, `city_from` STRING,
        | `city_from_r` STRING, `city_to_r` STRING, `ck_conf_scl_mx` FLOAT, `ck_egt_scl_max` FLOAT,
        | `ck_epr_scl_max` FLOAT, `ck_ins_eng_mod` INT, `ck_itt_scl_max` FLOAT, `ck_ldg_nr` INT, `ck_n1_scl_max` FLOAT,
        | `ck_tla_scl_max` FLOAT, `ck_tla_scl_min` FLOAT, `ck_torq_scl_mx` FLOAT, `cmc_tr_fail` INT, `dat_day` FLOAT,
        | `dat_month` FLOAT, `day_curr` INT, `db_upd_cyc` INT, `db_upd_date` INT, `dc1_bus_on` INT, `dc2_bus_on` INT,
        | `dec_height` FLOAT, `dfc` INT, `dits_fail` INT, `dmc_1_invald` INT, `dmc_2_invald` INT, `dmc_3_invald` INT,
        | `dmc_3_xfr_ca` INT, `dmc_3_xfr_fo` INT, `dmc_mark_id` INT, `dme_dis1` FLOAT, `dme_dis2` FLOAT, `dme_frq1` FLOAT,
        | `dme_frq2` FLOAT, `drift` FLOAT, `ecamdu_1_off` INT, `ecamdu_2_off` INT, `ecamnd_xfrca` INT, `ecamnd_xfrfo` INT,
        | `ecam_sel_mat` INT, `ecu_eec_1_cb` INT, `ecu_eec_2_cb` INT, `egp1` FLOAT, `egp2` FLOAT, `elac1_pt_flt` INT,
        | `elac1_ro_flt` INT, `elac2_pt_flt` INT, `elac2_rl_flt` INT, `elac_1_fault` INT, `elac_2_fault` INT, `eng1_pt25` FLOAT,
        | `eng2_pt25` FLOAT, `epr_max` FLOAT, `ext_tmp_eng1` FLOAT, `ext_tmp_eng2` FLOAT, `fdiu_fail` INT, `fdiu_prg_id` INT,
        | `fdr_fail` INT, `fdr_pb_fail` INT, `flap_faul` INT, `flap_lever_c` FLOAT, `flare` FLOAT, `fleet_idt` INT,
        | `flight_phas0` INT, `fltnum` FLOAT, `fwc_valid` INT, `gw` FLOAT, `head_selon` INT, `hpv_nfc_1` INT,
        | `hpv_nfc_2` INT, `ident_eng1` FLOAT, `ident_eng2` FLOAT, `ils_frq1` FLOAT, `ils_frq2` FLOAT, `isov1` INT,
        | `isov2` INT, `ivv_c` FLOAT, `landing_roll` FLOAT, `latp` FLOAT, `lat_ck_fail` INT, `lat_ck_n_r` INT,
        | `ldg_seldw` INT, `ldg_selup` INT, `ldg_sel_up` FLOAT, `long_ck_fail` INT, `long_ck_n_r` INT, `lonp` FLOAT,
        | `mach_selon` INT, `mls_select` INT, `mode_mmr1` INT, `mode_mmr2` INT, `mode_nd_capt` INT, `mode_nd_fo` INT,
        | `month_curr` INT, `n1_epr_tgt1` INT, `nd_ca_an_off` INT, `nd_fo_an_off` INT, `nm_range_ca` INT, `nm_range_fo` INT,
        | `norm_ck_fail` INT, `norm_ck_n_r` INT, `no_data_eec1` INT, `no_data_eec2` INT, `oil_prs1` FLOAT, `oil_prs2` FLOAT,
        | `oil_tmp1` FLOAT, `oil_tmp2` FLOAT, `oiq1` FLOAT, `oiq2` FLOAT, `opv_nfo_1` INT, `opv_nfo_2` INT, `pfdnd_xfr_ca` INT,
        | `pfdnd_xfr_fo` INT, `pfd_ca_anoff` INT, `pfd_fo_anoff` INT, `pitch_altr1` INT, `pitch_altr2` INT, `pitch_dir_lw` INT,
        | `pitch_nor` INT, `playbk_fail` INT, `playbk_inact` INT, `prv_nfc_1` INT, `prv_nfc_2` INT, `qar_fail` INT, `qar_tapelow` INT,
        | `sdac1_valid` INT, `sdac2_valid` INT, `sec_1_fault` INT, `sec_2_fault` INT, `sec_3_fault` INT, `slat_faul` INT, `spd_brk` INT,
        | `stab_bld_pos1` FLOAT, `stab_bld_pos2` FLOAT, `start_vlv1` INT, `start_vlv2` INT, `sta_van_eng1` FLOAT, `sta_van_eng2` FLOAT,
        | `tcas_sens_1` INT, `tcas_sens_2` INT, `tcas_sens_3` INT, `tla1_c` FLOAT, `tla2_c` FLOAT, `touch_and_go` FLOAT,
        | `trb1_cool` FLOAT, `trb2_cool` FLOAT, `tt25_l` FLOAT, `tt25_r` FLOAT, `t_inl_prs1` FLOAT, `t_inl_prs2` FLOAT,
        | `t_inl_tmp1` FLOAT, `t_inl_tmp2` FLOAT, `utc_hour` FLOAT, `utc_min_sec` INT, `vor_frq1` FLOAT, `vor_frq2` FLOAT,
        | `win_spdr` FLOAT, `year_curr` INT, `year_med` FLOAT,
        | `flt_dt` STRING, `tail_num` STRING,`file_no` STRING
        | ) USING lakesoul PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)
        | LOCATION 'hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_p25hz_fillup'
        | """.stripMargin)
    spark.sql("alter table qara_mid.mid_qara_320020_p25hz_fillup alter column flt_dt drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_p25hz_fillup alter column tail_num drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_p25hz_fillup alter column file_no drop not null;")
    partitionList.forEach(partition => {
      println("dealP25hzTableData: begin deal with tuple: " + partition.toString)
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sql = s"""
                   |insert into  qara_mid.mid_qara_320020_p25hz_fillup
                   |select t0.`time`,t0.time_series,
                   |COALESCE(t0.acms_fp,t1.acms_fp,t2.acms_fp,t3.acms_fp) as acms_fp,
                   |COALESCE(t0.acv_b_on2,t1.acv_b_on2,t2.acv_b_on2,t3.acv_b_on2) as acv_b_on2,
                   |COALESCE(t0.ac_iden,t1.ac_iden,t2.ac_iden,t3.ac_iden) as ac_iden,
                   |COALESCE(t0.ac_tail123,t1.ac_tail123,t2.ac_tail123,t3.ac_tail123) as ac_tail123,
                   |COALESCE(t0.ac_tail4,t1.ac_tail4,t2.ac_tail4,t3.ac_tail4) as ac_tail4,
                   |COALESCE(t0.ac_tail456,t1.ac_tail456,t2.ac_tail456,t3.ac_tail456) as ac_tail456,
                   |COALESCE(t0.ac_tail7,t1.ac_tail7,t2.ac_tail7,t3.ac_tail7) as ac_tail7,
                   |COALESCE(t0.ac_typ,t1.ac_typ,t2.ac_typ,t3.ac_typ) as ac_typ,
                   |COALESCE(t0.ail_lh_avl_b,t1.ail_lh_avl_b,t2.ail_lh_avl_b,t3.ail_lh_avl_b) as ail_lh_avl_b,
                   |COALESCE(t0.ail_lh_avl_g,t1.ail_lh_avl_g,t2.ail_lh_avl_g,t3.ail_lh_avl_g) as ail_lh_avl_g,
                   |COALESCE(t0.ail_rh_avl_b,t1.ail_rh_avl_b,t2.ail_rh_avl_b,t3.ail_rh_avl_b) as ail_rh_avl_b,
                   |COALESCE(t0.ail_rh_avl_g,t1.ail_rh_avl_g,t2.ail_rh_avl_g,t3.ail_rh_avl_g) as ail_rh_avl_g,
                   |COALESCE(t0.airline_id,t1.airline_id,t2.airline_id,t3.airline_id) as airline_id,
                   |COALESCE(t0.airl_idt,t1.airl_idt,t2.airl_idt,t3.airl_idt) as airl_idt,
                   |COALESCE(t0.air_ground,t1.air_ground,t2.air_ground,t3.air_ground) as air_ground,
                   |COALESCE(t0.aiw_rw,t1.aiw_rw,t2.aiw_rw,t3.aiw_rw) as aiw_rw,
                   |COALESCE(t0.alt_cpt,t1.alt_cpt,t2.alt_cpt,t3.alt_cpt) as alt_cpt,
                   |COALESCE(t0.alt_fo,t1.alt_fo,t2.alt_fo,t3.alt_fo) as alt_fo,
                   |COALESCE(t0.alt_swc,t1.alt_swc,t2.alt_swc,t3.alt_swc) as alt_swc,
                   |COALESCE(t0.apr_ctl,t1.apr_ctl,t2.apr_ctl,t3.apr_ctl) as apr_ctl,
                   |COALESCE(t0.apubld_vv_op,t1.apubld_vv_op,t2.apubld_vv_op,t3.apubld_vv_op) as apubld_vv_op,
                   |COALESCE(t0.aspd_ctl,t1.aspd_ctl,t2.aspd_ctl,t3.aspd_ctl) as aspd_ctl,
                   |COALESCE(t0.aud_tr_fail,t1.aud_tr_fail,t2.aud_tr_fail,t3.aud_tr_fail) as aud_tr_fail,
                   |COALESCE(t0.bld_vlv1,t1.bld_vlv1,t2.bld_vlv1,t3.bld_vlv1) as bld_vlv1,
                   |COALESCE(t0.bld_vlv2,t1.bld_vlv2,t2.bld_vlv2,t3.bld_vlv2) as bld_vlv2,
                   |COALESCE(t0.bp1,t1.bp1,t2.bp1,t3.bp1) as bp1,
                   |COALESCE(t0.bp2,t1.bp2,t2.bp2,t3.bp2) as bp2,
                   |COALESCE(t0.cent_curr,t1.cent_curr,t2.cent_curr,t3.cent_curr) as cent_curr,
                   |COALESCE(t0.city_from,t1.city_from,t2.city_from,t3.city_from) as city_from,
                   |COALESCE(t0.city_from_r,t1.city_from_r,t2.city_from_r,t3.city_from_r) as city_from_r,
                   |COALESCE(t0.city_to_r,t1.city_to_r,t2.city_to_r,t3.city_to_r) as city_to_r,
                   |COALESCE(t0.ck_conf_scl_mx,t1.ck_conf_scl_mx,t2.ck_conf_scl_mx,t3.ck_conf_scl_mx) as ck_conf_scl_mx,
                   |COALESCE(t0.ck_egt_scl_max,t1.ck_egt_scl_max,t2.ck_egt_scl_max,t3.ck_egt_scl_max) as ck_egt_scl_max,
                   |COALESCE(t0.ck_epr_scl_max,t1.ck_epr_scl_max,t2.ck_epr_scl_max,t3.ck_epr_scl_max) as ck_epr_scl_max,
                   |COALESCE(t0.ck_ins_eng_mod,t1.ck_ins_eng_mod,t2.ck_ins_eng_mod,t3.ck_ins_eng_mod) as ck_ins_eng_mod,
                   |COALESCE(t0.ck_itt_scl_max,t1.ck_itt_scl_max,t2.ck_itt_scl_max,t3.ck_itt_scl_max) as ck_itt_scl_max,
                   |COALESCE(t0.ck_ldg_nr,t1.ck_ldg_nr,t2.ck_ldg_nr,t3.ck_ldg_nr) as ck_ldg_nr,
                   |COALESCE(t0.ck_n1_scl_max,t1.ck_n1_scl_max,t2.ck_n1_scl_max,t3.ck_n1_scl_max) as ck_n1_scl_max,
                   |COALESCE(t0.ck_tla_scl_max,t1.ck_tla_scl_max,t2.ck_tla_scl_max,t3.ck_tla_scl_max) as ck_tla_scl_max,
                   |COALESCE(t0.ck_tla_scl_min,t1.ck_tla_scl_min,t2.ck_tla_scl_min,t3.ck_tla_scl_min) as ck_tla_scl_min,
                   |COALESCE(t0.ck_torq_scl_mx,t1.ck_torq_scl_mx,t2.ck_torq_scl_mx,t3.ck_torq_scl_mx) as ck_torq_scl_mx,
                   |COALESCE(t0.cmc_tr_fail,t1.cmc_tr_fail,t2.cmc_tr_fail,t3.cmc_tr_fail) as cmc_tr_fail,
                   |COALESCE(t0.dat_day,t1.dat_day,t2.dat_day,t3.dat_day) as dat_day,
                   |COALESCE(t0.dat_month,t1.dat_month,t2.dat_month,t3.dat_month) as dat_month,
                   |COALESCE(t0.day_curr,t1.day_curr,t2.day_curr,t3.day_curr) as day_curr,
                   |COALESCE(t0.db_upd_cyc,t1.db_upd_cyc,t2.db_upd_cyc,t3.db_upd_cyc) as db_upd_cyc,
                   |COALESCE(t0.db_upd_date,t1.db_upd_date,t2.db_upd_date,t3.db_upd_date) as db_upd_date,
                   |COALESCE(t0.dc1_bus_on,t1.dc1_bus_on,t2.dc1_bus_on,t3.dc1_bus_on) as dc1_bus_on,
                   |COALESCE(t0.dc2_bus_on,t1.dc2_bus_on,t2.dc2_bus_on,t3.dc2_bus_on) as dc2_bus_on,
                   |COALESCE(t0.dec_height,t1.dec_height,t2.dec_height,t3.dec_height) as dec_height,
                   |COALESCE(t0.dfc,t1.dfc,t2.dfc,t3.dfc) as dfc,
                   |COALESCE(t0.dits_fail,t1.dits_fail,t2.dits_fail,t3.dits_fail) as dits_fail,
                   |COALESCE(t0.dmc_1_invald,t1.dmc_1_invald,t2.dmc_1_invald,t3.dmc_1_invald) as dmc_1_invald,
                   |COALESCE(t0.dmc_2_invald,t1.dmc_2_invald,t2.dmc_2_invald,t3.dmc_2_invald) as dmc_2_invald,
                   |COALESCE(t0.dmc_3_invald,t1.dmc_3_invald,t2.dmc_3_invald,t3.dmc_3_invald) as dmc_3_invald,
                   |COALESCE(t0.dmc_3_xfr_ca,t1.dmc_3_xfr_ca,t2.dmc_3_xfr_ca,t3.dmc_3_xfr_ca) as dmc_3_xfr_ca,
                   |COALESCE(t0.dmc_3_xfr_fo,t1.dmc_3_xfr_fo,t2.dmc_3_xfr_fo,t3.dmc_3_xfr_fo) as dmc_3_xfr_fo,
                   |COALESCE(t0.dmc_mark_id,t1.dmc_mark_id,t2.dmc_mark_id,t3.dmc_mark_id) as dmc_mark_id,
                   |COALESCE(t0.dme_dis1,t1.dme_dis1,t2.dme_dis1,t3.dme_dis1) as dme_dis1,
                   |COALESCE(t0.dme_dis2,t1.dme_dis2,t2.dme_dis2,t3.dme_dis2) as dme_dis2,
                   |COALESCE(t0.dme_frq1,t1.dme_frq1,t2.dme_frq1,t3.dme_frq1) as dme_frq1,
                   |COALESCE(t0.dme_frq2,t1.dme_frq2,t2.dme_frq2,t3.dme_frq2) as dme_frq2,
                   |COALESCE(t0.drift,t1.drift,t2.drift,t3.drift) as drift,
                   |COALESCE(t0.ecamdu_1_off,t1.ecamdu_1_off,t2.ecamdu_1_off,t3.ecamdu_1_off) as ecamdu_1_off,
                   |COALESCE(t0.ecamdu_2_off,t1.ecamdu_2_off,t2.ecamdu_2_off,t3.ecamdu_2_off) as ecamdu_2_off,
                   |COALESCE(t0.ecamnd_xfrca,t1.ecamnd_xfrca,t2.ecamnd_xfrca,t3.ecamnd_xfrca) as ecamnd_xfrca,
                   |COALESCE(t0.ecamnd_xfrfo,t1.ecamnd_xfrfo,t2.ecamnd_xfrfo,t3.ecamnd_xfrfo) as ecamnd_xfrfo,
                   |COALESCE(t0.ecam_sel_mat,t1.ecam_sel_mat,t2.ecam_sel_mat,t3.ecam_sel_mat) as ecam_sel_mat,
                   |COALESCE(t0.ecu_eec_1_cb,t1.ecu_eec_1_cb,t2.ecu_eec_1_cb,t3.ecu_eec_1_cb) as ecu_eec_1_cb,
                   |COALESCE(t0.ecu_eec_2_cb,t1.ecu_eec_2_cb,t2.ecu_eec_2_cb,t3.ecu_eec_2_cb) as ecu_eec_2_cb,
                   |COALESCE(t0.egp1,t1.egp1,t2.egp1,t3.egp1) as egp1,
                   |COALESCE(t0.egp2,t1.egp2,t2.egp2,t3.egp2) as egp2,
                   |COALESCE(t0.elac1_pt_flt,t1.elac1_pt_flt,t2.elac1_pt_flt,t3.elac1_pt_flt) as elac1_pt_flt,
                   |COALESCE(t0.elac1_ro_flt,t1.elac1_ro_flt,t2.elac1_ro_flt,t3.elac1_ro_flt) as elac1_ro_flt,
                   |COALESCE(t0.elac2_pt_flt,t1.elac2_pt_flt,t2.elac2_pt_flt,t3.elac2_pt_flt) as elac2_pt_flt,
                   |COALESCE(t0.elac2_rl_flt,t1.elac2_rl_flt,t2.elac2_rl_flt,t3.elac2_rl_flt) as elac2_rl_flt,
                   |COALESCE(t0.elac_1_fault,t1.elac_1_fault,t2.elac_1_fault,t3.elac_1_fault) as elac_1_fault,
                   |COALESCE(t0.elac_2_fault,t1.elac_2_fault,t2.elac_2_fault,t3.elac_2_fault) as elac_2_fault,
                   |COALESCE(t0.eng1_pt25,t1.eng1_pt25,t2.eng1_pt25,t3.eng1_pt25) as eng1_pt25,
                   |COALESCE(t0.eng2_pt25,t1.eng2_pt25,t2.eng2_pt25,t3.eng2_pt25) as eng2_pt25,
                   |COALESCE(t0.epr_max,t1.epr_max,t2.epr_max,t3.epr_max) as epr_max,
                   |COALESCE(t0.ext_tmp_eng1,t1.ext_tmp_eng1,t2.ext_tmp_eng1,t3.ext_tmp_eng1) as ext_tmp_eng1,
                   |COALESCE(t0.ext_tmp_eng2,t1.ext_tmp_eng2,t2.ext_tmp_eng2,t3.ext_tmp_eng2) as ext_tmp_eng2,
                   |COALESCE(t0.fdiu_fail,t1.fdiu_fail,t2.fdiu_fail,t3.fdiu_fail) as fdiu_fail,
                   |COALESCE(t0.fdiu_prg_id,t1.fdiu_prg_id,t2.fdiu_prg_id,t3.fdiu_prg_id) as fdiu_prg_id,
                   |COALESCE(t0.fdr_fail,t1.fdr_fail,t2.fdr_fail,t3.fdr_fail) as fdr_fail,
                   |COALESCE(t0.fdr_pb_fail,t1.fdr_pb_fail,t2.fdr_pb_fail,t3.fdr_pb_fail) as fdr_pb_fail,
                   |COALESCE(t0.flap_faul,t1.flap_faul,t2.flap_faul,t3.flap_faul) as flap_faul,
                   |COALESCE(t0.flap_lever_c,t1.flap_lever_c,t2.flap_lever_c,t3.flap_lever_c) as flap_lever_c,
                   |COALESCE(t0.flare,t1.flare,t2.flare,t3.flare) as flare,
                   |COALESCE(t0.fleet_idt,t1.fleet_idt,t2.fleet_idt,t3.fleet_idt) as fleet_idt,
                   |COALESCE(t0.flight_phas0,t1.flight_phas0,t2.flight_phas0,t3.flight_phas0) as flight_phas0,
                   |COALESCE(t0.fltnum,t1.fltnum,t2.fltnum,t3.fltnum) as fltnum,
                   |COALESCE(t0.fwc_valid,t1.fwc_valid,t2.fwc_valid,t3.fwc_valid) as fwc_valid,
                   |COALESCE(t0.gw,t1.gw,t2.gw,t3.gw) as gw,
                   |COALESCE(t0.head_selon,t1.head_selon,t2.head_selon,t3.head_selon) as head_selon,
                   |COALESCE(t0.hpv_nfc_1,t1.hpv_nfc_1,t2.hpv_nfc_1,t3.hpv_nfc_1) as hpv_nfc_1,
                   |COALESCE(t0.hpv_nfc_2,t1.hpv_nfc_2,t2.hpv_nfc_2,t3.hpv_nfc_2) as hpv_nfc_2,
                   |COALESCE(t0.ident_eng1,t1.ident_eng1,t2.ident_eng1,t3.ident_eng1) as ident_eng1,
                   |COALESCE(t0.ident_eng2,t1.ident_eng2,t2.ident_eng2,t3.ident_eng2) as ident_eng2,
                   |COALESCE(t0.ils_frq1,t1.ils_frq1,t2.ils_frq1,t3.ils_frq1) as ils_frq1,
                   |COALESCE(t0.ils_frq2,t1.ils_frq2,t2.ils_frq2,t3.ils_frq2) as ils_frq2,
                   |COALESCE(t0.isov1,t1.isov1,t2.isov1,t3.isov1) as isov1,
                   |COALESCE(t0.isov2,t1.isov2,t2.isov2,t3.isov2) as isov2,
                   |COALESCE(t0.ivv_c,t1.ivv_c,t2.ivv_c,t3.ivv_c) as ivv_c,
                   |COALESCE(t0.landing_roll,t1.landing_roll,t2.landing_roll,t3.landing_roll) as landing_roll,
                   |COALESCE(t0.latp,t1.latp,t2.latp,t3.latp) as latp,
                   |COALESCE(t0.lat_ck_fail,t1.lat_ck_fail,t2.lat_ck_fail,t3.lat_ck_fail) as lat_ck_fail,
                   |COALESCE(t0.lat_ck_n_r,t1.lat_ck_n_r,t2.lat_ck_n_r,t3.lat_ck_n_r) as lat_ck_n_r,
                   |COALESCE(t0.ldg_seldw,t1.ldg_seldw,t2.ldg_seldw,t3.ldg_seldw) as ldg_seldw,
                   |COALESCE(t0.ldg_selup,t1.ldg_selup,t2.ldg_selup,t3.ldg_selup) as ldg_selup,
                   |COALESCE(t0.ldg_sel_up,t1.ldg_sel_up,t2.ldg_sel_up,t3.ldg_sel_up) as ldg_sel_up,
                   |COALESCE(t0.long_ck_fail,t1.long_ck_fail,t2.long_ck_fail,t3.long_ck_fail) as long_ck_fail,
                   |COALESCE(t0.long_ck_n_r,t1.long_ck_n_r,t2.long_ck_n_r,t3.long_ck_n_r) as long_ck_n_r,
                   |COALESCE(t0.lonp,t1.lonp,t2.lonp,t3.lonp) as lonp,
                   |COALESCE(t0.mach_selon,t1.mach_selon,t2.mach_selon,t3.mach_selon) as mach_selon,
                   |COALESCE(t0.mls_select,t1.mls_select,t2.mls_select,t3.mls_select) as mls_select,
                   |COALESCE(t0.mode_mmr1,t1.mode_mmr1,t2.mode_mmr1,t3.mode_mmr1) as mode_mmr1,
                   |COALESCE(t0.mode_mmr2,t1.mode_mmr2,t2.mode_mmr2,t3.mode_mmr2) as mode_mmr2,
                   |COALESCE(t0.mode_nd_capt,t1.mode_nd_capt,t2.mode_nd_capt,t3.mode_nd_capt) as mode_nd_capt,
                   |COALESCE(t0.mode_nd_fo,t1.mode_nd_fo,t2.mode_nd_fo,t3.mode_nd_fo) as mode_nd_fo,
                   |COALESCE(t0.month_curr,t1.month_curr,t2.month_curr,t3.month_curr) as month_curr,
                   |COALESCE(t0.n1_epr_tgt1,t1.n1_epr_tgt1,t2.n1_epr_tgt1,t3.n1_epr_tgt1) as n1_epr_tgt1,
                   |COALESCE(t0.nd_ca_an_off,t1.nd_ca_an_off,t2.nd_ca_an_off,t3.nd_ca_an_off) as nd_ca_an_off,
                   |COALESCE(t0.nd_fo_an_off,t1.nd_fo_an_off,t2.nd_fo_an_off,t3.nd_fo_an_off) as nd_fo_an_off,
                   |COALESCE(t0.nm_range_ca,t1.nm_range_ca,t2.nm_range_ca,t3.nm_range_ca) as nm_range_ca,
                   |COALESCE(t0.nm_range_fo,t1.nm_range_fo,t2.nm_range_fo,t3.nm_range_fo) as nm_range_fo,
                   |COALESCE(t0.norm_ck_fail,t1.norm_ck_fail,t2.norm_ck_fail,t3.norm_ck_fail) as norm_ck_fail,
                   |COALESCE(t0.norm_ck_n_r,t1.norm_ck_n_r,t2.norm_ck_n_r,t3.norm_ck_n_r) as norm_ck_n_r,
                   |COALESCE(t0.no_data_eec1,t1.no_data_eec1,t2.no_data_eec1,t3.no_data_eec1) as no_data_eec1,
                   |COALESCE(t0.no_data_eec2,t1.no_data_eec2,t2.no_data_eec2,t3.no_data_eec2) as no_data_eec2,
                   |COALESCE(t0.oil_prs1,t1.oil_prs1,t2.oil_prs1,t3.oil_prs1) as oil_prs1,
                   |COALESCE(t0.oil_prs2,t1.oil_prs2,t2.oil_prs2,t3.oil_prs2) as oil_prs2,
                   |COALESCE(t0.oil_tmp1,t1.oil_tmp1,t2.oil_tmp1,t3.oil_tmp1) as oil_tmp1,
                   |COALESCE(t0.oil_tmp2,t1.oil_tmp2,t2.oil_tmp2,t3.oil_tmp2) as oil_tmp2,
                   |COALESCE(t0.oiq1,t1.oiq1,t2.oiq1,t3.oiq1) as oiq1,
                   |COALESCE(t0.oiq2,t1.oiq2,t2.oiq2,t3.oiq2) as oiq2,
                   |COALESCE(t0.opv_nfo_1,t1.opv_nfo_1,t2.opv_nfo_1,t3.opv_nfo_1) as opv_nfo_1,
                   |COALESCE(t0.opv_nfo_2,t1.opv_nfo_2,t2.opv_nfo_2,t3.opv_nfo_2) as opv_nfo_2,
                   |COALESCE(t0.pfdnd_xfr_ca,t1.pfdnd_xfr_ca,t2.pfdnd_xfr_ca,t3.pfdnd_xfr_ca) as pfdnd_xfr_ca,
                   |COALESCE(t0.pfdnd_xfr_fo,t1.pfdnd_xfr_fo,t2.pfdnd_xfr_fo,t3.pfdnd_xfr_fo) as pfdnd_xfr_fo,
                   |COALESCE(t0.pfd_ca_anoff,t1.pfd_ca_anoff,t2.pfd_ca_anoff,t3.pfd_ca_anoff) as pfd_ca_anoff,
                   |COALESCE(t0.pfd_fo_anoff,t1.pfd_fo_anoff,t2.pfd_fo_anoff,t3.pfd_fo_anoff) as pfd_fo_anoff,
                   |COALESCE(t0.pitch_altr1,t1.pitch_altr1,t2.pitch_altr1,t3.pitch_altr1) as pitch_altr1,
                   |COALESCE(t0.pitch_altr2,t1.pitch_altr2,t2.pitch_altr2,t3.pitch_altr2) as pitch_altr2,
                   |COALESCE(t0.pitch_dir_lw,t1.pitch_dir_lw,t2.pitch_dir_lw,t3.pitch_dir_lw) as pitch_dir_lw,
                   |COALESCE(t0.pitch_nor,t1.pitch_nor,t2.pitch_nor,t3.pitch_nor) as pitch_nor,
                   |COALESCE(t0.playbk_fail,t1.playbk_fail,t2.playbk_fail,t3.playbk_fail) as playbk_fail,
                   |COALESCE(t0.playbk_inact,t1.playbk_inact,t2.playbk_inact,t3.playbk_inact) as playbk_inact,
                   |COALESCE(t0.prv_nfc_1,t1.prv_nfc_1,t2.prv_nfc_1,t3.prv_nfc_1) as prv_nfc_1,
                   |COALESCE(t0.prv_nfc_2,t1.prv_nfc_2,t2.prv_nfc_2,t3.prv_nfc_2) as prv_nfc_2,
                   |COALESCE(t0.qar_fail,t1.qar_fail,t2.qar_fail,t3.qar_fail) as qar_fail,
                   |COALESCE(t0.qar_tapelow,t1.qar_tapelow,t2.qar_tapelow,t3.qar_tapelow) as qar_tapelow,
                   |COALESCE(t0.sdac1_valid,t1.sdac1_valid,t2.sdac1_valid,t3.sdac1_valid) as sdac1_valid,
                   |COALESCE(t0.sdac2_valid,t1.sdac2_valid,t2.sdac2_valid,t3.sdac2_valid) as sdac2_valid,
                   |COALESCE(t0.sec_1_fault,t1.sec_1_fault,t2.sec_1_fault,t3.sec_1_fault) as sec_1_fault,
                   |COALESCE(t0.sec_2_fault,t1.sec_2_fault,t2.sec_2_fault,t3.sec_2_fault) as sec_2_fault,
                   |COALESCE(t0.sec_3_fault,t1.sec_3_fault,t2.sec_3_fault,t3.sec_3_fault) as sec_3_fault,
                   |COALESCE(t0.slat_faul,t1.slat_faul,t2.slat_faul,t3.slat_faul) as slat_faul,
                   |COALESCE(t0.spd_brk,t1.spd_brk,t2.spd_brk,t3.spd_brk) as spd_brk,
                   |COALESCE(t0.stab_bld_pos1,t1.stab_bld_pos1,t2.stab_bld_pos1,t3.stab_bld_pos1) as stab_bld_pos1,
                   |COALESCE(t0.stab_bld_pos2,t1.stab_bld_pos2,t2.stab_bld_pos2,t3.stab_bld_pos2) as stab_bld_pos2,
                   |COALESCE(t0.start_vlv1,t1.start_vlv1,t2.start_vlv1,t3.start_vlv1) as start_vlv1,
                   |COALESCE(t0.start_vlv2,t1.start_vlv2,t2.start_vlv2,t3.start_vlv2) as start_vlv2,
                   |COALESCE(t0.sta_van_eng1,t1.sta_van_eng1,t2.sta_van_eng1,t3.sta_van_eng1) as sta_van_eng1,
                   |COALESCE(t0.sta_van_eng2,t1.sta_van_eng2,t2.sta_van_eng2,t3.sta_van_eng2) as sta_van_eng2,
                   |COALESCE(t0.tcas_sens_1,t1.tcas_sens_1,t2.tcas_sens_1,t3.tcas_sens_1) as tcas_sens_1,
                   |COALESCE(t0.tcas_sens_2,t1.tcas_sens_2,t2.tcas_sens_2,t3.tcas_sens_2) as tcas_sens_2,
                   |COALESCE(t0.tcas_sens_3,t1.tcas_sens_3,t2.tcas_sens_3,t3.tcas_sens_3) as tcas_sens_3,
                   |COALESCE(t0.tla1_c,t1.tla1_c,t2.tla1_c,t3.tla1_c) as tla1_c,
                   |COALESCE(t0.tla2_c,t1.tla2_c,t2.tla2_c,t3.tla2_c) as tla2_c,
                   |COALESCE(t0.touch_and_go,t1.touch_and_go,t2.touch_and_go,t3.touch_and_go) as touch_and_go,
                   |COALESCE(t0.trb1_cool,t1.trb1_cool,t2.trb1_cool,t3.trb1_cool) as trb1_cool,
                   |COALESCE(t0.trb2_cool,t1.trb2_cool,t2.trb2_cool,t3.trb2_cool) as trb2_cool,
                   |COALESCE(t0.tt25_l,t1.tt25_l,t2.tt25_l,t3.tt25_l) as tt25_l,
                   |COALESCE(t0.tt25_r,t1.tt25_r,t2.tt25_r,t3.tt25_r) as tt25_r,
                   |COALESCE(t0.t_inl_prs1,t1.t_inl_prs1,t2.t_inl_prs1,t3.t_inl_prs1) as t_inl_prs1,
                   |COALESCE(t0.t_inl_prs2,t1.t_inl_prs2,t2.t_inl_prs2,t3.t_inl_prs2) as t_inl_prs2,
                   |COALESCE(t0.t_inl_tmp1,t1.t_inl_tmp1,t2.t_inl_tmp1,t3.t_inl_tmp1) as t_inl_tmp1,
                   |COALESCE(t0.t_inl_tmp2,t1.t_inl_tmp2,t2.t_inl_tmp2,t3.t_inl_tmp2) as t_inl_tmp2,
                   |COALESCE(t0.utc_hour,t1.utc_hour,t2.utc_hour,t3.utc_hour) as utc_hour,
                   |COALESCE(t0.utc_min_sec,t1.utc_min_sec,t2.utc_min_sec,t3.utc_min_sec) as utc_min_sec,
                   |COALESCE(t0.vor_frq1,t1.vor_frq1,t2.vor_frq1,t3.vor_frq1) as vor_frq1,
                   |COALESCE(t0.vor_frq2,t1.vor_frq2,t2.vor_frq2,t3.vor_frq2) as vor_frq2,
                   |COALESCE(t0.win_spdr,t1.win_spdr,t2.win_spdr,t3.win_spdr) as win_spdr,
                   |COALESCE(t0.year_curr,t1.year_curr,t2.year_curr,t3.year_curr) as year_curr,
                   |COALESCE(t0.year_med,t1.year_med,t2.year_med,t3.year_med) as year_med,
                   |t0.flt_dt,t0.tail_num,t0.file_no
                   |from
                   |(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*
                   |    from  czods.s_qara_320020_p25hz
                   |    where  flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'   ) t0
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*
                   |    from  czods.s_qara_320020_p25hz
                   |    where  flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'  ) t1
                   |on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*
                   |    from  czods.s_qara_320020_p25hz
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'  ) t2
                   |on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add
                   |    left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,*
                   |    from  czods.s_qara_320020_p25hz
                   |    where  flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'  ) t3
                   |on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add
                   |""".stripMargin
      spark.sql(sql)

      for (i <- 1 to 31) {
        val sql1 =
          s"""insert into  qara_mid.mid_qara_320020_p25hz_fillup
             |select
             |`time`
             |,$i
             |,acms_fp
             |,acv_b_on2
             |,ac_iden
             |,ac_tail123
             |,ac_tail4
             |,ac_tail456
             |,ac_tail7
             |,ac_typ
             |,ail_lh_avl_b
             |,ail_lh_avl_g
             |,ail_rh_avl_b
             |,ail_rh_avl_g
             |,airline_id
             |,airl_idt
             |,air_ground
             |,aiw_rw
             |,alt_cpt
             |,alt_fo
             |,alt_swc
             |,apr_ctl
             |,apubld_vv_op
             |,aspd_ctl
             |,aud_tr_fail
             |,bld_vlv1
             |,bld_vlv2
             |,bp1
             |,bp2
             |,cent_curr
             |,city_from
             |,city_from_r
             |,city_to_r
             |,ck_conf_scl_mx
             |,ck_egt_scl_max
             |,ck_epr_scl_max
             |,ck_ins_eng_mod
             |,ck_itt_scl_max
             |,ck_ldg_nr
             |,ck_n1_scl_max
             |,ck_tla_scl_max
             |,ck_tla_scl_min
             |,ck_torq_scl_mx
             |,cmc_tr_fail
             |,dat_day
             |,dat_month
             |,day_curr
             |,db_upd_cyc
             |,db_upd_date
             |,dc1_bus_on
             |,dc2_bus_on
             |,dec_height
             |,dfc
             |,dits_fail
             |,dmc_1_invald
             |,dmc_2_invald
             |,dmc_3_invald
             |,dmc_3_xfr_ca
             |,dmc_3_xfr_fo
             |,dmc_mark_id
             |,dme_dis1
             |,dme_dis2
             |,dme_frq1
             |,dme_frq2
             |,drift
             |,ecamdu_1_off
             |,ecamdu_2_off
             |,ecamnd_xfrca
             |,ecamnd_xfrfo
             |,ecam_sel_mat
             |,ecu_eec_1_cb
             |,ecu_eec_2_cb
             |,egp1
             |,egp2
             |,elac1_pt_flt
             |,elac1_ro_flt
             |,elac2_pt_flt
             |,elac2_rl_flt
             |,elac_1_fault
             |,elac_2_fault
             |,eng1_pt25
             |,eng2_pt25
             |,epr_max
             |,ext_tmp_eng1
             |,ext_tmp_eng2
             |,fdiu_fail
             |,fdiu_prg_id
             |,fdr_fail
             |,fdr_pb_fail
             |,flap_faul
             |,flap_lever_c
             |,flare
             |,fleet_idt
             |,flight_phas0
             |,fltnum
             |,fwc_valid
             |,gw
             |,head_selon
             |,hpv_nfc_1
             |,hpv_nfc_2
             |,ident_eng1
             |,ident_eng2
             |,ils_frq1
             |,ils_frq2
             |,isov1
             |,isov2
             |,ivv_c
             |,landing_roll
             |,latp
             |,lat_ck_fail
             |,lat_ck_n_r
             |,ldg_seldw
             |,ldg_selup
             |,ldg_sel_up
             |,long_ck_fail
             |,long_ck_n_r
             |,lonp
             |,mach_selon
             |,mls_select
             |,mode_mmr1
             |,mode_mmr2
             |,mode_nd_capt
             |,mode_nd_fo
             |,month_curr
             |,n1_epr_tgt1
             |,nd_ca_an_off
             |,nd_fo_an_off
             |,nm_range_ca
             |,nm_range_fo
             |,norm_ck_fail
             |,norm_ck_n_r
             |,no_data_eec1
             |,no_data_eec2
             |,oil_prs1
             |,oil_prs2
             |,oil_tmp1
             |,oil_tmp2
             |,oiq1
             |,oiq2
             |,opv_nfo_1
             |,opv_nfo_2
             |,pfdnd_xfr_ca
             |,pfdnd_xfr_fo
             |,pfd_ca_anoff
             |,pfd_fo_anoff
             |,pitch_altr1
             |,pitch_altr2
             |,pitch_dir_lw
             |,pitch_nor
             |,playbk_fail
             |,playbk_inact
             |,prv_nfc_1
             |,prv_nfc_2
             |,qar_fail
             |,qar_tapelow
             |,sdac1_valid
             |,sdac2_valid
             |,sec_1_fault
             |,sec_2_fault
             |,sec_3_fault
             |,slat_faul
             |,spd_brk
             |,stab_bld_pos1
             |,stab_bld_pos2
             |,start_vlv1
             |,start_vlv2
             |,sta_van_eng1
             |,sta_van_eng2
             |,tcas_sens_1
             |,tcas_sens_2
             |,tcas_sens_3
             |,tla1_c
             |,tla2_c
             |,touch_and_go
             |,trb1_cool
             |,trb2_cool
             |,tt25_l
             |,tt25_r
             |,t_inl_prs1
             |,t_inl_prs2
             |,t_inl_tmp1
             |,t_inl_tmp2
             |,utc_hour
             |,utc_min_sec
             |,vor_frq1
             |,vor_frq2
             |,win_spdr
             |,year_curr
             |,year_med
             |,flt_dt
             |,tail_num
             |,file_no
             |from  qara_mid.mid_qara_320020_p25hz_fillup
             |where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo' and time_series=0 """.stripMargin
        spark.sql(sql1)
      }

    })
  }

  private def deal1hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_1hz_fillup;")
    spark.sql(
      """create table if not exists qara_mid.mid_qara_320020_1hz_fillup (
        |`time` INT, `time_series` INT,
        |`aapp` INT, `aapp_td` INT, `ac_tail1` STRING, `ac_tail2` STRING, `ac_tail3` STRING,
        |`ac_type` INT, `ac_type_csn` INT, `aie1` INT, `aie2` INT, `aill` FLOAT, `ailr` FLOAT,
        |`air_durn` INT, `aiw` INT, `alpha_floor` INT, `alt_const` FLOAT, `alt_qnh` INT,
        |`alt_qnh_csn` INT, `alt_qnh_ld` FLOAT, `alt_qnh_to` FLOAT, `alt_sel` FLOAT, `alt_sel_csn` FLOAT,
        |`alt_std` FLOAT, `alt_stdc` FLOAT, `alt_stdc_csn` FLOAT, `aoal` FLOAT, `aoar` FLOAT, `apu_on` INT,
        |`apu_on_csn` INT, `ap_egd1` INT, `ap_egd1_csn` INT, `ap_egd2` INT, `ap_egd2_csn` INT, `ap_off` INT,
        |`ap_off_csn` INT, `arp_dist_dest` FLOAT, `arp_dist_dest_csn` FLOAT, `arp_dist_orig` FLOAT,
        |`arp_heigh_dest` FLOAT, `arp_heigh_orig` FLOAT, `ats_active` INT, `ats_egd` INT,
        |`ats_egdoract_csn` INT, `ats_egd_csn` INT, `ats_ret_mod` INT, `ats_spd_mach` INT,
        |`ats_thrustn1` INT, `auto_brk_flt` INT, `auto_brk_off` INT, `auto_land_wn` INT, `auto_v2` INT,
        |`b3d_day` INT, `b3d_hour` INT, `b3d_min` INT, `b3d_month` INT, `badsf_nr` INT, `baromb` FLOAT,
        |`bea_mk` INT, `brk_pdll` FLOAT, `brk_ped_rh` FLOAT, `cab_prs_war` INT, `city_from_r_csn` STRING,
        |`city_to` STRING, `city_to_r_csn` STRING, `ck_alt_sel` FLOAT, `ck_alt_std` FLOAT, `ck_c1_r1_col` INT,
        |`ck_c1_r1_txt` INT, `ck_c1_r3_col` INT, `ck_c1_r3_txt` INT, `ck_c2_r1_col` INT, `ck_c2_r1_txt` INT,
        |`ck_c2_r2_col` INT, `ck_c2_r2_txt` INT, `ck_c2_r3_col` INT, `ck_c2_r3_txt` INT, `ck_c3_r1_col` INT,
        |`ck_c3_r1_txt` INT, `ck_c3_r2_col` INT, `ck_c3_r2_txt` INT, `ck_c4_r1_col` INT, `ck_c4_r1_txt` INT,
        |`ck_c4_r2_col` INT, `ck_c4_r2_txt` INT, `ck_c4_r3_col` INT, `ck_c4_r3_txt` INT, `ck_c5_r1_col` INT,
        |`ck_c5_r1_txt` INT, `ck_c5_r2_col` INT, `ck_c5_r2_txt` INT, `ck_c5_r3_col` INT, `ck_c5_r3_txt` INT,
        |`ck_conf_pos` INT, `ck_conf_pos_pc` FLOAT, `ck_egt1` FLOAT, `ck_egt2` FLOAT, `ck_epr1` FLOAT,
        |`ck_epr2` FLOAT, `ck_flap_spd` FLOAT, `ck_fqty` FLOAT, `ck_gpws_mode` INT, `ck_gw` FLOAT,
        |`ck_head` FLOAT, `ck_head_mag` FLOAT, `ck_head_sel` FLOAT, `ck_head_selon` INT, `ck_ias` FLOAT,
        |`ck_ldg_seldw` INT, `ck_ldg_wow` INT, `ck_mach` FLOAT, `ck_mas_war` INT, `ck_n11` FLOAT,
        |`ck_n12` FLOAT, `ck_n21` FLOAT, `ck_n22` FLOAT, `ck_ralt` FLOAT, `ck_rev_dep1` INT, `ck_rev_dep2` INT,
        |`ck_spd_brk` INT, `ck_tcas_ra` INT, `ck_tcas_ta` INT, `ck_tla_pct1` INT, `ck_tla_pct2` INT,
        |`ck_v1` FLOAT, `ck_v2` FLOAT, `ck_vapp` FLOAT, `ck_vhf1_emit` INT, `ck_vls` FLOAT,
        |`ck_vmax` FLOAT, `ck_vr` FLOAT, `conf` INT, `conf_csn` INT, `conf_ld` INT, `conf_to` INT,
        |`cut` INT, `date` INT, `date_r` INT, `date_r_csn` INT, `date_to` INT, `dat_year` FLOAT,
        |`day_ld` INT, `day_to` INT, `debug` INT, `destination` STRING, `dev_mag` FLOAT, `dfc_csn` INT,
        |`dfc_pos_cfa` INT, `dist` FLOAT, `dist0` FLOAT, `dist_lat_td` FLOAT, `dist_ldg` FLOAT,
        |`dist_ldg_csn` FLOAT, `dist_ldg_from_gs` FLOAT, `dist_pthr` FLOAT, `dist_to` FLOAT, `drift_csn` FLOAT,
        |`dual_input` INT, `durn_200_ld` INT, `durn_txo` FLOAT, `egt1` FLOAT, `egt1c` FLOAT, `egt1c_csn` FLOAT,
        |`egt1_csn` FLOAT, `egt2` FLOAT, `egt2c` FLOAT, `egt2c_csn` FLOAT, `egt2_csn` FLOAT, `egt_max` FLOAT,
        |`elevl` FLOAT, `elevr` FLOAT, `eng1_severit` INT, `eng2_severit` INT, `eng_man` INT, `epr1` FLOAT,
        |`epr1c` FLOAT, `epr1_csn` FLOAT, `epr2` FLOAT, `epr2c` FLOAT, `epr2_csn` FLOAT, `epr_cmd_1` FLOAT,
        |`epr_cmd_2` FLOAT, `epr_tgt1` FLOAT, `evt_mk` INT, `fburn` FLOAT, `fburn1` FLOAT, `fburn1_txo` FLOAT,
        |`fburn2` FLOAT, `fburn2_txo` FLOAT, `fburn3_txo` FLOAT, `fburn4_txo` FLOAT, `fburn_avg` FLOAT,
        |`fburn_ti` FLOAT, `fburn_txo` FLOAT, `fburn_txo_avg` FLOAT, `fd_1` INT, `fd_2` INT, `ff1` FLOAT,
        |`ff1c` FLOAT, `ff1_csn` FLOAT, `ff2` FLOAT, `ff2c` FLOAT, `ff2_csn` FLOAT, `fire1` INT,
        |`fire2` INT, `fire_apu` INT, `flap` FLOAT, `flapc` FLOAT, `flapc_csn` FLOAT, `flap_spd` FLOAT,
        |`flight_chang` INT, `flight_ident` INT, `flight_input` INT, `flight_no1` STRING, `flight_no1_csn` STRING,
        |`flight_no2` STRING, `flight_no2_csn` STRING, `flight_phase` INT, `flight_phase_csn` INT,
        |`flight_recogn` INT, `flight_type` INT, `flt_path_angl` FLOAT, `flt_path_angl_csn` FLOAT,
        |`fma_lat` INT, `fma_long` INT, `fpa_selected` FLOAT, `fqty_ld` FLOAT, `fqty_to` FLOAT, `fr_count` INT,
        |`fuel_fire_1` INT, `fuel_fire_2` INT, `gamax1000` FLOAT, `gamax150` FLOAT, `gamax500` FLOAT, `gamin1000` FLOAT,
        |`gamin150` FLOAT, `gamin500` FLOAT, `glide_devc` FLOAT, `glide_devc_csn` FLOAT, `glide_dev_max` FLOAT,
        |`glide_dev_min` FLOAT, `glide_gap` FLOAT, `glide_ils` FLOAT, `gpws_ctype` INT, `gpws_dont_sink` INT,
        |`gpws_flap_low` INT, `gpws_gear_low` INT, `gpws_glide` INT, `gpws_mode` INT, `gpws_mode_csn` INT,
        |`gpws_pull_up` INT, `gpws_sink_rate` INT, `gpws_tr_low` INT, `gpws_tr_up` INT, `gpws_war` INT,
        |`gpws_wsh_war` INT, `gs` FLOAT, `gsc` FLOAT, `gsc1p` FLOAT, `gsc_csn` FLOAT, `gs_csn` FLOAT,
        |`gwc` FLOAT, `gw_csn` FLOAT, `gw_landing_lim` FLOAT, `gw_ld` FLOAT, `gw_takeoff_lim` FLOAT,
        |`gw_taxi_lim` FLOAT, `gw_to` FLOAT, `head` FLOAT, `head_csn` FLOAT, `head_lin` FLOAT, `head_lin_csn` FLOAT,
        |`head_mag` FLOAT, `head_sel` FLOAT, `head_true` FLOAT, `head_true_csn` FLOAT, `height` FLOAT,
        |`height_csn` FLOAT, `heig_1conf_chg` FLOAT, `heig_dev_1000` INT, `heig_dev_500` INT, `heig_gear_dw` FLOAT,
        |`heig_lconf_chg` FLOAT, `hf` INT, `hf_cfa` INT, `hp_fuel_vv_1` INT, `hp_fuel_vv_2` INT, `ias` FLOAT,
        |`iasc` FLOAT, `iasc_csn` FLOAT, `ias_app_1000` FLOAT, `ias_app_50` FLOAT, `ias_app_500` FLOAT,
        |`ias_bef_ld` FLOAT, `ias_cl_1000` FLOAT, `ias_cl_500` FLOAT, `ias_csn` FLOAT, `ias_ext_conf1` FLOAT,
        |`ias_ext_conf2` FLOAT, `ias_ext_conf3` FLOAT, `ias_ext_conf4` FLOAT, `ias_ext_conf5` FLOAT, `ias_ld` FLOAT,
        |`ias_max` FLOAT, `ias_maxcnf1ld` FLOAT, `ias_maxcnf2ld` FLOAT, `ias_maxcnf3ld` FLOAT, `ias_maxcnf4ld` FLOAT,
        |`ias_maxcnf5ld` FLOAT, `ias_maxcnf6ld` FLOAT, `ias_maxcnf7ld` FLOAT, `ias_maxcnf8ld` FLOAT,
        |`ias_maxconf1to` FLOAT, `ias_maxconf2to` FLOAT, `ias_maxconf3to` FLOAT, `ias_maxconf4to` FLOAT,
        |`ias_maxconf5to` FLOAT, `ias_max_geardw` FLOAT, `ias_min_finapp` FLOAT, `ias_to` FLOAT, `ias_to_50` FLOAT,
        |`ils_lim` INT, `ils_val` INT, `in_flight` INT, `ivv` FLOAT, `ivv_csn` FLOAT, `ivv_max_bl2000` FLOAT,
        |`ivv_min_cl35` FLOAT, `ivv_min_cl400` FLOAT, `ivv_sel` FLOAT, `latpc` FLOAT, `latpc_csn` FLOAT,
        |`lat_air` FLOAT, `ldgl` INT, `ldgl_csn` INT, `ldgnos` INT, `ldgnos_csn` INT, `ldgr` INT, `ldgr_csn` INT,
        |`ldg_seldw_csn` INT, `ldg_unlokdw` INT, `lift_gnd` INT, `loc_devc` FLOAT, `loc_devc_csn` FLOAT,
        |`loc_gap` FLOAT, `long_air` FLOAT, `lonpc` FLOAT, `lonpc_csn` FLOAT, `low_press_1` INT, `low_press_2` INT,
        |`lo_armed` INT, `mach` FLOAT, `mach_buff` FLOAT, `mach_buff_csn` FLOAT, `mach_csn` FLOAT, `mach_max` FLOAT,
        |`mas_cau` INT, `mas_cau_csn` INT, `mas_war` INT, `max_armed` INT, `med_armed` INT, `min_alt_n1_40` FLOAT,
        |`min_n1_40` FLOAT, `mmo` FLOAT, `n11c` FLOAT, `n11_csn` FLOAT, `n12c` FLOAT, `n12_csn` FLOAT, `n1_cmd1` FLOAT,
        |`n1_cmd2` FLOAT, `n1_epr1` INT, `n1_epr2` INT, `n1_max_txo3min` FLOAT, `n1_min_bl500` FLOAT,
        |`n1_ths_tgt` FLOAT, `n21` FLOAT, `n21c` FLOAT, `n21_csn` FLOAT, `n22` FLOAT, `n22c` FLOAT, `n22_csn` FLOAT,
        |`oil_qty1_csn` FLOAT, `oil_qty2_csn` FLOAT, `one_eng_app` INT, `origin` STRING, `par_qual` FLOAT,
        |`pass_no` INT, `pf` INT, `pilot_ld` INT, `pilot_to` INT, `pitch_altr` INT, `ralt1c` FLOAT, `ralt2c` FLOAT,
        |`raltc` FLOAT, `raltc_csn` FLOAT, `rate` INT, `refused_trans` INT, `relief` FLOAT, `rev_deployd1` INT,
        |`rev_deployd2` INT, `rev_unlock_1` INT, `rev_unlock_2` INT, `roll_cpt` FLOAT, `roll_fo` FLOAT,
        |`runway_ld` STRING, `runway_ld_csn` STRING, `runway_to` STRING,
        |`runway_to_csn` STRING, `sat` FLOAT, `satr_csn` FLOAT, `sf_count` INT, `single_txo` INT,
        |`slat` FLOAT, `slatc` FLOAT, `slatc_csn` FLOAT, `slatrw` FLOAT, `slat_spd` FLOAT, `smk_avi_war` INT,
        |`smk_crg_war` INT, `smk_lvy_war` INT, `spd_mach_a_m` INT, `spoil_l2` FLOAT, `spoil_l2c` FLOAT,
        |`spoil_l4` FLOAT, `spoil_l4c` FLOAT, `spoil_l5` FLOAT, `spoil_l5c` FLOAT, `spoil_r2` FLOAT, `spoil_r2c` FLOAT,
        |`spoil_r3` FLOAT, `spoil_r3c` FLOAT, `spoil_r5` FLOAT, `spoil_r5c` FLOAT, `spoil_val1` INT, `spoil_val2` INT,
        |`spoil_val3` INT, `spoil_val4` INT, `spoil_val5` INT, `sstck_cpt_ino` INT, `sstck_fo_ino` INT, `stab` FLOAT,
        |`stab_ld` INT, `stall_war` INT, `start_analysis` INT, `start_flight` INT, `surf_war` INT, `tail_wind` FLOAT,
        |`tas` FLOAT, `tas_csn` FLOAT, `tat_csn` FLOAT, `tat_to` FLOAT, `tcas_cmb_ctl` INT, `tcas_crw_sel` INT,
        |`tcas_dwn_adv` INT, `tcas_rac` INT, `tcas_ra_1` INT, `tcas_ra_10` INT, `tcas_ra_11` INT, `tcas_ra_12` INT,
        |`tcas_ra_2` INT, `tcas_ra_3` INT, `tcas_ra_4` INT, `tcas_ra_5` INT, `tcas_ra_6` INT, `tcas_ra_7` INT,
        |`tcas_ra_8` INT, `tcas_ra_9` INT, `tcas_ta` INT, `tcas_ta_1` INT, `tcas_ta_2` INT, `tcas_up_adv` INT,
        |`tcas_vrt_ctl` INT, `thrust_epr` INT, `thr_deficit` FLOAT, `time_1000` INT, `time_csn` INT,
        |`time_eng_start` INT, `time_eng_stop` INT, `time_ld` INT, `time_r` INT, `time_r_csn` INT, `time_to` INT,
        |`tla1` FLOAT, `tla1c` INT, `tla1_csn` FLOAT, `tla2` FLOAT, `tla2c` INT, `tla2_csn` FLOAT, `touch_gnd` INT,
        |`track_select` FLOAT, `trigger_code` INT, `ttdwn` INT, `v1` FLOAT, `v1_csn` FLOAT, `v2` FLOAT,
        |`v2_csn` FLOAT, `v2_min` FLOAT, `v2_to` FLOAT, `vapp` FLOAT, `vapp_ld` FLOAT, `vev` FLOAT, `vfe` FLOAT,
        |`vgdot` FLOAT, `vhf` INT, `vhf_cfa` INT, `vhf_keying_c_csn` INT, `vhf_keying_l_csn` INT,
        |`vhf_keying_r_csn` INT, `vib_n11_csn` FLOAT, `vib_n12_csn` FLOAT, `vib_n1fnt1` FLOAT, `vib_n1fnt2` FLOAT,
        |`vib_n21_csn` FLOAT, `vib_n22_csn` FLOAT, `vib_n2fnt1` FLOAT, `vib_n2fnt2` FLOAT, `vls` FLOAT,
        |`vls_csn` FLOAT, `vmax` FLOAT, `vmo` FLOAT, `vmo_mmo_ovs` INT, `vr` FLOAT, `vref` FLOAT, `vref_csn` FLOAT,
        |`vr_csn` FLOAT, `vs1g` FLOAT, `wea_alt` FLOAT, `wea_dewpoint` FLOAT, `wea_temp` FLOAT, `wea_valid` INT,
        |`wea_visib` FLOAT, `wea_windir` FLOAT, `wea_winspd` FLOAT, `win_dir` FLOAT, `win_dir_csn` FLOAT,
        |`win_spd` FLOAT, `win_spd_csn` FLOAT, `x_air` FLOAT, `y_air` FLOAT,
        |`flt_dt` STRING, `tail_num` STRING, `file_no` STRING
        |) USING lakesoul PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)
        |LOCATION 'hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_1hz_fillup'
        |""".stripMargin)
    spark.sql("alter table qara_mid.mid_qara_320020_1hz_fillup alter column flt_dt drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_1hz_fillup alter column tail_num drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_1hz_fillup alter column file_no drop not null;")
    partitionList.forEach(partition => {
      println("deal1hzTableData: begin deal with tuple: " + partition.toString());
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sql =
        s"""
           |insert into  qara_mid.mid_qara_320020_1hz_fillup
           |select
           |`time`
           |,1
           |,aapp
           |,aapp_td
           |,ac_tail1
           |,ac_tail2
           |,ac_tail3
           |,ac_type
           |,ac_type_csn
           |,aie1
           |,aie2
           |,aill
           |,ailr
           |,air_durn
           |,aiw
           |,alpha_floor
           |,alt_const
           |,alt_qnh
           |,alt_qnh_csn
           |,alt_qnh_ld
           |,alt_qnh_to
           |,alt_sel
           |,alt_sel_csn
           |,alt_std
           |,alt_stdc
           |,alt_stdc_csn
           |,aoal
           |,aoar
           |,apu_on
           |,apu_on_csn
           |,ap_egd1
           |,ap_egd1_csn
           |,ap_egd2
           |,ap_egd2_csn
           |,ap_off
           |,ap_off_csn
           |,arp_dist_dest
           |,arp_dist_dest_csn
           |,arp_dist_orig
           |,arp_heigh_dest
           |,arp_heigh_orig
           |,ats_active
           |,ats_egd
           |,ats_egdoract_csn
           |,ats_egd_csn
           |,ats_ret_mod
           |,ats_spd_mach
           |,ats_thrustn1
           |,auto_brk_flt
           |,auto_brk_off
           |,auto_land_wn
           |,auto_v2
           |,b3d_day
           |,b3d_hour
           |,b3d_min
           |,b3d_month
           |,badsf_nr
           |,baromb
           |,bea_mk
           |,brk_pdll
           |,brk_ped_rh
           |,cab_prs_war
           |,city_from_r_csn
           |,city_to
           |,city_to_r_csn
           |,ck_alt_sel
           |,ck_alt_std
           |,ck_c1_r1_col
           |,ck_c1_r1_txt
           |,ck_c1_r3_col
           |,ck_c1_r3_txt
           |,ck_c2_r1_col
           |,ck_c2_r1_txt
           |,ck_c2_r2_col
           |,ck_c2_r2_txt
           |,ck_c2_r3_col
           |,ck_c2_r3_txt
           |,ck_c3_r1_col
           |,ck_c3_r1_txt
           |,ck_c3_r2_col
           |,ck_c3_r2_txt
           |,ck_c4_r1_col
           |,ck_c4_r1_txt
           |,ck_c4_r2_col
           |,ck_c4_r2_txt
           |,ck_c4_r3_col
           |,ck_c4_r3_txt
           |,ck_c5_r1_col
           |,ck_c5_r1_txt
           |,ck_c5_r2_col
           |,ck_c5_r2_txt
           |,ck_c5_r3_col
           |,ck_c5_r3_txt
           |,ck_conf_pos
           |,ck_conf_pos_pc
           |,ck_egt1
           |,ck_egt2
           |,ck_epr1
           |,ck_epr2
           |,ck_flap_spd
           |,ck_fqty
           |,ck_gpws_mode
           |,ck_gw
           |,ck_head
           |,ck_head_mag
           |,ck_head_sel
           |,ck_head_selon
           |,ck_ias
           |,ck_ldg_seldw
           |,ck_ldg_wow
           |,ck_mach
           |,ck_mas_war
           |,ck_n11
           |,ck_n12
           |,ck_n21
           |,ck_n22
           |,ck_ralt
           |,ck_rev_dep1
           |,ck_rev_dep2
           |,ck_spd_brk
           |,ck_tcas_ra
           |,ck_tcas_ta
           |,ck_tla_pct1
           |,ck_tla_pct2
           |,ck_v1
           |,ck_v2
           |,ck_vapp
           |,ck_vhf1_emit
           |,ck_vls
           |,ck_vmax
           |,ck_vr
           |,conf
           |,conf_csn
           |,conf_ld
           |,conf_to
           |,cut
           |,`date`
           |,date_r
           |,date_r_csn
           |,date_to
           |,dat_year
           |,day_ld
           |,day_to
           |,debug
           |,destination
           |,dev_mag
           |,dfc_csn
           |,dfc_pos_cfa
           |,dist
           |,dist0
           |,dist_lat_td
           |,dist_ldg
           |,dist_ldg_csn
           |,dist_ldg_from_gs
           |,dist_pthr
           |,dist_to
           |,drift_csn
           |,dual_input
           |,durn_200_ld
           |,durn_txo
           |,egt1
           |,egt1c
           |,egt1c_csn
           |,egt1_csn
           |,egt2
           |,egt2c
           |,egt2c_csn
           |,egt2_csn
           |,egt_max
           |,elevl
           |,elevr
           |,eng1_severit
           |,eng2_severit
           |,eng_man
           |,epr1
           |,epr1c
           |,epr1_csn
           |,epr2
           |,epr2c
           |,epr2_csn
           |,epr_cmd_1
           |,epr_cmd_2
           |,epr_tgt1
           |,evt_mk
           |,fburn
           |,fburn1
           |,fburn1_txo
           |,fburn2
           |,fburn2_txo
           |,fburn3_txo
           |,fburn4_txo
           |,fburn_avg
           |,fburn_ti
           |,fburn_txo
           |,fburn_txo_avg
           |,fd_1
           |,fd_2
           |,ff1
           |,ff1c
           |,ff1_csn
           |,ff2
           |,ff2c
           |,ff2_csn
           |,fire1
           |,fire2
           |,fire_apu
           |,flap
           |,flapc
           |,flapc_csn
           |,flap_spd
           |,flight_chang
           |,flight_ident
           |,flight_input
           |,flight_no1
           |,flight_no1_csn
           |,flight_no2
           |,flight_no2_csn
           |,flight_phase
           |,flight_phase_csn
           |,flight_recogn
           |,flight_type
           |,flt_path_angl
           |,flt_path_angl_csn
           |,fma_lat
           |,fma_long
           |,fpa_selected
           |,fqty_ld
           |,fqty_to
           |,fr_count
           |,fuel_fire_1
           |,fuel_fire_2
           |,gamax1000
           |,gamax150
           |,gamax500
           |,gamin1000
           |,gamin150
           |,gamin500
           |,glide_devc
           |,glide_devc_csn
           |,glide_dev_max
           |,glide_dev_min
           |,glide_gap
           |,glide_ils
           |,gpws_ctype
           |,gpws_dont_sink
           |,gpws_flap_low
           |,gpws_gear_low
           |,gpws_glide
           |,gpws_mode
           |,gpws_mode_csn
           |,gpws_pull_up
           |,gpws_sink_rate
           |,gpws_tr_low
           |,gpws_tr_up
           |,gpws_war
           |,gpws_wsh_war
           |,gs
           |,gsc
           |,gsc1p
           |,gsc_csn
           |,gs_csn
           |,gwc
           |,gw_csn
           |,gw_landing_lim
           |,gw_ld
           |,gw_takeoff_lim
           |,gw_taxi_lim
           |,gw_to
           |,head
           |,head_csn
           |,head_lin
           |,head_lin_csn
           |,head_mag
           |,head_sel
           |,head_true
           |,head_true_csn
           |,height
           |,height_csn
           |,heig_1conf_chg
           |,heig_dev_1000
           |,heig_dev_500
           |,heig_gear_dw
           |,heig_lconf_chg
           |,hf
           |,hf_cfa
           |,hp_fuel_vv_1
           |,hp_fuel_vv_2
           |,ias
           |,iasc
           |,iasc_csn
           |,ias_app_1000
           |,ias_app_50
           |,ias_app_500
           |,ias_bef_ld
           |,ias_cl_1000
           |,ias_cl_500
           |,ias_csn
           |,ias_ext_conf1
           |,ias_ext_conf2
           |,ias_ext_conf3
           |,ias_ext_conf4
           |,ias_ext_conf5
           |,ias_ld
           |,ias_max
           |,ias_maxcnf1ld
           |,ias_maxcnf2ld
           |,ias_maxcnf3ld
           |,ias_maxcnf4ld
           |,ias_maxcnf5ld
           |,ias_maxcnf6ld
           |,ias_maxcnf7ld
           |,ias_maxcnf8ld
           |,ias_maxconf1to
           |,ias_maxconf2to
           |,ias_maxconf3to
           |,ias_maxconf4to
           |,ias_maxconf5to
           |,ias_max_geardw
           |,ias_min_finapp
           |,ias_to
           |,ias_to_50
           |,ils_lim
           |,ils_val
           |,in_flight
           |,ivv
           |,ivv_csn
           |,ivv_max_bl2000
           |,ivv_min_cl35
           |,ivv_min_cl400
           |,ivv_sel
           |,latpc
           |,latpc_csn
           |,lat_air
           |,ldgl
           |,ldgl_csn
           |,ldgnos
           |,ldgnos_csn
           |,ldgr
           |,ldgr_csn
           |,ldg_seldw_csn
           |,ldg_unlokdw
           |,lift_gnd
           |,loc_devc
           |,loc_devc_csn
           |,loc_gap
           |,long_air
           |,lonpc
           |,lonpc_csn
           |,low_press_1
           |,low_press_2
           |,lo_armed
           |,mach
           |,mach_buff
           |,mach_buff_csn
           |,mach_csn
           |,mach_max
           |,mas_cau
           |,mas_cau_csn
           |,mas_war
           |,max_armed
           |,med_armed
           |,min_alt_n1_40
           |,min_n1_40
           |,mmo
           |,n11c
           |,n11_csn
           |,n12c
           |,n12_csn
           |,n1_cmd1
           |,n1_cmd2
           |,n1_epr1
           |,n1_epr2
           |,n1_max_txo3min
           |,n1_min_bl500
           |,n1_ths_tgt
           |,n21
           |,n21c
           |,n21_csn
           |,n22
           |,n22c
           |,n22_csn
           |,oil_qty1_csn
           |,oil_qty2_csn
           |,one_eng_app
           |,origin
           |,par_qual
           |,pass_no
           |,pf
           |,pilot_ld
           |,pilot_to
           |,pitch_altr
           |,ralt1c
           |,ralt2c
           |,raltc
           |,raltc_csn
           |,rate
           |,refused_trans
           |,relief
           |,rev_deployd1
           |,rev_deployd2
           |,rev_unlock_1
           |,rev_unlock_2
           |,roll_cpt
           |,roll_fo
           |,runway_ld
           |,runway_ld_csn
           |,runway_to
           |,runway_to_csn
           |,sat
           |,satr_csn
           |,sf_count
           |,single_txo
           |,slat
           |,slatc
           |,slatc_csn
           |,slatrw
           |,slat_spd
           |,smk_avi_war
           |,smk_crg_war
           |,smk_lvy_war
           |,spd_mach_a_m
           |,spoil_l2
           |,spoil_l2c
           |,spoil_l4
           |,spoil_l4c
           |,spoil_l5
           |,spoil_l5c
           |,spoil_r2
           |,spoil_r2c
           |,spoil_r3
           |,spoil_r3c
           |,spoil_r5
           |,spoil_r5c
           |,spoil_val1
           |,spoil_val2
           |,spoil_val3
           |,spoil_val4
           |,spoil_val5
           |,sstck_cpt_ino
           |,sstck_fo_ino
           |,stab
           |,stab_ld
           |,stall_war
           |,start_analysis
           |,start_flight
           |,surf_war
           |,tail_wind
           |,tas
           |,tas_csn
           |,tat_csn
           |,tat_to
           |,tcas_cmb_ctl
           |,tcas_crw_sel
           |,tcas_dwn_adv
           |,tcas_rac
           |,tcas_ra_1
           |,tcas_ra_10
           |,tcas_ra_11
           |,tcas_ra_12
           |,tcas_ra_2
           |,tcas_ra_3
           |,tcas_ra_4
           |,tcas_ra_5
           |,tcas_ra_6
           |,tcas_ra_7
           |,tcas_ra_8
           |,tcas_ra_9
           |,tcas_ta
           |,tcas_ta_1
           |,tcas_ta_2
           |,tcas_up_adv
           |,tcas_vrt_ctl
           |,thrust_epr
           |,thr_deficit
           |,time_1000
           |,time_csn
           |,time_eng_start
           |,time_eng_stop
           |,time_ld
           |,time_r
           |,time_r_csn
           |,time_to
           |,tla1
           |,tla1c
           |,tla1_csn
           |,tla2
           |,tla2c
           |,tla2_csn
           |,touch_gnd
           |,track_select
           |,trigger_code
           |,ttdwn
           |,v1
           |,v1_csn
           |,v2
           |,v2_csn
           |,v2_min
           |,v2_to
           |,vapp
           |,vapp_ld
           |,vev
           |,vfe
           |,vgdot
           |,vhf
           |,vhf_cfa
           |,vhf_keying_c_csn
           |,vhf_keying_l_csn
           |,vhf_keying_r_csn
           |,vib_n11_csn
           |,vib_n12_csn
           |,vib_n1fnt1
           |,vib_n1fnt2
           |,vib_n21_csn
           |,vib_n22_csn
           |,vib_n2fnt1
           |,vib_n2fnt2
           |,vls
           |,vls_csn
           |,vmax
           |,vmo
           |,vmo_mmo_ovs
           |,vr
           |,vref
           |,vref_csn
           |,vr_csn
           |,vs1g
           |,wea_alt
           |,wea_dewpoint
           |,wea_temp
           |,wea_valid
           |,wea_visib
           |,wea_windir
           |,wea_winspd
           |,win_dir
           |,win_dir_csn
           |,win_spd
           |,win_spd_csn
           |,x_air
           |,y_air
           |,flt_dt
           |,tail_num
           |,file_no
           |from  czods.s_qara_320020_1hz
           |where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo' and time_series=0
           |""".stripMargin
      spark.sql(sql)
    })
  }

  private def deal2hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_2hz_fillup_temp;")
    spark.sql(
      """create table if not exists qara_mid.mid_qara_320020_2hz_fillup_temp (
        |`time` INT, `time_series` INT,
        |`ck_pitch_cpt` FLOAT, `ck_pitch_fo` FLOAT, `ck_roll_cpt` FLOAT, `ck_roll_fo` FLOAT,
        |`ck_rudd` FLOAT,`pitch_cpt` FLOAT, `pitch_fo` FLOAT, `roll` FLOAT, `roll_abs` FLOAT,
        |`roll_csn` FLOAT,`roll_max_ab500` FLOAT, `roll_max_bl100` FLOAT, `roll_max_bl20` FLOAT,
        |`roll_max_bl500` FLOAT,`rudd` FLOAT, `spoil_gnd_arm` INT, `spoil_lout1` INT,
        |`spoil_rout1` INT,`yaw_faul1` INT, `yaw_faul2` INT,
        |`flt_dt` STRING, `tail_num` STRING,`file_no` STRING
        |) USING lakesoul PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)
        |LOCATION 'hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_2hz_fillup_temp'
        |""".stripMargin)
    spark.sql("alter table qara_mid.mid_qara_320020_2hz_fillup_temp alter column flt_dt drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_2hz_fillup_temp alter column tail_num drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_2hz_fillup_temp alter column file_no drop not null;")
    spark.sql("drop table if exists qara_mid.mid_qara_320020_2hz_fillup;")
    spark.sql(
      """create table if not exists qara_mid.mid_qara_320020_2hz_fillup (
        |`time` INT, `time_series` INT,
        |`ck_pitch_cpt` FLOAT, `ck_pitch_fo` FLOAT, `ck_roll_cpt` FLOAT, `ck_roll_fo` FLOAT,
        |`ck_rudd` FLOAT,`pitch_cpt` FLOAT, `pitch_fo` FLOAT, `roll` FLOAT, `roll_abs` FLOAT,
        |`roll_csn` FLOAT,`roll_max_ab500` FLOAT, `roll_max_bl100` FLOAT, `roll_max_bl20` FLOAT,
        |`roll_max_bl500` FLOAT,`rudd` FLOAT, `spoil_gnd_arm` INT, `spoil_lout1` INT, `spoil_rout1` INT,
        |`yaw_faul1` INT, `yaw_faul2` INT,
        |`flt_dt` STRING, `tail_num` STRING,`file_no` STRING
        |) USING lakesoul PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)
        |LOCATION 'hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_2hz_fillup'
        |""".stripMargin)
    spark.sql("alter table qara_mid.mid_qara_320020_2hz_fillup alter column flt_dt drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_2hz_fillup alter column tail_num drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_2hz_fillup alter column file_no drop not null;")
    partitionList.forEach(partition => {
      println("deal2hzTableData: begin deal with tuple: " + partition.toString());
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sql =
        s"""
           |insert into  qara_mid.mid_qara_320020_2hz_fillup_temp
           |select t0.`time`,t0.time_series
           |,COALESCE(t0.ck_pitch_cpt,t1.ck_pitch_cpt,t2.ck_pitch_cpt,t3.ck_pitch_cpt,t4.ck_pitch_cpt,t5.ck_pitch_cpt,t6.ck_pitch_cpt,t7.ck_pitch_cpt) as ck_pitch_cpt
           |,COALESCE(t0.ck_pitch_fo,t1.ck_pitch_fo,t2.ck_pitch_fo,t3.ck_pitch_fo,t4.ck_pitch_fo,t5.ck_pitch_fo,t6.ck_pitch_fo,t7.ck_pitch_fo) as ck_pitch_fo
           |,COALESCE(t0.ck_roll_cpt,t1.ck_roll_cpt,t2.ck_roll_cpt,t3.ck_roll_cpt,t4.ck_roll_cpt,t5.ck_roll_cpt,t6.ck_roll_cpt,t7.ck_roll_cpt) as ck_roll_cpt
           |,COALESCE(t0.ck_roll_fo,t1.ck_roll_fo,t2.ck_roll_fo,t3.ck_roll_fo,t4.ck_roll_fo,t5.ck_roll_fo,t6.ck_roll_fo,t7.ck_roll_fo) as ck_roll_fo
           |,COALESCE(t0.ck_rudd,t1.ck_rudd,t2.ck_rudd,t3.ck_rudd,t4.ck_rudd,t5.ck_rudd,t6.ck_rudd,t7.ck_rudd) as ck_rudd
           |,COALESCE(t0.pitch_cpt,t1.pitch_cpt,t2.pitch_cpt,t3.pitch_cpt,t4.pitch_cpt,t5.pitch_cpt,t6.pitch_cpt,t7.pitch_cpt) as pitch_cpt
           |,COALESCE(t0.pitch_fo,t1.pitch_fo,t2.pitch_fo,t3.pitch_fo,t4.pitch_fo,t5.pitch_fo,t6.pitch_fo,t7.pitch_fo) as pitch_fo
           |,COALESCE(t0.roll,t1.roll,t2.roll,t3.roll,t4.roll,t5.roll,t6.roll,t7.roll) as roll
           |,COALESCE(t0.roll_abs,t1.roll_abs,t2.roll_abs,t3.roll_abs,t4.roll_abs,t5.roll_abs,t6.roll_abs,t7.roll_abs) as roll_abs
           |,COALESCE(t0.roll_csn,t1.roll_csn,t2.roll_csn,t3.roll_csn,t4.roll_csn,t5.roll_csn,t6.roll_csn,t7.roll_csn) as roll_csn
           |,COALESCE(t0.roll_max_ab500,t1.roll_max_ab500,t2.roll_max_ab500,t3.roll_max_ab500,t4.roll_max_ab500,t5.roll_max_ab500,t6.roll_max_ab500,t7.roll_max_ab500) as roll_max_ab500
           |,COALESCE(t0.roll_max_bl100,t1.roll_max_bl100,t2.roll_max_bl100,t3.roll_max_bl100,t4.roll_max_bl100,t5.roll_max_bl100,t6.roll_max_bl100,t7.roll_max_bl100) as roll_max_bl100
           |,COALESCE(t0.roll_max_bl20,t1.roll_max_bl20,t2.roll_max_bl20,t3.roll_max_bl20,t4.roll_max_bl20,t5.roll_max_bl20,t6.roll_max_bl20,t7.roll_max_bl20) as roll_max_bl20
           |,COALESCE(t0.roll_max_bl500,t1.roll_max_bl500,t2.roll_max_bl500,t3.roll_max_bl500,t4.roll_max_bl500,t5.roll_max_bl500,t6.roll_max_bl500,t7.roll_max_bl500) as roll_max_bl500
           |,COALESCE(t0.rudd,t1.rudd,t2.rudd,t3.rudd,t4.rudd,t5.rudd,t6.rudd,t7.rudd) as rudd
           |,COALESCE(t0.spoil_gnd_arm,t1.spoil_gnd_arm,t2.spoil_gnd_arm,t3.spoil_gnd_arm,t4.spoil_gnd_arm,t5.spoil_gnd_arm,t6.spoil_gnd_arm,t7.spoil_gnd_arm) as spoil_gnd_arm
           |,COALESCE(t0.spoil_lout1,t1.spoil_lout1,t2.spoil_lout1,t3.spoil_lout1,t4.spoil_lout1,t5.spoil_lout1,t6.spoil_lout1,t7.spoil_lout1) as spoil_lout1
           |,COALESCE(t0.spoil_rout1,t1.spoil_rout1,t2.spoil_rout1,t3.spoil_rout1,t4.spoil_rout1,t5.spoil_rout1,t6.spoil_rout1,t7.spoil_rout1) as spoil_rout1
           |,COALESCE(t0.yaw_faul1,t1.yaw_faul1,t2.yaw_faul1,t3.yaw_faul1,t4.yaw_faul1,t5.yaw_faul1,t6.yaw_faul1,t7.yaw_faul1) as yaw_faul1
           |,COALESCE(t0.yaw_faul2,t1.yaw_faul2,t2.yaw_faul2,t3.yaw_faul2,t4.yaw_faul2,t5.yaw_faul2,t6.yaw_faul2,t7.yaw_faul2) as yaw_faul2
           |,t0.flt_dt,t0.tail_num,t0.file_no
           |from
           |(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where    flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'     ) t0
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where    flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'    ) t1
           |on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'      ) t2
           |on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add
           |    left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where   flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'     ) t3
           |on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+4 as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'       ) t4
           |on t0.flt_dt=t4.flt_dt and t0.file_no=t4.file_no and t0.tail_num=t4.tail_num and t0.row_id_add=t4.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+5 as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'      ) t5
           |on t0.flt_dt=t5.flt_dt and t0.file_no=t5.file_no and t0.tail_num=t5.tail_num and t0.row_id_add=t5.row_id_add
           |    left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+6 as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where   flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'     ) t6
           |on t0.flt_dt=t6.flt_dt and t0.file_no=t6.file_no and t0.tail_num=t6.tail_num and t0.row_id_add=t6.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+7 as row_id_add,*
           |    from  czods.s_qara_320020_2hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'       ) t7
           |on t0.flt_dt=t7.flt_dt and t0.file_no=t7.file_no and t0.tail_num=t7.tail_num and t0.row_id_add=t7.row_id_add
           |""".stripMargin
      spark.sql(sql)

      val sql1 = """
                   |insert into  qara_mid.mid_qara_320020_2hz_fillup
                   |select t0.`time`,t0.time_series,COALESCE(t0.ck_pitch_cpt,t1.ck_pitch_cpt,t2.ck_pitch_cpt,t3.ck_pitch_cpt,t4.ck_pitch_cpt,t5.ck_pitch_cpt,t6.ck_pitch_cpt,t7.ck_pitch_cpt,t8.ck_pitch_cpt) as ck_pitch_cpt,COALESCE(t0.ck_pitch_fo,t1.ck_pitch_fo,t2.ck_pitch_fo,t3.ck_pitch_fo,t4.ck_pitch_fo,t5.ck_pitch_fo,t6.ck_pitch_fo,t7.ck_pitch_fo,t8.ck_pitch_fo) as ck_pitch_fo,COALESCE(t0.ck_roll_cpt,t1.ck_roll_cpt,t2.ck_roll_cpt,t3.ck_roll_cpt,t4.ck_roll_cpt,t5.ck_roll_cpt,t6.ck_roll_cpt,t7.ck_roll_cpt,t8.ck_roll_cpt) as ck_roll_cpt,COALESCE(t0.ck_roll_fo,t1.ck_roll_fo,t2.ck_roll_fo,t3.ck_roll_fo,t4.ck_roll_fo,t5.ck_roll_fo,t6.ck_roll_fo,t7.ck_roll_fo,t8.ck_roll_fo) as ck_roll_fo,COALESCE(t0.ck_rudd,t1.ck_rudd,t2.ck_rudd,t3.ck_rudd,t4.ck_rudd,t5.ck_rudd,t6.ck_rudd,t7.ck_rudd,t8.ck_rudd) as ck_rudd,COALESCE(t0.pitch_cpt,t1.pitch_cpt,t2.pitch_cpt,t3.pitch_cpt,t4.pitch_cpt,t5.pitch_cpt,t6.pitch_cpt,t7.pitch_cpt,t8.pitch_cpt) as pitch_cpt,COALESCE(t0.pitch_fo,t1.pitch_fo,t2.pitch_fo,t3.pitch_fo,t4.pitch_fo,t5.pitch_fo,t6.pitch_fo,t7.pitch_fo,t8.pitch_fo) as pitch_fo,COALESCE(t0.roll,t1.roll,t2.roll,t3.roll,t4.roll,t5.roll,t6.roll,t7.roll,t8.roll) as roll,COALESCE(t0.roll_abs,t1.roll_abs,t2.roll_abs,t3.roll_abs,t4.roll_abs,t5.roll_abs,t6.roll_abs,t7.roll_abs,t8.roll_abs) as roll_abs,COALESCE(t0.roll_csn,t1.roll_csn,t2.roll_csn,t3.roll_csn,t4.roll_csn,t5.roll_csn,t6.roll_csn,t7.roll_csn,t8.roll_csn) as roll_csn,COALESCE(t0.roll_max_ab500,t1.roll_max_ab500,t2.roll_max_ab500,t3.roll_max_ab500,t4.roll_max_ab500,t5.roll_max_ab500,t6.roll_max_ab500,t7.roll_max_ab500,t8.roll_max_ab500) as roll_max_ab500,COALESCE(t0.roll_max_bl100,t1.roll_max_bl100,t2.roll_max_bl100,t3.roll_max_bl100,t4.roll_max_bl100,t5.roll_max_bl100,t6.roll_max_bl100,t7.roll_max_bl100,t8.roll_max_bl100) as roll_max_bl100,COALESCE(t0.roll_max_bl20,t1.roll_max_bl20,t2.roll_max_bl20,t3.roll_max_bl20,t4.roll_max_bl20,t5.roll_max_bl20,t6.roll_max_bl20,t7.roll_max_bl20,t8.roll_max_bl20) as roll_max_bl20,COALESCE(t0.roll_max_bl500,t1.roll_max_bl500,t2.roll_max_bl500,t3.roll_max_bl500,t4.roll_max_bl500,t5.roll_max_bl500,t6.roll_max_bl500,t7.roll_max_bl500,t8.roll_max_bl500) as roll_max_bl500,COALESCE(t0.rudd,t1.rudd,t2.rudd,t3.rudd,t4.rudd,t5.rudd,t6.rudd,t7.rudd,t8.rudd) as rudd,COALESCE(t0.spoil_gnd_arm,t1.spoil_gnd_arm,t2.spoil_gnd_arm,t3.spoil_gnd_arm,t4.spoil_gnd_arm,t5.spoil_gnd_arm,t6.spoil_gnd_arm,t7.spoil_gnd_arm,t8.spoil_gnd_arm) as spoil_gnd_arm,COALESCE(t0.spoil_lout1,t1.spoil_lout1,t2.spoil_lout1,t3.spoil_lout1,t4.spoil_lout1,t5.spoil_lout1,t6.spoil_lout1,t7.spoil_lout1,t8.spoil_lout1) as spoil_lout1,COALESCE(t0.spoil_rout1,t1.spoil_rout1,t2.spoil_rout1,t3.spoil_rout1,t4.spoil_rout1,t5.spoil_rout1,t6.spoil_rout1,t7.spoil_rout1,t8.spoil_rout1) as spoil_rout1,COALESCE(t0.yaw_faul1,t1.yaw_faul1,t2.yaw_faul1,t3.yaw_faul1,t4.yaw_faul1,t5.yaw_faul1,t6.yaw_faul1,t7.yaw_faul1,t8.yaw_faul1) as yaw_faul1,COALESCE(t0.yaw_faul2,t1.yaw_faul2,t2.yaw_faul2,t3.yaw_faul2,t4.yaw_faul2,t5.yaw_faul2,t6.yaw_faul2,t7.yaw_faul2,t8.yaw_faul2) as yaw_faul2,t0.flt_dt,t0.tail_num,t0.file_no
                   |from
                   |(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'            ) t0
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'           ) t1
                   |on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'          ) t2
                   |on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add
                   |    left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,*
                   |    from  czods.s_qara_320020_2hz
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'           ) t3
                   |on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+4 as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'           ) t4
                   |on t0.flt_dt=t4.flt_dt and t0.file_no=t4.file_no and t0.tail_num=t4.tail_num and t0.row_id_add=t4.row_id_add
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+5 as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'          ) t5
                   |on t0.flt_dt=t5.flt_dt and t0.file_no=t5.file_no and t0.tail_num=t5.tail_num and t0.row_id_add=t5.row_id_add
                   |    left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+6 as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'           ) t6
                   |on t0.flt_dt=t6.flt_dt and t0.file_no=t6.file_no and t0.tail_num=t6.tail_num and t0.row_id_add=t6.row_id_add
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+7 as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'           ) t7
                   |on t0.flt_dt=t7.flt_dt and t0.file_no=t7.file_no and t0.tail_num=t7.tail_num and t0.row_id_add=t7.row_id_add
                   |left join
                   |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+8 as row_id_add,*
                   |    from  qara_mid.mid_qara_320020_2hz_fillup_temp
                   |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'           ) t8
                   |on t0.flt_dt=t8.flt_dt and t0.file_no=t8.file_no and t0.tail_num=t8.tail_num and t0.row_id_add=t8.row_id_add""".stripMargin
      spark.sql(sql1)
    })

  }

  private def deal4hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_4hz_fillup;")
    spark.sql(
      """create table if not exists qara_mid.mid_qara_320020_4hz_fillup (
        |`time` INT,
        |`time_series` INT,`latg` FLOAT, `latg_csn` FLOAT, `lift_off_pitch1` FLOAT,
        |`lift_off_pitch2` FLOAT, `lift_off_pitch3` FLOAT, `lift_off_pitch4` FLOAT,
        |`lift_off_raltc1` FLOAT, `lift_off_raltc2` FLOAT, `lift_off_raltc3` FLOAT,
        |`lift_off_raltc4` FLOAT,`lift_off_t1` INT, `lift_off_t2` INT, `lift_off_t3` INT,
        |`lift_off_t4` INT,`long` FLOAT, `longc` FLOAT, `long_csn` FLOAT,`pitch` FLOAT,
        |`pitch_csn` FLOAT, `pitch_max_ld` FLOAT, `pitch_max_to` FLOAT,`pitch_ratavgto` FLOAT,
        |`pitch_rate` FLOAT, `pitch_rate_csn` FLOAT, `pitch_ratmaxto` FLOAT,
        |`flt_dt` STRING, `tail_num` STRING, `file_no` STRING
        |) USING lakesoul PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)
        |LOCATION 'hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_4hz_fillup'
        |""".stripMargin)
    spark.sql("alter table qara_mid.mid_qara_320020_4hz_fillup alter column flt_dt drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_4hz_fillup alter column tail_num drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_4hz_fillup alter column file_no drop not null;")
    partitionList.forEach(partition => {
      println("deal4hzTableData: begin deal with tuple: " + partition.toString());
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sql =
        s"""
           |insert into  qara_mid.mid_qara_320020_4hz_fillup
           |select t0.`time`,t0.time_series,COALESCE(t0.latg,t1.latg,t2.latg,t3.latg,t4.latg,t5.latg,t6.latg,t7.latg) as latg,COALESCE(t0.latg_csn,t1.latg_csn,t2.latg_csn,t3.latg_csn,t4.latg_csn,t5.latg_csn,t6.latg_csn,t7.latg_csn) as latg_csn,COALESCE(t0.lift_off_pitch1,t1.lift_off_pitch1,t2.lift_off_pitch1,t3.lift_off_pitch1,t4.lift_off_pitch1,t5.lift_off_pitch1,t6.lift_off_pitch1,t7.lift_off_pitch1) as lift_off_pitch1,COALESCE(t0.lift_off_pitch2,t1.lift_off_pitch2,t2.lift_off_pitch2,t3.lift_off_pitch2,t4.lift_off_pitch2,t5.lift_off_pitch2,t6.lift_off_pitch2,t7.lift_off_pitch2) as lift_off_pitch2,COALESCE(t0.lift_off_pitch3,t1.lift_off_pitch3,t2.lift_off_pitch3,t3.lift_off_pitch3,t4.lift_off_pitch3,t5.lift_off_pitch3,t6.lift_off_pitch3,t7.lift_off_pitch3) as lift_off_pitch3,COALESCE(t0.lift_off_pitch4,t1.lift_off_pitch4,t2.lift_off_pitch4,t3.lift_off_pitch4,t4.lift_off_pitch4,t5.lift_off_pitch4,t6.lift_off_pitch4,t7.lift_off_pitch4) as lift_off_pitch4,COALESCE(t0.lift_off_raltc1,t1.lift_off_raltc1,t2.lift_off_raltc1,t3.lift_off_raltc1,t4.lift_off_raltc1,t5.lift_off_raltc1,t6.lift_off_raltc1,t7.lift_off_raltc1) as lift_off_raltc1,COALESCE(t0.lift_off_raltc2,t1.lift_off_raltc2,t2.lift_off_raltc2,t3.lift_off_raltc2,t4.lift_off_raltc2,t5.lift_off_raltc2,t6.lift_off_raltc2,t7.lift_off_raltc2) as lift_off_raltc2,COALESCE(t0.lift_off_raltc3,t1.lift_off_raltc3,t2.lift_off_raltc3,t3.lift_off_raltc3,t4.lift_off_raltc3,t5.lift_off_raltc3,t6.lift_off_raltc3,t7.lift_off_raltc3) as lift_off_raltc3,COALESCE(t0.lift_off_raltc4,t1.lift_off_raltc4,t2.lift_off_raltc4,t3.lift_off_raltc4,t4.lift_off_raltc4,t5.lift_off_raltc4,t6.lift_off_raltc4,t7.lift_off_raltc4) as lift_off_raltc4,COALESCE(t0.lift_off_t1,t1.lift_off_t1,t2.lift_off_t1,t3.lift_off_t1,t4.lift_off_t1,t5.lift_off_t1,t6.lift_off_t1,t7.lift_off_t1) as lift_off_t1,COALESCE(t0.lift_off_t2,t1.lift_off_t2,t2.lift_off_t2,t3.lift_off_t2,t4.lift_off_t2,t5.lift_off_t2,t6.lift_off_t2,t7.lift_off_t2) as lift_off_t2,COALESCE(t0.lift_off_t3,t1.lift_off_t3,t2.lift_off_t3,t3.lift_off_t3,t4.lift_off_t3,t5.lift_off_t3,t6.lift_off_t3,t7.lift_off_t3) as lift_off_t3,COALESCE(t0.lift_off_t4,t1.lift_off_t4,t2.lift_off_t4,t3.lift_off_t4,t4.lift_off_t4,t5.lift_off_t4,t6.lift_off_t4,t7.lift_off_t4) as lift_off_t4,COALESCE(t0.long,t1.long,t2.long,t3.long,t4.long,t5.long,t6.long,t7.long) as long,COALESCE(t0.longc,t1.longc,t2.longc,t3.longc,t4.longc,t5.longc,t6.longc,t7.longc) as longc,COALESCE(t0.long_csn,t1.long_csn,t2.long_csn,t3.long_csn,t4.long_csn,t5.long_csn,t6.long_csn,t7.long_csn) as long_csn,COALESCE(t0.pitch,t1.pitch,t2.pitch,t3.pitch,t4.pitch,t5.pitch,t6.pitch,t7.pitch) as pitch,COALESCE(t0.pitch_csn,t1.pitch_csn,t2.pitch_csn,t3.pitch_csn,t4.pitch_csn,t5.pitch_csn,t6.pitch_csn,t7.pitch_csn) as pitch_csn,COALESCE(t0.pitch_max_ld,t1.pitch_max_ld,t2.pitch_max_ld,t3.pitch_max_ld,t4.pitch_max_ld,t5.pitch_max_ld,t6.pitch_max_ld,t7.pitch_max_ld) as pitch_max_ld,COALESCE(t0.pitch_max_to,t1.pitch_max_to,t2.pitch_max_to,t3.pitch_max_to,t4.pitch_max_to,t5.pitch_max_to,t6.pitch_max_to,t7.pitch_max_to) as pitch_max_to,COALESCE(t0.pitch_ratavgto,t1.pitch_ratavgto,t2.pitch_ratavgto,t3.pitch_ratavgto,t4.pitch_ratavgto,t5.pitch_ratavgto,t6.pitch_ratavgto,t7.pitch_ratavgto) as pitch_ratavgto,COALESCE(t0.pitch_rate,t1.pitch_rate,t2.pitch_rate,t3.pitch_rate,t4.pitch_rate,t5.pitch_rate,t6.pitch_rate,t7.pitch_rate) as pitch_rate,COALESCE(t0.pitch_rate_csn,t1.pitch_rate_csn,t2.pitch_rate_csn,t3.pitch_rate_csn,t4.pitch_rate_csn,t5.pitch_rate_csn,t6.pitch_rate_csn,t7.pitch_rate_csn) as pitch_rate_csn,COALESCE(t0.pitch_ratmaxto,t1.pitch_ratmaxto,t2.pitch_ratmaxto,t3.pitch_ratmaxto,t4.pitch_ratmaxto,t5.pitch_ratmaxto,t6.pitch_ratmaxto,t7.pitch_ratmaxto) as pitch_ratmaxto,t0.flt_dt,t0.tail_num,t0.file_no
           |from
           |(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'          ) t0
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'         ) t1
           |on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'        ) t2
           |on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add
           |    left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'         ) t3
           |on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+4 as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'         ) t4
           |on t0.flt_dt=t4.flt_dt and t0.file_no=t4.file_no and t0.tail_num=t4.tail_num and t0.row_id_add=t4.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+5 as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'        ) t5
           |on t0.flt_dt=t5.flt_dt and t0.file_no=t5.file_no and t0.tail_num=t5.tail_num and t0.row_id_add=t5.row_id_add
           |    left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+6 as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'         ) t6
           |on t0.flt_dt=t6.flt_dt and t0.file_no=t6.file_no and t0.tail_num=t6.tail_num and t0.row_id_add=t6.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+7 as row_id_add,*
           |    from  czods.s_qara_320020_4hz
           |    where flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'         ) t7
           |on t0.flt_dt=t7.flt_dt and t0.file_no=t7.file_no and t0.tail_num=t7.tail_num and t0.row_id_add=t7.row_id_add
           |""".stripMargin
      spark.sql(sql)
    })
  }

  private def deal8hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_8hz_fillup;")
    spark.sql(
      """create table if not exists qara_mid.mid_qara_320020_8hz_fillup (
        |`time` INT,
        |`time_series` INT,`bad_sf` INT, `lift_off` INT, `pad` INT, `t` INT,
        |`time_stamp` INT, `time_tag` INT, `touch_down` INT, `turbulence` INT,
        |`value_st` INT, `vrtg` FLOAT, `vrtg_avg` FLOAT, `vrtg_csn` FLOAT,
        |`vrtg_max_air` FLOAT, `vrtg_max_gnd` FLOAT, `vrtg_max_ld` FLOAT,
        |`flt_dt` STRING, `tail_num` STRING, `file_no` STRING
        |)USING lakesoul PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)
        |LOCATION 'hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_8hz_fillup'
        |""".stripMargin)
    spark.sql("alter table qara_mid.mid_qara_320020_8hz_fillup alter column flt_dt drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_8hz_fillup alter column tail_num drop not null;")
    spark.sql("alter table qara_mid.mid_qara_320020_8hz_fillup alter column file_no drop not null;")
    partitionList.forEach(partition => {
      println("deal8hzTableData: begin deal with tuple: " + partition.toString());
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sql =
        s"""
           |insert into  qara_mid.mid_qara_320020_8hz_fillup
           |select t0.`time`,t0.time_series
           |,COALESCE(t0.bad_sf,t1.bad_sf,t2.bad_sf,t3.bad_sf) as bad_sf
           |,COALESCE(t0.lift_off,t1.lift_off,t2.lift_off,t3.lift_off) as lift_off
           |,COALESCE(t0.pad,t1.pad,t2.pad,t3.pad) as pad
           |,COALESCE(t0.t,t1.t,t2.t,t3.t) as t
           |,COALESCE(t0.time_stamp,t1.time_stamp,t2.time_stamp,t3.time_stamp) as time_stamp
           |,COALESCE(t0.time_tag,t1.time_tag,t2.time_tag,t3.time_tag) as time_tag
           |,COALESCE(t0.touch_down,t1.touch_down,t2.touch_down,t3.touch_down) as touch_down
           |,COALESCE(t0.turbulence,t1.turbulence,t2.turbulence,t3.turbulence) as turbulence
           |,COALESCE(t0.value_st,t1.value_st,t2.value_st,t3.value_st) as value_st
           |,COALESCE(t0.vrtg,t1.vrtg,t2.vrtg,t3.vrtg) as vrtg
           |,COALESCE(t0.vrtg_avg,t1.vrtg_avg,t2.vrtg_avg,t3.vrtg_avg) as vrtg_avg
           |,COALESCE(t0.vrtg_csn,t1.vrtg_csn,t2.vrtg_csn,t3.vrtg_csn) as vrtg_csn
           |,COALESCE(t0.vrtg_max_air,t1.vrtg_max_air,t2.vrtg_max_air,t3.vrtg_max_air) as vrtg_max_air
           |,COALESCE(t0.vrtg_max_gnd,t1.vrtg_max_gnd,t2.vrtg_max_gnd,t3.vrtg_max_gnd) as vrtg_max_gnd
           |,COALESCE(t0.vrtg_max_ld,t1.vrtg_max_ld,t2.vrtg_max_ld,t3.vrtg_max_ld) as vrtg_max_ld
           |,t0.flt_dt,t0.tail_num,t0.file_no
           |from
           |(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*
           |    from  czods.s_qara_320020_8hz
           |    where   flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'      ) t0
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*
           |    from  czods.s_qara_320020_8hz
           |    where   flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'     ) t1
           |on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add
           |left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*
           |    from  czods.s_qara_320020_8hz
           |    where   flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'    ) t2
           |on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add
           |    left join
           |(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,*
           |    from  czods.s_qara_320020_8hz
           |    where  flt_dt='$fltDt' and tail_num='$tailNum' and file_no='$fileNo'    ) t3
           |on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add
           |""".stripMargin
      spark.sql(sql)
    })
  }

  private def getPartitions(inputTableName: String, outputTableName: String, dayList: util.List[String]): util.ArrayList[Row] = {
    val partitionList = new util.ArrayList[Row]
    val dbManager = new DBManager
    val inputTableIdent = inputTableName.split("\\.")
    val inputTableInfo = dbManager.getTableInfoByNameAndNamespace(inputTableIdent(1), inputTableIdent(0))
    val inputTablePartitionInfo = dbManager.getAllPartitionInfo(inputTableInfo.getTableId)
    println("============= PartitionInfo size: " + inputTablePartitionInfo.size + " =============")
    val inputTablePartitionStrList = new util.ArrayList[String]
    val outputTableIdent = outputTableName.split("\\.")
    val outputTableInfo = dbManager.getTableInfoByNameAndNamespace(outputTableIdent(1), outputTableIdent(0))
    val outputTablePartitionInfo = dbManager.getAllPartitionInfo(outputTableInfo.getTableId)
    val outputTablePartitionStrList = new util.ArrayList[String]
    dayList.stream.forEach((day: String) => {
      inputTablePartitionInfo.stream
        .filter((p: PartitionInfo) => p.getPartitionDesc.contains(day))
        .forEach((partitionInfo: PartitionInfo) => inputTablePartitionStrList.add(partitionInfo.getPartitionDesc))
      outputTablePartitionInfo.stream
        .filter((p: PartitionInfo) => p.getPartitionDesc.contains(day))
        .forEach((partitionInfo: PartitionInfo) => outputTablePartitionStrList.add(partitionInfo.getPartitionDesc))
    })
    println("============= inputTablePartitionStrList size: " + inputTablePartitionStrList.size + " =============")
    inputTablePartitionStrList.removeAll(outputTablePartitionStrList)
    println("============= inputTablePartitionStrList size after remove: " + inputTablePartitionStrList.size + " =============")
    inputTablePartitionStrList.stream.forEach((info: String) => {
      val map = DBUtil.parsePartitionDesc(info)
      partitionList.add(Row(map.get("flt_dt"), map.get("tail_num"), map.get("file_no")))
    })
    println("------------ operate getPartitions! partitionList size: " + partitionList.size + " -------------")
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
