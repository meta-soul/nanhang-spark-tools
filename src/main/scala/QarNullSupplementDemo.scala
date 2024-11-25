import com.dmetasoul.lakesoul.meta.{DBManager, DBUtil}
import com.dmetasoul.lakesoul.meta.entity.{PartitionInfo, TableInfo}
import com.dmetasoul.lakesoul.spark.ParametersTool
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util

object QarNullSupplementDemo {

  private val BEGIN_TIME: String = "begin_day"
  private val END_DAY: String = "end_day"
  private val BATCH_SIZE: String = "batch_size"

  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    val beginDay = parameter.get(BEGIN_TIME)
    val endDay = parameter.get(END_DAY)
    val batchSize = parameter.getInt(BATCH_SIZE, 10)

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
    val needMergePartition = getPartitions("", "", selectTimeRange)
    spark.sql("use qara_mid")
  }


  private def dealPH5HZ(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_p5hz_fillup;")
    spark.sql("create table qara_mid.mid_qara_320020_p5hz_fillup with('path'='hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_p5hz_fillup') like czods.s_qara_320020_p5hz")
    partitionList
      .forEach(partition => {
        println("-- begin deal with tuple: " + partition.toString())
        val fltDt = partition.get(0)
        val tailNum = partition.get(1)
        val fileNo = partition.get(2)
        val sqlDemo = s"insert into  qara_mid.mid_qara_320020_p5hz_fillup  select t0.file_no,t0.flt_dt,t0.tail_num,t0.`time`,t0.time_series, COALESCE(t0.aie1_rw,t1.aie1_rw) as aie1_rw,COALESCE(t0.aie2_rw,t1.aie2_rw) as aie2_rw,COALESCE(t0.aiwl,t1.aiwl) as aiwl,COALESCE(t0.aiwr,t1.aiwr) as aiwr,COALESCE(t0.ai_faul1,t1.ai_faul1) as ai_faul1,COALESCE(t0.ai_faul2,t1.ai_faul2) as ai_faul2,COALESCE(t0.flaprw,t1.flaprw) as flaprw,COALESCE(t0.glide_dev1,t1.glide_dev1) as glide_dev1,COALESCE(t0.glide_dev2,t1.glide_dev2) as glide_dev2,COALESCE(t0.hyd_l_pres_b,t1.hyd_l_pres_b) as hyd_l_pres_b,COALESCE(t0.hyd_l_pres_g,t1.hyd_l_pres_g) as hyd_l_pres_g,COALESCE(t0.hyd_l_pres_y,t1.hyd_l_pres_y) as hyd_l_pres_y,COALESCE(t0.loc_dev1,t1.loc_dev1) as loc_dev1,COALESCE(t0.loc_dev2,t1.loc_dev2) as loc_dev2,COALESCE(t0.n1_epr_cmd1,t1.n1_epr_cmd1) as n1_epr_cmd1,COALESCE(t0.n1_epr_cmd2,t1.n1_epr_cmd2) as n1_epr_cmd2,COALESCE(t0.n1_mod_sel1,t1.n1_mod_sel1) as n1_mod_sel1,COALESCE(t0.n1_mod_sel2,t1.n1_mod_sel2) as n1_mod_sel2,COALESCE(t0.pck_1_fl_ctl,t1.pck_1_fl_ctl) as pck_1_fl_ctl,COALESCE(t0.pck_2_fl_ctl,t1.pck_2_fl_ctl) as pck_2_fl_ctl,COALESCE(t0.ralt1,t1.ralt1) as ralt1,COALESCE(t0.ralt2,t1.ralt2) as ralt2,COALESCE(t0.tat,t1.tat) as tatfrom (select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*     from  czods.s_qara_320020_p5hz      where  flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'      ) t0left join (select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*    from  czods.s_qara_320020_p5hz      where  flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'     ) t1on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add;\n"
        spark.sql(sqlDemo)
      })
  }

  private def dealP5hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_p25hz_fillup")
    spark.sql("create table if not exists qara_mid.mid_qara_320020_p25hz_fillup with('path'='hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_p25hz_fillup') like czods.s_qara_320020_p25hz")
    partitionList.forEach( partition => {
      println("begin deal with tuple: " + partition.toString)
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sqlDemo1 = s"insert into  qara_mid.mid_qara_320020_p25hz_fillup select t0.file_no,t0.flt_dt,t0.tail_num,t0.`time`,t0.time_series\n" +
        s",COALESCE(t0.acms_fp,t1.acms_fp,t2.acms_fp,t3.acms_fp) as acms_fp,\n" +
        s"COALESCE(t0.acv_b_on2,t1.acv_b_on2,t2.acv_b_on2,t3.acv_b_on2) as acv_b_on2,\n" +
        s"COALESCE(t0.ac_iden,t1.ac_iden,t2.ac_iden,t3.ac_iden) as ac_iden,\n" +
        s"COALESCE(t0.ac_tail123,t1.ac_tail123,t2.ac_tail123,t3.ac_tail123) as ac_tail123,\n" +
        s"COALESCE(t0.ac_tail4,t1.ac_tail4,t2.ac_tail4,t3.ac_tail4) as ac_tail4,\n" +
        s"COALESCE(t0.ac_tail456,t1.ac_tail456,t2.ac_tail456,t3.ac_tail456) as ac_tail456,\n" +
        s"COALESCE(t0.ac_tail7,t1.ac_tail7,t2.ac_tail7,t3.ac_tail7) as ac_tail7,\n" +
        s"COALESCE(t0.ac_typ,t1.ac_typ,t2.ac_typ,t3.ac_typ) as ac_typ,\n" +
        s"COALESCE(t0.ail_lh_avl_b,t1.ail_lh_avl_b,t2.ail_lh_avl_b,t3.ail_lh_avl_b) as ail_lh_avl_b,\n" +
        s"COALESCE(t0.ail_lh_avl_g,t1.ail_lh_avl_g,t2.ail_lh_avl_g,t3.ail_lh_avl_g) as ail_lh_avl_g,\n" +
        s"COALESCE(t0.ail_rh_avl_b,t1.ail_rh_avl_b,t2.ail_rh_avl_b,t3.ail_rh_avl_b) as ail_rh_avl_b,\n" +
        s"COALESCE(t0.ail_rh_avl_g,t1.ail_rh_avl_g,t2.ail_rh_avl_g,t3.ail_rh_avl_g) as ail_rh_avl_g,\n" +
        s"COALESCE(t0.airline_id,t1.airline_id,t2.airline_id,t3.airline_id) as airline_id,\n" +
        s"COALESCE(t0.airl_idt,t1.airl_idt,t2.airl_idt,t3.airl_idt) as airl_idt,\n" +
        s"COALESCE(t0.air_ground,t1.air_ground,t2.air_ground,t3.air_ground) as air_ground,\n" +
        s"COALESCE(t0.aiw_rw,t1.aiw_rw,t2.aiw_rw,t3.aiw_rw) as aiw_rw,\n" +
        s"COALESCE(t0.alt_cpt,t1.alt_cpt,t2.alt_cpt,t3.alt_cpt) as alt_cpt,\n" +
        s"COALESCE(t0.alt_fo,t1.alt_fo,t2.alt_fo,t3.alt_fo) as alt_fo,\n" +
        s"COALESCE(t0.alt_swc,t1.alt_swc,t2.alt_swc,t3.alt_swc) as alt_swc,\n" +
        s"COALESCE(t0.apr_ctl,t1.apr_ctl,t2.apr_ctl,t3.apr_ctl) as apr_ctl,\n" +
        s"COALESCE(t0.apubld_vv_op,t1.apubld_vv_op,t2.apubld_vv_op,t3.apubld_vv_op) as apubld_vv_op,\n" +
        s"COALESCE(t0.aspd_ctl,t1.aspd_ctl,t2.aspd_ctl,t3.aspd_ctl) as aspd_ctl,\n" +
        s"COALESCE(t0.aud_tr_fail,t1.aud_tr_fail,t2.aud_tr_fail,t3.aud_tr_fail) as aud_tr_fail,\n" +
        s"COALESCE(t0.bld_vlv1,t1.bld_vlv1,t2.bld_vlv1,t3.bld_vlv1) as bld_vlv1,\n" +
        s"COALESCE(t0.bld_vlv2,t1.bld_vlv2,t2.bld_vlv2,t3.bld_vlv2) as bld_vlv2,\n" +
        s"COALESCE(t0.bp1,t1.bp1,t2.bp1,t3.bp1) as bp1,\n" +
        s"COALESCE(t0.bp2,t1.bp2,t2.bp2,t3.bp2) as bp2,\n" +
        s"COALESCE(t0.cent_curr,t1.cent_curr,t2.cent_curr,t3.cent_curr) as cent_curr,\n" +
        s"COALESCE(t0.city_from,t1.city_from,t2.city_from,t3.city_from) as city_from,\n" +
        s"COALESCE(t0.city_from_r,t1.city_from_r,t2.city_from_r,t3.city_from_r) as city_from_r,\n" +
        s"COALESCE(t0.city_to_r,t1.city_to_r,t2.city_to_r,t3.city_to_r) as city_to_r,\n" +
        s"COALESCE(t0.ck_conf_scl_mx,t1.ck_conf_scl_mx,t2.ck_conf_scl_mx,t3.ck_conf_scl_mx) as ck_conf_scl_mx,\n" +
        s"COALESCE(t0.ck_egt_scl_max,t1.ck_egt_scl_max,t2.ck_egt_scl_max,t3.ck_egt_scl_max) as ck_egt_scl_max,\n" +
        s"COALESCE(t0.ck_epr_scl_max,t1.ck_epr_scl_max,t2.ck_epr_scl_max,t3.ck_epr_scl_max) as ck_epr_scl_max,\n" +
        s"COALESCE(t0.ck_ins_eng_mod,t1.ck_ins_eng_mod,t2.ck_ins_eng_mod,t3.ck_ins_eng_mod) as ck_ins_eng_mod,\n" +
        s"COALESCE(t0.ck_itt_scl_max,t1.ck_itt_scl_max,t2.ck_itt_scl_max,t3.ck_itt_scl_max) as ck_itt_scl_max,\n" +
        s"COALESCE(t0.ck_ldg_nr,t1.ck_ldg_nr,t2.ck_ldg_nr,t3.ck_ldg_nr) as ck_ldg_nr,\n" +
        s"COALESCE(t0.ck_n1_scl_max,t1.ck_n1_scl_max,t2.ck_n1_scl_max,t3.ck_n1_scl_max) as ck_n1_scl_max,\n" +
        s"COALESCE(t0.ck_tla_scl_max,t1.ck_tla_scl_max,t2.ck_tla_scl_max,t3.ck_tla_scl_max) as ck_tla_scl_max,\n" +
        s"COALESCE(t0.ck_tla_scl_min,t1.ck_tla_scl_min,t2.ck_tla_scl_min,t3.ck_tla_scl_min) as ck_tla_scl_min,\n" +
        s"COALESCE(t0.ck_torq_scl_mx,t1.ck_torq_scl_mx,t2.ck_torq_scl_mx,t3.ck_torq_scl_mx) as ck_torq_scl_mx,\n" +
        s"COALESCE(t0.cmc_tr_fail,t1.cmc_tr_fail,t2.cmc_tr_fail,t3.cmc_tr_fail) as cmc_tr_fail,\n" +
        s"COALESCE(t0.dat_day,t1.dat_day,t2.dat_day,t3.dat_day) as dat_day,\n" +
        s"COALESCE(t0.dat_month,t1.dat_month,t2.dat_month,t3.dat_month) as dat_month,\n" +
        s"COALESCE(t0.day_curr,t1.day_curr,t2.day_curr,t3.day_curr) as day_curr,\n" +
        s"COALESCE(t0.db_upd_cyc,t1.db_upd_cyc,t2.db_upd_cyc,t3.db_upd_cyc) as db_upd_cyc,\n" +
        s"COALESCE(t0.db_upd_date,t1.db_upd_date,t2.db_upd_date,t3.db_upd_date) as db_upd_date,\n" +
        s"COALESCE(t0.dc1_bus_on,t1.dc1_bus_on,t2.dc1_bus_on,t3.dc1_bus_on) as dc1_bus_on,\n" +
        s"COALESCE(t0.dc2_bus_on,t1.dc2_bus_on,t2.dc2_bus_on,t3.dc2_bus_on) as dc2_bus_on,\n" +
        s"COALESCE(t0.dec_height,t1.dec_height,t2.dec_height,t3.dec_height) as dec_height,\n" +
        s"COALESCE(t0.dfc,t1.dfc,t2.dfc,t3.dfc) as dfc,\n" +
        s"COALESCE(t0.dits_fail,t1.dits_fail,t2.dits_fail,t3.dits_fail) as dits_fail,\n" +
        s"COALESCE(t0.dmc_1_invald,t1.dmc_1_invald,t2.dmc_1_invald,t3.dmc_1_invald) as dmc_1_invald,\n" +
        s"COALESCE(t0.dmc_2_invald,t1.dmc_2_invald,t2.dmc_2_invald,t3.dmc_2_invald) as dmc_2_invald,\n" +
        s"COALESCE(t0.dmc_3_invald,t1.dmc_3_invald,t2.dmc_3_invald,t3.dmc_3_invald) as dmc_3_invald,\n" +
        s"COALESCE(t0.dmc_3_xfr_ca,t1.dmc_3_xfr_ca,t2.dmc_3_xfr_ca,t3.dmc_3_xfr_ca) as dmc_3_xfr_ca,\n" +
        s"COALESCE(t0.dmc_3_xfr_fo,t1.dmc_3_xfr_fo,t2.dmc_3_xfr_fo,t3.dmc_3_xfr_fo) as dmc_3_xfr_fo,\n" +
        s"COALESCE(t0.dmc_mark_id,t1.dmc_mark_id,t2.dmc_mark_id,t3.dmc_mark_id) as dmc_mark_id,\n" +
        s"COALESCE(t0.dme_dis1,t1.dme_dis1,t2.dme_dis1,t3.dme_dis1) as dme_dis1,\n" +
        s"COALESCE(t0.dme_dis2,t1.dme_dis2,t2.dme_dis2,t3.dme_dis2) as dme_dis2,\n" +
        s"COALESCE(t0.dme_frq1,t1.dme_frq1,t2.dme_frq1,t3.dme_frq1) as dme_frq1,\n" +
        s"COALESCE(t0.dme_frq2,t1.dme_frq2,t2.dme_frq2,t3.dme_frq2) as dme_frq2,\n" +
        s"COALESCE(t0.drift,t1.drift,t2.drift,t3.drift) as drift,\n" +
        s"COALESCE(t0.ecamdu_1_off,t1.ecamdu_1_off,t2.ecamdu_1_off,t3.ecamdu_1_off) as ecamdu_1_off,\n" +
        s"COALESCE(t0.ecamdu_2_off,t1.ecamdu_2_off,t2.ecamdu_2_off,t3.ecamdu_2_off) as ecamdu_2_off,\n" +
        s"COALESCE(t0.ecamnd_xfrca,t1.ecamnd_xfrca,t2.ecamnd_xfrca,t3.ecamnd_xfrca) as ecamnd_xfrca,\n" +
        s"COALESCE(t0.ecamnd_xfrfo,t1.ecamnd_xfrfo,t2.ecamnd_xfrfo,t3.ecamnd_xfrfo) as ecamnd_xfrfo,\n" +
        s"COALESCE(t0.ecam_sel_mat,t1.ecam_sel_mat,t2.ecam_sel_mat,t3.ecam_sel_mat) as ecam_sel_mat,\n" +
        s"COALESCE(t0.ecu_eec_1_cb,t1.ecu_eec_1_cb,t2.ecu_eec_1_cb,t3.ecu_eec_1_cb) as ecu_eec_1_cb,\n" +
        s"COALESCE(t0.ecu_eec_2_cb,t1.ecu_eec_2_cb,t2.ecu_eec_2_cb,t3.ecu_eec_2_cb) as ecu_eec_2_cb,\n" +
        s"COALESCE(t0.egp1,t1.egp1,t2.egp1,t3.egp1) as egp1,\n" +
        s"COALESCE(t0.egp2,t1.egp2,t2.egp2,t3.egp2) as egp2,\n" +
        s"COALESCE(t0.elac1_pt_flt,t1.elac1_pt_flt,t2.elac1_pt_flt,t3.elac1_pt_flt) as elac1_pt_flt,\n" +
        s"COALESCE(t0.elac1_ro_flt,t1.elac1_ro_flt,t2.elac1_ro_flt,t3.elac1_ro_flt) as elac1_ro_flt,\n" +
        s"COALESCE(t0.elac2_pt_flt,t1.elac2_pt_flt,t2.elac2_pt_flt,t3.elac2_pt_flt) as elac2_pt_flt,\n" +
        s"COALESCE(t0.elac2_rl_flt,t1.elac2_rl_flt,t2.elac2_rl_flt,t3.elac2_rl_flt) as elac2_rl_flt,\n" +
        s"COALESCE(t0.elac_1_fault,t1.elac_1_fault,t2.elac_1_fault,t3.elac_1_fault) as elac_1_fault,\n" +
        s"COALESCE(t0.elac_2_fault,t1.elac_2_fault,t2.elac_2_fault,t3.elac_2_fault) as elac_2_fault,\n" +
        s"COALESCE(t0.eng1_pt25,t1.eng1_pt25,t2.eng1_pt25,t3.eng1_pt25) as eng1_pt25,\n" +
        s"COALESCE(t0.eng2_pt25,t1.eng2_pt25,t2.eng2_pt25,t3.eng2_pt25) as eng2_pt25,\n" +
        s"COALESCE(t0.epr_max,t1.epr_max,t2.epr_max,t3.epr_max) as epr_max,\n" +
        s"COALESCE(t0.ext_tmp_eng1,t1.ext_tmp_eng1,t2.ext_tmp_eng1,t3.ext_tmp_eng1) as ext_tmp_eng1,\n" +
        s"COALESCE(t0.ext_tmp_eng2,t1.ext_tmp_eng2,t2.ext_tmp_eng2,t3.ext_tmp_eng2) as ext_tmp_eng2,\n" +
        s"COALESCE(t0.fdiu_fail,t1.fdiu_fail,t2.fdiu_fail,t3.fdiu_fail) as fdiu_fail,\n" +
        s"COALESCE(t0.fdiu_prg_id,t1.fdiu_prg_id,t2.fdiu_prg_id,t3.fdiu_prg_id) as fdiu_prg_id,\n" +
        s"COALESCE(t0.fdr_fail,t1.fdr_fail,t2.fdr_fail,t3.fdr_fail) as fdr_fail,\n" +
        s"COALESCE(t0.fdr_pb_fail,t1.fdr_pb_fail,t2.fdr_pb_fail,t3.fdr_pb_fail) as fdr_pb_fail,\n" +
        s"COALESCE(t0.flap_faul,t1.flap_faul,t2.flap_faul,t3.flap_faul) as flap_faul,\n" +
        s"COALESCE(t0.flap_lever_c,t1.flap_lever_c,t2.flap_lever_c,t3.flap_lever_c) as flap_lever_c,\n" +
        s"COALESCE(t0.flare,t1.flare,t2.flare,t3.flare) as flare,\n" +
        s"COALESCE(t0.fleet_idt,t1.fleet_idt,t2.fleet_idt,t3.fleet_idt) as fleet_idt,\n" +
        s"COALESCE(t0.flight_phas0,t1.flight_phas0,t2.flight_phas0,t3.flight_phas0) as flight_phas0,\n" +
        s"COALESCE(t0.fltnum,t1.fltnum,t2.fltnum,t3.fltnum) as fltnum,\n" +
        s"COALESCE(t0.fwc_valid,t1.fwc_valid,t2.fwc_valid,t3.fwc_valid) as fwc_valid,\n" +
        s"COALESCE(t0.gw,t1.gw,t2.gw,t3.gw) as gw,\n" +
        s"COALESCE(t0.head_selon,t1.head_selon,t2.head_selon,t3.head_selon) as head_selon,\n" +
        s"COALESCE(t0.hpv_nfc_1,t1.hpv_nfc_1,t2.hpv_nfc_1,t3.hpv_nfc_1) as hpv_nfc_1,\n" +
        s"COALESCE(t0.hpv_nfc_2,t1.hpv_nfc_2,t2.hpv_nfc_2,t3.hpv_nfc_2) as hpv_nfc_2,\n" +
        s"COALESCE(t0.ident_eng1,t1.ident_eng1,t2.ident_eng1,t3.ident_eng1) as ident_eng1,\n" +
        s"COALESCE(t0.ident_eng2,t1.ident_eng2,t2.ident_eng2,t3.ident_eng2) as ident_eng2,\n" +
        s"COALESCE(t0.ils_frq1,t1.ils_frq1,t2.ils_frq1,t3.ils_frq1) as ils_frq1,\n" +
        s"COALESCE(t0.ils_frq2,t1.ils_frq2,t2.ils_frq2,t3.ils_frq2) as ils_frq2,\n" +
        s"COALESCE(t0.isov1,t1.isov1,t2.isov1,t3.isov1) as isov1,\n" +
        s"COALESCE(t0.isov2,t1.isov2,t2.isov2,t3.isov2) as isov2,\n" +
        s"COALESCE(t0.ivv_c,t1.ivv_c,t2.ivv_c,t3.ivv_c) as ivv_c,\n" +
        s"COALESCE(t0.landing_roll,t1.landing_roll,t2.landing_roll,t3.landing_roll) as landing_roll,\n" +
        s"COALESCE(t0.latp,t1.latp,t2.latp,t3.latp) as latp,\n" +
        s"COALESCE(t0.lat_ck_fail,t1.lat_ck_fail,t2.lat_ck_fail,t3.lat_ck_fail) as lat_ck_fail,\n" +
        s"COALESCE(t0.lat_ck_n_r,t1.lat_ck_n_r,t2.lat_ck_n_r,t3.lat_ck_n_r) as lat_ck_n_r,\n" +
        s"COALESCE(t0.ldg_seldw,t1.ldg_seldw,t2.ldg_seldw,t3.ldg_seldw) as ldg_seldw,\n" +
        s"COALESCE(t0.ldg_selup,t1.ldg_selup,t2.ldg_selup,t3.ldg_selup) as ldg_selup,\n" +
        s"COALESCE(t0.ldg_sel_up,t1.ldg_sel_up,t2.ldg_sel_up,t3.ldg_sel_up) as ldg_sel_up,\n" +
        s"COALESCE(t0.long_ck_fail,t1.long_ck_fail,t2.long_ck_fail,t3.long_ck_fail) as long_ck_fail,\n" +
        s"COALESCE(t0.long_ck_n_r,t1.long_ck_n_r,t2.long_ck_n_r,t3.long_ck_n_r) as long_ck_n_r,\n" +
        s"COALESCE(t0.lonp,t1.lonp,t2.lonp,t3.lonp) as lonp,\n" +
        s"COALESCE(t0.mach_selon,t1.mach_selon,t2.mach_selon,t3.mach_selon) as mach_selon,\n" +
        s"COALESCE(t0.mls_select,t1.mls_select,t2.mls_select,t3.mls_select) as mls_select,\n" +
        s"COALESCE(t0.mode_mmr1,t1.mode_mmr1,t2.mode_mmr1,t3.mode_mmr1) as mode_mmr1,\n" +
        s"COALESCE(t0.mode_mmr2,t1.mode_mmr2,t2.mode_mmr2,t3.mode_mmr2) as mode_mmr2,\n" +
        s"COALESCE(t0.mode_nd_capt,t1.mode_nd_capt,t2.mode_nd_capt,t3.mode_nd_capt) as mode_nd_capt,\n" +
        s"COALESCE(t0.mode_nd_fo,t1.mode_nd_fo,t2.mode_nd_fo,t3.mode_nd_fo) as mode_nd_fo,\n" +
        s"COALESCE(t0.month_curr,t1.month_curr,t2.month_curr,t3.month_curr) as month_curr,\n" +
        s"COALESCE(t0.n1_epr_tgt1,t1.n1_epr_tgt1,t2.n1_epr_tgt1,t3.n1_epr_tgt1) as n1_epr_tgt1,\n" +
        s"COALESCE(t0.nd_ca_an_off,t1.nd_ca_an_off,t2.nd_ca_an_off,t3.nd_ca_an_off) as nd_ca_an_off,\n" +
        s"COALESCE(t0.nd_fo_an_off,t1.nd_fo_an_off,t2.nd_fo_an_off,t3.nd_fo_an_off) as nd_fo_an_off,\n" +
        s"COALESCE(t0.nm_range_ca,t1.nm_range_ca,t2.nm_range_ca,t3.nm_range_ca) as nm_range_ca,\n" +
        s"COALESCE(t0.nm_range_fo,t1.nm_range_fo,t2.nm_range_fo,t3.nm_range_fo) as nm_range_fo,\n" +
        s"COALESCE(t0.norm_ck_fail,t1.norm_ck_fail,t2.norm_ck_fail,t3.norm_ck_fail) as norm_ck_fail,\n" +
        s"COALESCE(t0.norm_ck_n_r,t1.norm_ck_n_r,t2.norm_ck_n_r,t3.norm_ck_n_r) as norm_ck_n_r,\n" +
        s"COALESCE(t0.no_data_eec1,t1.no_data_eec1,t2.no_data_eec1,t3.no_data_eec1) as no_data_eec1,\n" +
        s"COALESCE(t0.no_data_eec2,t1.no_data_eec2,t2.no_data_eec2,t3.no_data_eec2) as no_data_eec2,\n" +
        s"COALESCE(t0.oil_prs1,t1.oil_prs1,t2.oil_prs1,t3.oil_prs1) as oil_prs1,\n" +
        s"COALESCE(t0.oil_prs2,t1.oil_prs2,t2.oil_prs2,t3.oil_prs2) as oil_prs2,\n" +
        s"COALESCE(t0.oil_tmp1,t1.oil_tmp1,t2.oil_tmp1,t3.oil_tmp1) as oil_tmp1,\n" +
        s"COALESCE(t0.oil_tmp2,t1.oil_tmp2,t2.oil_tmp2,t3.oil_tmp2) as oil_tmp2,\n" +
        s"COALESCE(t0.oiq1,t1.oiq1,t2.oiq1,t3.oiq1) as oiq1,\n" +
        s"COALESCE(t0.oiq2,t1.oiq2,t2.oiq2,t3.oiq2) as oiq2,\n" +
        s"COALESCE(t0.opv_nfo_1,t1.opv_nfo_1,t2.opv_nfo_1,t3.opv_nfo_1) as opv_nfo_1,\n" +
        s"COALESCE(t0.opv_nfo_2,t1.opv_nfo_2,t2.opv_nfo_2,t3.opv_nfo_2) as opv_nfo_2,\n" +
        s"COALESCE(t0.pfdnd_xfr_ca,t1.pfdnd_xfr_ca,t2.pfdnd_xfr_ca,t3.pfdnd_xfr_ca) as pfdnd_xfr_ca,\n" +
        s"COALESCE(t0.pfdnd_xfr_fo,t1.pfdnd_xfr_fo,t2.pfdnd_xfr_fo,t3.pfdnd_xfr_fo) as pfdnd_xfr_fo,\n" +
        s"COALESCE(t0.pfd_ca_anoff,t1.pfd_ca_anoff,t2.pfd_ca_anoff,t3.pfd_ca_anoff) as pfd_ca_anoff,\n" +
        s"COALESCE(t0.pfd_fo_anoff,t1.pfd_fo_anoff,t2.pfd_fo_anoff,t3.pfd_fo_anoff) as pfd_fo_anoff,\n" +
        s"COALESCE(t0.pitch_altr1,t1.pitch_altr1,t2.pitch_altr1,t3.pitch_altr1) as pitch_altr1,\n" +
        s"COALESCE(t0.pitch_altr2,t1.pitch_altr2,t2.pitch_altr2,t3.pitch_altr2) as pitch_altr2,\n" +
        s"COALESCE(t0.pitch_dir_lw,t1.pitch_dir_lw,t2.pitch_dir_lw,t3.pitch_dir_lw) as pitch_dir_lw,\n" +
        s"COALESCE(t0.pitch_nor,t1.pitch_nor,t2.pitch_nor,t3.pitch_nor) as pitch_nor,\n" +
        s"COALESCE(t0.playbk_fail,t1.playbk_fail,t2.playbk_fail,t3.playbk_fail) as playbk_fail,\n" +
        s"COALESCE(t0.playbk_inact,t1.playbk_inact,t2.playbk_inact,t3.playbk_inact) as playbk_inact,\n" +
        s"COALESCE(t0.prv_nfc_1,t1.prv_nfc_1,t2.prv_nfc_1,t3.prv_nfc_1) as prv_nfc_1,\n" +
        s"COALESCE(t0.prv_nfc_2,t1.prv_nfc_2,t2.prv_nfc_2,t3.prv_nfc_2) as prv_nfc_2,\n" +
        s"COALESCE(t0.qar_fail,t1.qar_fail,t2.qar_fail,t3.qar_fail) as qar_fail,\n" +
        s"COALESCE(t0.qar_tapelow,t1.qar_tapelow,t2.qar_tapelow,t3.qar_tapelow) as qar_tapelow,\n" +
        s"COALESCE(t0.sdac1_valid,t1.sdac1_valid,t2.sdac1_valid,t3.sdac1_valid) as sdac1_valid,\n" +
        s"COALESCE(t0.sdac2_valid,t1.sdac2_valid,t2.sdac2_valid,t3.sdac2_valid) as sdac2_valid,\n" +
        s"COALESCE(t0.sec_1_fault,t1.sec_1_fault,t2.sec_1_fault,t3.sec_1_fault) as sec_1_fault,\n" +
        s"COALESCE(t0.sec_2_fault,t1.sec_2_fault,t2.sec_2_fault,t3.sec_2_fault) as sec_2_fault,\n" +
        s"COALESCE(t0.sec_3_fault,t1.sec_3_fault,t2.sec_3_fault,t3.sec_3_fault) as sec_3_fault,\n" +
        s"COALESCE(t0.slat_faul,t1.slat_faul,t2.slat_faul,t3.slat_faul) as slat_faul,\n" +
        s"COALESCE(t0.spd_brk,t1.spd_brk,t2.spd_brk,t3.spd_brk) as spd_brk,\n" +
        s"COALESCE(t0.stab_bld_pos1,t1.stab_bld_pos1,t2.stab_bld_pos1,t3.stab_bld_pos1) as stab_bld_pos1,\n" +
        s"COALESCE(t0.stab_bld_pos2,t1.stab_bld_pos2,t2.stab_bld_pos2,t3.stab_bld_pos2) as stab_bld_pos2,\n" +
        s"COALESCE(t0.start_vlv1,t1.start_vlv1,t2.start_vlv1,t3.start_vlv1) as start_vlv1,\n" +
        s"COALESCE(t0.start_vlv2,t1.start_vlv2,t2.start_vlv2,t3.start_vlv2) as start_vlv2,\n" +
        s"COALESCE(t0.sta_van_eng1,t1.sta_van_eng1,t2.sta_van_eng1,t3.sta_van_eng1) as sta_van_eng1,\n" +
        s"COALESCE(t0.sta_van_eng2,t1.sta_van_eng2,t2.sta_van_eng2,t3.sta_van_eng2) as sta_van_eng2,\n" +
        s"COALESCE(t0.tcas_sens_1,t1.tcas_sens_1,t2.tcas_sens_1,t3.tcas_sens_1) as tcas_sens_1,\n" +
        s"COALESCE(t0.tcas_sens_2,t1.tcas_sens_2,t2.tcas_sens_2,t3.tcas_sens_2) as tcas_sens_2,\n" +
        s"COALESCE(t0.tcas_sens_3,t1.tcas_sens_3,t2.tcas_sens_3,t3.tcas_sens_3) as tcas_sens_3,\n" +
        s"COALESCE(t0.tla1_c,t1.tla1_c,t2.tla1_c,t3.tla1_c) as tla1_c,\n" +
        s"COALESCE(t0.tla2_c,t1.tla2_c,t2.tla2_c,t3.tla2_c) as tla2_c,\n" +
        s"COALESCE(t0.touch_and_go,t1.touch_and_go,t2.touch_and_go,t3.touch_and_go) as touch_and_go,\n" +
        s"COALESCE(t0.trb1_cool,t1.trb1_cool,t2.trb1_cool,t3.trb1_cool) as trb1_cool,\n" +
        s"COALESCE(t0.trb2_cool,t1.trb2_cool,t2.trb2_cool,t3.trb2_cool) as trb2_cool,\n" +
        s"COALESCE(t0.tt25_l,t1.tt25_l,t2.tt25_l,t3.tt25_l) as tt25_l,\n" +
        s"COALESCE(t0.tt25_r,t1.tt25_r,t2.tt25_r,t3.tt25_r) as tt25_r,\n" +
        s"COALESCE(t0.t_inl_prs1,t1.t_inl_prs1,t2.t_inl_prs1,t3.t_inl_prs1) as t_inl_prs1,\n" +
        s"COALESCE(t0.t_inl_prs2,t1.t_inl_prs2,t2.t_inl_prs2,t3.t_inl_prs2) as t_inl_prs2,\n" +
        s"COALESCE(t0.t_inl_tmp1,t1.t_inl_tmp1,t2.t_inl_tmp1,t3.t_inl_tmp1) as t_inl_tmp1,\n" +
        s"COALESCE(t0.t_inl_tmp2,t1.t_inl_tmp2,t2.t_inl_tmp2,t3.t_inl_tmp2) as t_inl_tmp2,\n" +
        s"COALESCE(t0.utc_hour,t1.utc_hour,t2.utc_hour,t3.utc_hour) as utc_hour,\n" +
        s"COALESCE(t0.utc_min_sec,t1.utc_min_sec,t2.utc_min_sec,t3.utc_min_sec) as utc_min_sec,\n" +
        s"COALESCE(t0.vor_frq1,t1.vor_frq1,t2.vor_frq1,t3.vor_frq1) as vor_frq1,\n" +
        s"COALESCE(t0.vor_frq2,t1.vor_frq2,t2.vor_frq2,t3.vor_frq2) as vor_frq2,\n" +
        s"COALESCE(t0.win_spdr,t1.win_spdr,t2.win_spdr,t3.win_spdr) as win_spdr,\n" +
        s"COALESCE(t0.year_curr,t1.year_curr,t2.year_curr,t3.year_curr) as year_curr,\n" +
        s"COALESCE(t0.year_med,t1.year_med,t2.year_med,t3.year_med) as year_med\n" +
        s"from \n" +
        s"(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_p25hz  \n" +
        s"    where  flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'   ) t0\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_p25hz  \n" +
        s"    where  flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'  ) t1\n" +
        s"on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_p25hz  \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'  ) t2\n" +
        s"on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add\n" +
        s"    left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_p25hz \n" +
        s"    where  flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'  ) t3\n" +
        s"on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add;\n"
      spark.sql(sqlDemo1)
    })
  }

  private def deal1hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_1hz_fillup;")
    spark.sql("create table if not exists qara_mid.mid_qara_320020_1hz_fillup with('path'='hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_1hz_fillup') like czods.s_qara_320020_1hz;")
    partitionList.forEach(partition => {
      System.out.println("begin deal with tuple: " + partition.toString());
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sqlDemo = s"insert into  qara_mid.mid_qara_320020_1hz_fillup select file_no,flt_dt,tail_num,`time`,1,aapp,aapp_td,ac_tail1,ac_tail2,ac_tail3,ac_type,ac_type_csn,aie1,aie2,aill,ailr,air_durn,aiw,alpha_floor,alt_const,alt_qnh,alt_qnh_csn,alt_qnh_ld,alt_qnh_to,alt_sel,alt_sel_csn,alt_std,alt_stdc,alt_stdc_csn,aoal,aoar,apu_on,apu_on_csn,ap_egd1,ap_egd1_csn,ap_egd2,ap_egd2_csn,ap_off,ap_off_csn,arp_dist_dest,arp_dist_dest_csn,arp_dist_orig,arp_heigh_dest,arp_heigh_orig,ats_active,ats_egd,ats_egdoract_csn,ats_egd_csn,ats_ret_mod,ats_spd_mach,ats_thrustn1,auto_brk_flt,auto_brk_off,auto_land_wn,auto_v2,b3d_day,b3d_hour,b3d_min,b3d_month,badsf_nr,baromb,bea_mk,brk_pdll,brk_ped_rh,cab_prs_war,city_from_r_csn,city_to,city_to_r_csn,ck_alt_sel,ck_alt_std,ck_c1_r1_col,ck_c1_r1_txt,ck_c1_r3_col,ck_c1_r3_txt,ck_c2_r1_col,ck_c2_r1_txt,ck_c2_r2_col,ck_c2_r2_txt,ck_c2_r3_col,ck_c2_r3_txt,ck_c3_r1_col,ck_c3_r1_txt,ck_c3_r2_col,ck_c3_r2_txt,ck_c4_r1_col,ck_c4_r1_txt,ck_c4_r2_col,ck_c4_r2_txt,ck_c4_r3_col,ck_c4_r3_txt,ck_c5_r1_col,ck_c5_r1_txt,ck_c5_r2_col,ck_c5_r2_txt,ck_c5_r3_col,ck_c5_r3_txt,ck_conf_pos,ck_conf_pos_pc,ck_egt1,ck_egt2,ck_epr1,ck_epr2,ck_flap_spd,ck_fqty,ck_gpws_mode,ck_gw,ck_head,ck_head_mag,ck_head_sel,ck_head_selon,ck_ias,ck_ldg_seldw,ck_ldg_wow,ck_mach,ck_mas_war,ck_n11,ck_n12,ck_n21,ck_n22,ck_ralt,ck_rev_dep1,ck_rev_dep2,ck_spd_brk,ck_tcas_ra,ck_tcas_ta,ck_tla_pct1,ck_tla_pct2,ck_v1,ck_v2,ck_vapp,ck_vhf1_emit,ck_vls,ck_vmax,ck_vr,conf,conf_csn,conf_ld,conf_to,cut,`date`,date_r,date_r_csn,date_to,dat_year,day_ld,day_to,debug,destination,dev_mag,dfc_csn,dfc_pos_cfa,dist,dist0,dist_lat_td,dist_ldg,dist_ldg_csn,dist_ldg_from_gs,dist_pthr,dist_to,drift_csn,dual_input,durn_200_ld,durn_txo,egt1,egt1c,egt1c_csn,egt1_csn,egt2,egt2c,egt2c_csn,egt2_csn,egt_max,elevl,elevr,eng1_severit,eng2_severit,eng_man,epr1,epr1c,epr1_csn,epr2,epr2c,epr2_csn,epr_cmd_1,epr_cmd_2,epr_tgt1,evt_mk,fburn,fburn1,fburn1_txo,fburn2,fburn2_txo,fburn3_txo,fburn4_txo,fburn_avg,fburn_ti,fburn_txo,fburn_txo_avg,fd_1,fd_2,ff1,ff1c,ff1_csn,ff2,ff2c,ff2_csn,fire1,fire2,fire_apu,flap,flapc,flapc_csn,flap_spd,flight_chang,flight_ident,flight_input,flight_no1,flight_no1_csn,flight_no2,flight_no2_csn,flight_phase,flight_phase_csn,flight_recogn,flight_type,flt_path_angl,flt_path_angl_csn,fma_lat,fma_long,fpa_selected,fqty_ld,fqty_to,fr_count,fuel_fire_1,fuel_fire_2,gamax1000,gamax150,gamax500,gamin1000,gamin150,gamin500,glide_devc,glide_devc_csn,glide_dev_max,glide_dev_min,glide_gap,glide_ils,gpws_ctype,gpws_dont_sink,gpws_flap_low,gpws_gear_low,gpws_glide,gpws_mode,gpws_mode_csn,gpws_pull_up,gpws_sink_rate,gpws_tr_low,gpws_tr_up,gpws_war,gpws_wsh_war,gs,gsc,gsc1p,gsc_csn,gs_csn,gwc,gw_csn,gw_landing_lim,gw_ld,gw_takeoff_lim,gw_taxi_lim,gw_to,head,head_csn,head_lin,head_lin_csn,head_mag,head_sel,head_true,head_true_csn,height,height_csn,heig_1conf_chg,heig_dev_1000,heig_dev_500,heig_gear_dw,heig_lconf_chg,hf,hf_cfa,hp_fuel_vv_1,hp_fuel_vv_2,ias,iasc,iasc_csn,ias_app_1000,ias_app_50,ias_app_500,ias_bef_ld,ias_cl_1000,ias_cl_500,ias_csn,ias_ext_conf1,ias_ext_conf2,ias_ext_conf3,ias_ext_conf4,ias_ext_conf5,ias_ld,ias_max,ias_maxcnf1ld,ias_maxcnf2ld,ias_maxcnf3ld,ias_maxcnf4ld,ias_maxcnf5ld,ias_maxcnf6ld,ias_maxcnf7ld,ias_maxcnf8ld,ias_maxconf1to,ias_maxconf2to,ias_maxconf3to,ias_maxconf4to,ias_maxconf5to,ias_max_geardw,ias_min_finapp,ias_to,ias_to_50,ils_lim,ils_val,in_flight,ivv,ivv_csn,ivv_max_bl2000,ivv_min_cl35,ivv_min_cl400,ivv_sel,latpc,latpc_csn,lat_air,ldgl,ldgl_csn,ldgnos,ldgnos_csn,ldgr,ldgr_csn,ldg_seldw_csn,ldg_unlokdw,lift_gnd,loc_devc,loc_devc_csn,loc_gap,long_air,lonpc,lonpc_csn,low_press_1,low_press_2,lo_armed,mach,mach_buff,mach_buff_csn,mach_csn,mach_max,mas_cau,mas_cau_csn,mas_war,max_armed,med_armed,min_alt_n1_40,min_n1_40,mmo,n11c,n11_csn,n12c,n12_csn,n1_cmd1,n1_cmd2,n1_epr1,n1_epr2,n1_max_txo3min,n1_min_bl500,n1_ths_tgt,n21,n21c,n21_csn,n22,n22c,n22_csn,oil_qty1_csn,oil_qty2_csn,one_eng_app,origin,par_qual,pass_no,pf,pilot_ld,pilot_to,pitch_altr,ralt1c,ralt2c,raltc,raltc_csn,rate,refused_trans,relief,rev_deployd1,rev_deployd2,rev_unlock_1,rev_unlock_2,roll_cpt,roll_fo,runway_ld,runway_ld_csn,runway_to,runway_to_csn,sat,satr_csn,sf_count,single_txo,slat,slatc,slatc_csn,slatrw,slat_spd,smk_avi_war,smk_crg_war,smk_lvy_war,spd_mach_a_m,spoil_l2,spoil_l2c,spoil_l4,spoil_l4c,spoil_l5,spoil_l5c,spoil_r2,spoil_r2c,spoil_r3,spoil_r3c,spoil_r5,spoil_r5c,spoil_val1,spoil_val2,spoil_val3,spoil_val4,spoil_val5,sstck_cpt_ino,sstck_fo_ino,stab,stab_ld,stall_war,start_analysis,start_flight,surf_war,tail_wind,tas,tas_csn,tat_csn,tat_to,tcas_cmb_ctl,tcas_crw_sel,tcas_dwn_adv,tcas_rac,tcas_ra_1,tcas_ra_10,tcas_ra_11,tcas_ra_12,tcas_ra_2,tcas_ra_3,tcas_ra_4,tcas_ra_5,tcas_ra_6,tcas_ra_7,tcas_ra_8,tcas_ra_9,tcas_ta,tcas_ta_1,tcas_ta_2,tcas_up_adv,tcas_vrt_ctl,thrust_epr,thr_deficit,time_1000,time_csn,time_eng_start,time_eng_stop,time_ld,time_r,time_r_csn,time_to,tla1,tla1c,tla1_csn,tla2,tla2c,tla2_csn,touch_gnd,track_select,trigger_code,ttdwn,v1,v1_csn,v2,v2_csn,v2_min,v2_to,vapp,vapp_ld,vev,vfe,vgdot,vhf,vhf_cfa,vhf_keying_c_csn,vhf_keying_l_csn,vhf_keying_r_csn,vib_n11_csn,vib_n12_csn,vib_n1fnt1,vib_n1fnt2,vib_n21_csn,vib_n22_csn,vib_n2fnt1,vib_n2fnt2,vls,vls_csn,vmax,vmo,vmo_mmo_ovs,vr,vref,vref_csn,vr_csn,vs1g,wea_alt,wea_dewpoint,wea_temp,wea_valid,wea_visib,wea_windir,wea_winspd,win_dir,win_dir_csn,win_spd,win_spd_csn,x_air,y_airfrom  czods.s_qara_320020_1hz where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo' and time_series=0"
      spark.sql(sqlDemo);
    })

  }

  private def deal2hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_2hz_fillup_temp;")
    spark.sql("create table if not exists qara_mid.mid_qara_320020_2hz_fillup_temp USING lakesoul PARTITIONED BY (flt_dt, tail_num, file_no) LOCATION 'path'='hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_2hz_fillup_temp') AS SELECT * FROM czods.s_qara_320020_2hz;")
    spark.sql("drop table if exists qara_mid.mid_qara_320020_2hz_fillup;")
    spark.sql("create table if not exists qara_mid.mid_qara_320020_2hz_fillup with('path'='hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_2hz_fillup') like czods.s_qara_320020_2hz;")
    partitionList.forEach(partition => {

        System.out.println("begin deal with tuple: " + partition.toString());
        val fltDt = partition.get(0)
        val tailNum = partition.get(1)
        val fileNo = partition.get(2)
        val sqlDemo = s"insert into  qara_mid.mid_qara_320020_2hz_fillup_temp\n" +
          s"select t0.file_no,t0.flt_dt,t0.tail_num,t0.`time`,t0.time_series,COALESCE(t0.ck_pitch_cpt,t1.ck_pitch_cpt,t2.ck_pitch_cpt,t3.ck_pitch_cpt,t4.ck_pitch_cpt,t5.ck_pitch_cpt,t6.ck_pitch_cpt,t7.ck_pitch_cpt) as ck_pitch_cpt,COALESCE(t0.ck_pitch_fo,t1.ck_pitch_fo,t2.ck_pitch_fo,t3.ck_pitch_fo,t4.ck_pitch_fo,t5.ck_pitch_fo,t6.ck_pitch_fo,t7.ck_pitch_fo) as ck_pitch_fo,COALESCE(t0.ck_roll_cpt,t1.ck_roll_cpt,t2.ck_roll_cpt,t3.ck_roll_cpt,t4.ck_roll_cpt,t5.ck_roll_cpt,t6.ck_roll_cpt,t7.ck_roll_cpt) as ck_roll_cpt,COALESCE(t0.ck_roll_fo,t1.ck_roll_fo,t2.ck_roll_fo,t3.ck_roll_fo,t4.ck_roll_fo,t5.ck_roll_fo,t6.ck_roll_fo,t7.ck_roll_fo) as ck_roll_fo,COALESCE(t0.ck_rudd,t1.ck_rudd,t2.ck_rudd,t3.ck_rudd,t4.ck_rudd,t5.ck_rudd,t6.ck_rudd,t7.ck_rudd) as ck_rudd,COALESCE(t0.pitch_cpt,t1.pitch_cpt,t2.pitch_cpt,t3.pitch_cpt,t4.pitch_cpt,t5.pitch_cpt,t6.pitch_cpt,t7.pitch_cpt) as pitch_cpt,COALESCE(t0.pitch_fo,t1.pitch_fo,t2.pitch_fo,t3.pitch_fo,t4.pitch_fo,t5.pitch_fo,t6.pitch_fo,t7.pitch_fo) as pitch_fo,COALESCE(t0.roll,t1.roll,t2.roll,t3.roll,t4.roll,t5.roll,t6.roll,t7.roll) as roll,COALESCE(t0.roll_abs,t1.roll_abs,t2.roll_abs,t3.roll_abs,t4.roll_abs,t5.roll_abs,t6.roll_abs,t7.roll_abs) as roll_abs,COALESCE(t0.roll_csn,t1.roll_csn,t2.roll_csn,t3.roll_csn,t4.roll_csn,t5.roll_csn,t6.roll_csn,t7.roll_csn) as roll_csn,COALESCE(t0.roll_max_ab500,t1.roll_max_ab500,t2.roll_max_ab500,t3.roll_max_ab500,t4.roll_max_ab500,t5.roll_max_ab500,t6.roll_max_ab500,t7.roll_max_ab500) as roll_max_ab500,COALESCE(t0.roll_max_bl100,t1.roll_max_bl100,t2.roll_max_bl100,t3.roll_max_bl100,t4.roll_max_bl100,t5.roll_max_bl100,t6.roll_max_bl100,t7.roll_max_bl100) as roll_max_bl100,COALESCE(t0.roll_max_bl20,t1.roll_max_bl20,t2.roll_max_bl20,t3.roll_max_bl20,t4.roll_max_bl20,t5.roll_max_bl20,t6.roll_max_bl20,t7.roll_max_bl20) as roll_max_bl20,COALESCE(t0.roll_max_bl500,t1.roll_max_bl500,t2.roll_max_bl500,t3.roll_max_bl500,t4.roll_max_bl500,t5.roll_max_bl500,t6.roll_max_bl500,t7.roll_max_bl500) as roll_max_bl500,COALESCE(t0.rudd,t1.rudd,t2.rudd,t3.rudd,t4.rudd,t5.rudd,t6.rudd,t7.rudd) as rudd,COALESCE(t0.spoil_gnd_arm,t1.spoil_gnd_arm,t2.spoil_gnd_arm,t3.spoil_gnd_arm,t4.spoil_gnd_arm,t5.spoil_gnd_arm,t6.spoil_gnd_arm,t7.spoil_gnd_arm) as spoil_gnd_arm,COALESCE(t0.spoil_lout1,t1.spoil_lout1,t2.spoil_lout1,t3.spoil_lout1,t4.spoil_lout1,t5.spoil_lout1,t6.spoil_lout1,t7.spoil_lout1) as spoil_lout1,COALESCE(t0.spoil_rout1,t1.spoil_rout1,t2.spoil_rout1,t3.spoil_rout1,t4.spoil_rout1,t5.spoil_rout1,t6.spoil_rout1,t7.spoil_rout1) as spoil_rout1,COALESCE(t0.yaw_faul1,t1.yaw_faul1,t2.yaw_faul1,t3.yaw_faul1,t4.yaw_faul1,t5.yaw_faul1,t6.yaw_faul1,t7.yaw_faul1) as yaw_faul1,COALESCE(t0.yaw_faul2,t1.yaw_faul2,t2.yaw_faul2,t3.yaw_faul2,t4.yaw_faul2,t5.yaw_faul2,t6.yaw_faul2,t7.yaw_faul2) as yaw_faul2\n" +
          s"from \n" +
          s"(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,* \n" +
          s"    from  czods.s_qara_320020_2hz  \n" +
          s"    where    flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'     ) t0\n" +
          s"left join \n" +
          s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*\n" +
          s"    from  czods.s_qara_320020_2hz  \n" +
          s"    where    flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'    ) t1\n" +
          s"on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add\n" +
          s"left join \n" +
          s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*\n" +
          s"    from  czods.s_qara_320020_2hz  \n" +
          s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'      ) t2\n" +
          s"on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add\n" +
          s"    left join \n" +
          s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,* \n" +
          s"    from  czods.s_qara_320020_2hz \n" +
          s"    where   flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'     ) t3\n" +
          s"on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add \n" +
          s"left join \n" +
          s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+4 as row_id_add,*\n" +
          s"    from  czods.s_qara_320020_2hz  \n" +
          s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'       ) t4\n" +
          s"on t0.flt_dt=t4.flt_dt and t0.file_no=t4.file_no and t0.tail_num=t4.tail_num and t0.row_id_add=t4.row_id_add\n" +
          s"left join \n" +
          s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+5 as row_id_add,*\n" +
          s"    from  czods.s_qara_320020_2hz  \n" +
          s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'      ) t5\n" +
          s"on t0.flt_dt=t5.flt_dt and t0.file_no=t5.file_no and t0.tail_num=t5.tail_num and t0.row_id_add=t5.row_id_add\n" +
          s"    left join \n" +
          s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+6 as row_id_add,* \n" +
          s"    from  czods.s_qara_320020_2hz \n" +
          s"    where   flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'     ) t6\n" +
          s"on t0.flt_dt=t6.flt_dt and t0.file_no=t6.file_no and t0.tail_num=t6.tail_num and t0.row_id_add=t6.row_id_add\n" +
          s"left join \n" +
          s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+7 as row_id_add,* \n" +
          s"    from  czods.s_qara_320020_2hz \n" +
          s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'       ) t7\n" +
          s"on t0.flt_dt=t7.flt_dt and t0.file_no=t7.file_no and t0.tail_num=t7.tail_num and t0.row_id_add=t7.row_id_add;\n";

        spark.sql(sqlDemo)
    })

  }

  private def deal4hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_4hz_fillup;")
    spark.sql("create table if not exists qara_mid.mid_qara_320020_4hz_fillup with('path'='hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_4hz_fillup') like czods.s_qara_320020_4hz;")
    partitionList.forEach(partition => {
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sqlDemo = "insert into  qara_mid.mid_qara_320020_4hz_fillup \n" +
        s"select t0.file_no,t0.flt_dt,t0.tail_num,t0.`time`,t0.time_series,COALESCE(t0.latg,t1.latg,t2.latg,t3.latg,t4.latg,t5.latg,t6.latg,t7.latg) as latg,COALESCE(t0.latg_csn,t1.latg_csn,t2.latg_csn,t3.latg_csn,t4.latg_csn,t5.latg_csn,t6.latg_csn,t7.latg_csn) as latg_csn,COALESCE(t0.lift_off_pitch1,t1.lift_off_pitch1,t2.lift_off_pitch1,t3.lift_off_pitch1,t4.lift_off_pitch1,t5.lift_off_pitch1,t6.lift_off_pitch1,t7.lift_off_pitch1) as lift_off_pitch1,COALESCE(t0.lift_off_pitch2,t1.lift_off_pitch2,t2.lift_off_pitch2,t3.lift_off_pitch2,t4.lift_off_pitch2,t5.lift_off_pitch2,t6.lift_off_pitch2,t7.lift_off_pitch2) as lift_off_pitch2,COALESCE(t0.lift_off_pitch3,t1.lift_off_pitch3,t2.lift_off_pitch3,t3.lift_off_pitch3,t4.lift_off_pitch3,t5.lift_off_pitch3,t6.lift_off_pitch3,t7.lift_off_pitch3) as lift_off_pitch3,COALESCE(t0.lift_off_pitch4,t1.lift_off_pitch4,t2.lift_off_pitch4,t3.lift_off_pitch4,t4.lift_off_pitch4,t5.lift_off_pitch4,t6.lift_off_pitch4,t7.lift_off_pitch4) as lift_off_pitch4,COALESCE(t0.lift_off_raltc1,t1.lift_off_raltc1,t2.lift_off_raltc1,t3.lift_off_raltc1,t4.lift_off_raltc1,t5.lift_off_raltc1,t6.lift_off_raltc1,t7.lift_off_raltc1) as lift_off_raltc1,COALESCE(t0.lift_off_raltc2,t1.lift_off_raltc2,t2.lift_off_raltc2,t3.lift_off_raltc2,t4.lift_off_raltc2,t5.lift_off_raltc2,t6.lift_off_raltc2,t7.lift_off_raltc2) as lift_off_raltc2,COALESCE(t0.lift_off_raltc3,t1.lift_off_raltc3,t2.lift_off_raltc3,t3.lift_off_raltc3,t4.lift_off_raltc3,t5.lift_off_raltc3,t6.lift_off_raltc3,t7.lift_off_raltc3) as lift_off_raltc3,COALESCE(t0.lift_off_raltc4,t1.lift_off_raltc4,t2.lift_off_raltc4,t3.lift_off_raltc4,t4.lift_off_raltc4,t5.lift_off_raltc4,t6.lift_off_raltc4,t7.lift_off_raltc4) as lift_off_raltc4,COALESCE(t0.lift_off_t1,t1.lift_off_t1,t2.lift_off_t1,t3.lift_off_t1,t4.lift_off_t1,t5.lift_off_t1,t6.lift_off_t1,t7.lift_off_t1) as lift_off_t1,COALESCE(t0.lift_off_t2,t1.lift_off_t2,t2.lift_off_t2,t3.lift_off_t2,t4.lift_off_t2,t5.lift_off_t2,t6.lift_off_t2,t7.lift_off_t2) as lift_off_t2,COALESCE(t0.lift_off_t3,t1.lift_off_t3,t2.lift_off_t3,t3.lift_off_t3,t4.lift_off_t3,t5.lift_off_t3,t6.lift_off_t3,t7.lift_off_t3) as lift_off_t3,COALESCE(t0.lift_off_t4,t1.lift_off_t4,t2.lift_off_t4,t3.lift_off_t4,t4.lift_off_t4,t5.lift_off_t4,t6.lift_off_t4,t7.lift_off_t4) as lift_off_t4,COALESCE(t0.long,t1.long,t2.long,t3.long,t4.long,t5.long,t6.long,t7.long) as long,COALESCE(t0.longc,t1.longc,t2.longc,t3.longc,t4.longc,t5.longc,t6.longc,t7.longc) as longc,COALESCE(t0.long_csn,t1.long_csn,t2.long_csn,t3.long_csn,t4.long_csn,t5.long_csn,t6.long_csn,t7.long_csn) as long_csn,COALESCE(t0.pitch,t1.pitch,t2.pitch,t3.pitch,t4.pitch,t5.pitch,t6.pitch,t7.pitch) as pitch,COALESCE(t0.pitch_csn,t1.pitch_csn,t2.pitch_csn,t3.pitch_csn,t4.pitch_csn,t5.pitch_csn,t6.pitch_csn,t7.pitch_csn) as pitch_csn,COALESCE(t0.pitch_max_ld,t1.pitch_max_ld,t2.pitch_max_ld,t3.pitch_max_ld,t4.pitch_max_ld,t5.pitch_max_ld,t6.pitch_max_ld,t7.pitch_max_ld) as pitch_max_ld,COALESCE(t0.pitch_max_to,t1.pitch_max_to,t2.pitch_max_to,t3.pitch_max_to,t4.pitch_max_to,t5.pitch_max_to,t6.pitch_max_to,t7.pitch_max_to) as pitch_max_to,COALESCE(t0.pitch_ratavgto,t1.pitch_ratavgto,t2.pitch_ratavgto,t3.pitch_ratavgto,t4.pitch_ratavgto,t5.pitch_ratavgto,t6.pitch_ratavgto,t7.pitch_ratavgto) as pitch_ratavgto,COALESCE(t0.pitch_rate,t1.pitch_rate,t2.pitch_rate,t3.pitch_rate,t4.pitch_rate,t5.pitch_rate,t6.pitch_rate,t7.pitch_rate) as pitch_rate,COALESCE(t0.pitch_rate_csn,t1.pitch_rate_csn,t2.pitch_rate_csn,t3.pitch_rate_csn,t4.pitch_rate_csn,t5.pitch_rate_csn,t6.pitch_rate_csn,t7.pitch_rate_csn) as pitch_rate_csn,COALESCE(t0.pitch_ratmaxto,t1.pitch_ratmaxto,t2.pitch_ratmaxto,t3.pitch_ratmaxto,t4.pitch_ratmaxto,t5.pitch_ratmaxto,t6.pitch_ratmaxto,t7.pitch_ratmaxto) as pitch_ratmaxto\n" +
        s"from \n" +
        s"(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_4hz  \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'          ) t0\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_4hz  \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'         ) t1\n" +
        s"on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_4hz  \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'        ) t2\n" +
        s"on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add\n" +
        s"    left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_4hz \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'         ) t3\n" +
        s"on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add \n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+4 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_4hz  \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'         ) t4\n" +
        s"on t0.flt_dt=t4.flt_dt and t0.file_no=t4.file_no and t0.tail_num=t4.tail_num and t0.row_id_add=t4.row_id_add\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+5 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_4hz  \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'        ) t5\n" +
        s"on t0.flt_dt=t5.flt_dt and t0.file_no=t5.file_no and t0.tail_num=t5.tail_num and t0.row_id_add=t5.row_id_add\n" +
        s"    left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+6 as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_4hz \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'         ) t6\n" +
        s"on t0.flt_dt=t6.flt_dt and t0.file_no=t6.file_no and t0.tail_num=t6.tail_num and t0.row_id_add=t6.row_id_add\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+7 as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_4hz \n" +
        s"    where flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'         ) t7\n" +
        s"on t0.flt_dt=t7.flt_dt and t0.file_no=t7.file_no and t0.tail_num=t7.tail_num and t0.row_id_add=t7.row_id_add;\n";
      spark.sql(sqlDemo)
    })
  }

  private def deal8hzTableData(spark: SparkSession, partitionList: util.ArrayList[Row]): Unit = {
    spark.sql("drop table if exists qara_mid.mid_qara_320020_8hz_fillup;")
    spark.sql("create table if not exists qara_mid.mid_qara_320020_8hz_fillup with('path'='hdfs://10.64.219.26:9000/flink/warehouse/qara_mid/mid_qara_320020_8hz_fillup') like czods.s_qara_320020_8hz;")
    partitionList.forEach(partition => {
      System.out.println("begin deal with tuple: " + partition.toString());
      val fltDt = partition.get(0)
      val tailNum = partition.get(1)
      val fileNo = partition.get(2)
      val sqlDemo = "\n" +
        s"insert into  qara_mid.mid_qara_320020_8hz_fillup \n" +
        s"select t0.file_no,t0.flt_dt,t0.tail_num,t0.`time`,t0.time_series\n" +
        s",COALESCE(t0.bad_sf,t1.bad_sf,t2.bad_sf,t3.bad_sf) as bad_sf\n" +
        s",COALESCE(t0.lift_off,t1.lift_off,t2.lift_off,t3.lift_off) as lift_off\n" +
        s",COALESCE(t0.pad,t1.pad,t2.pad,t3.pad) as pad\n" +
        s",COALESCE(t0.t,t1.t,t2.t,t3.t) as t\n" +
        s",COALESCE(t0.time_stamp,t1.time_stamp,t2.time_stamp,t3.time_stamp) as time_stamp\n" +
        s",COALESCE(t0.time_tag,t1.time_tag,t2.time_tag,t3.time_tag) as time_tag\n" +
        s",COALESCE(t0.touch_down,t1.touch_down,t2.touch_down,t3.touch_down) as touch_down\n" +
        s",COALESCE(t0.turbulence,t1.turbulence,t2.turbulence,t3.turbulence) as turbulence\n" +
        s",COALESCE(t0.value_st,t1.value_st,t2.value_st,t3.value_st) as value_st\n" +
        s",COALESCE(t0.vrtg,t1.vrtg,t2.vrtg,t3.vrtg) as vrtg\n" +
        s",COALESCE(t0.vrtg_avg,t1.vrtg_avg,t2.vrtg_avg,t3.vrtg_avg) as vrtg_avg\n" +
        s",COALESCE(t0.vrtg_csn,t1.vrtg_csn,t2.vrtg_csn,t3.vrtg_csn) as vrtg_csn\n" +
        s",COALESCE(t0.vrtg_max_air,t1.vrtg_max_air,t2.vrtg_max_air,t3.vrtg_max_air) as vrtg_max_air\n" +
        s",COALESCE(t0.vrtg_max_gnd,t1.vrtg_max_gnd,t2.vrtg_max_gnd,t3.vrtg_max_gnd) as vrtg_max_gnd\n" +
        s",COALESCE(t0.vrtg_max_ld,t1.vrtg_max_ld,t2.vrtg_max_ld,t3.vrtg_max_ld) as vrtg_max_ld\n" +
        s"from \n" +
        s"(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_8hz  \n" +
        s"    where   flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'      ) t0\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_8hz  \n" +
        s"    where   flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'     ) t1\n" +
        s"on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add\n" +
        s"left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*\n" +
        s"    from  czods.s_qara_320020_8hz  \n" +
        s"    where   flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'    ) t2\n" +
        s"on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add\n" +
        s"    left join \n" +
        s"(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,* \n" +
        s"    from  czods.s_qara_320020_8hz \n" +
        s"    where  flt_dt='$fltDt' and flt_dt='$tailNum' and flt_dt='$fileNo'    ) t3\n" +
        s"on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add ;\n";

      spark.sql(sqlDemo);
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
