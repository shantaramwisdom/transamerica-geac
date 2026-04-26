select actvty_gl_ldgr_cd,
       oracle_fah_ldgr_nm,
       actvty_lgl_enty_cd || actvty_gl_ldgr_cd as sec_ldgr_cd,
       include_exclude
from time_sch.lkp_actvty_ldgr_nm
where to_date('{cycledate}', 'yyyymmdd') between eff_strt_dt and eff_stop_dt
