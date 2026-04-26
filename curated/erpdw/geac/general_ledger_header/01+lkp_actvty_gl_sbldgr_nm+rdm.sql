select evnt_typ_cd,
       src_sys_nm_desc,
       actvty_gl_app_area_cd,
       actvty_gl_src_cd,
       oracle_fah_sublgdr_nm,
       include_exclude
from time_sch.lkp_actvty_gl_sbldgr_nm
where to_date('{cycledate}', 'yyyymmdd') between eff_strt_dt and eff_stop_dt
