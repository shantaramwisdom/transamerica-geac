with source_data as (
    select trim(fsi_gl_pt.effective_date) as transaction_date,
           trim(fsi_gl_bh_rev.effective_date) as gl_reversal_date,
           trim(fsi_gl_bh.application_area) as gl_application_area_code,
           trim(fsi_gl_pt.source_code) as gl_source_code,
           trim(fsi_gl_pt.company) as secondary_ledger_code,
           trim(fsi_gl_pt.source_code) as gl_full_source_code,
           trim(fsi_gl_pt.center) as orig_gl_center,
           case
               when trim(fsi_gl_pt.account) like '9%'
                 and not trim(fsi_gl_pt.account) like '99%' then 'Statistical'
               else 'Financial'
           end as data_type
    from {source_database}.fctransftupload_current
    where cycle_date = {cycledate}
    union
    select trim(fsi_gl_pt.effective_date) as transaction_date,
           trim(fsi_gl_bh_rev.effective_date) as gl_reversal_date,
           trim(fsi_gl_bh.application_area) as gl_application_area_code,
           trim(fsi_gl_pt.source_code) as gl_source_code,
           trim(fsi_gl_pt.company) as secondary_ledger_code,
           trim(fsi_gl_pt.source_code) as gl_full_source_code,
           trim(fsi_gl_pt.center) as orig_gl_center,
           case
               when trim(fsi_gl_pt.account) like '9%'
                 and not trim(fsi_gl_pt.account) like '99%' then 'Statistical'
               else 'Financial'
           end as data_type
    from {source_database}.fctransmainframe_current
    where cycle_date = {cycledate}
    union
    select trim(fsi_gl_pt.effective_date) as transaction_date,
           trim(fsi_gl_bh_rev.effective_date) as gl_reversal_date,
           trim(fsi_gl_bh.application_area) as gl_application_area_code,
           trim(fsi_gl_pt.source_code) as gl_source_code,
           trim(fsi_gl_pt.company) as secondary_ledger_code,
           trim(fsi_gl_pt.source_code) as gl_full_source_code,
           trim(fsi_gl_pt.center) as orig_gl_center,
           case
               when trim(fsi_gl_pt.account) like '9%'
                 and not trim(fsi_gl_pt.account) like '99%' then 'Statistical'
               else 'Financial'
           end as data_type
    from {source_database}.fctransnddistributeg_current
    where cycle_date = {cycledate}
),
source_adj_data as (
    select a.*
    from source_data a
    left join lkp_actvty_gl_sblgdr_nm c on a.gl_application_area_code = c.ACTVTY_GL_APP_AREA_CD
        and substr(gl_source_code, 1, 3) = c.ACTVTY_GL_SRC_CD
    left join lkp_actvty_gl_ldgr_nm d on substr(secondary_ledger_code, 3, 2) = d.actvty_gl_ldgr_cd
        and secondary_ledger_code = d.sec_ldgr_cd
    where c.include_exclude = 'Exclude'
       or d.include_exclude = 'Exclude'
),
target_data_temp as (
    select python_date_format_checker(transaction_date, '%Y%j') as transaction_date_chk,
           python_date_format_checker(gl_reversal_date, '%Y%j', True) as gl_reversal_date_chk,
           gl_source_code,
           secondary_ledger_code,
           transaction_date,
           gl_reversal_date,
           gl_application_area_code,
           data_type,
           substr(gl_source_code, 1, 3) as gl_source_code_drvd,
           substr(secondary_ledger_code, 3, 2) as secondary_ledger_code_drvd,
           case
               when substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
                    then nvl(d.reinsurance_assumed_ceded_flag, '@')
           end as reinsuranceassumedcededflag_drvd,
           decode(reinsuranceassumedcededflag_drvd, '@', 1, 0) as reinsuranceassumedcededflag_error_flag,
           nvl(a.orig_gl_center, '') || '||' || nvl(a.gl_full_source_code, '') as lkp_reinsuranceattributes_key,
           o.oracle_fah_ldgr_nm as ledger_name_drvd,
           b.src_sys_nm_desc as source_system_nm_drvd,
           b.event_typ_cd as event_type_code_drvd,
           b.oracle_fah_sublgdr_nm as subledger_short_name_drvd
    from source_data a
    left join lkp_actvty_gl_sblgdr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
        and substr(gl_source_code, 1, 3) = b.ACTVTY_GL_SRC_CD
    left join lkp_actvty_gl_ldgr_nm c on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
        and secondary_ledger_code = c.sec_ldgr_cd
    left join lkp_reinsuranceattributes d on substr(a.secondary_ledger_code, 1, 2) = d.legal_entity
        and a.orig_gl_center = d.geac_centers
    where substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
      and nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
      and nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
),
target_data as (
    select a.*,
           max(reinsuranceassumedcededflag_error_flag) over (
               partition by ledger_name_drvd,
                            source_system_nm_drvd,
                            event_type_code_drvd,
                            subledger_short_name_drvd,
                            gl_source_code_drvd,
                            gl_application_area_code,
                            transaction_date_chk,
                            gl_reversal_date_chk,
                            data_type,
                            secondary_ledger_code_drvd
           ) as max_reinsuranceassumedcededflag_error_flag
    from target_data_temp a
),
target_force_ignore as (
    select distinct gl_application_area_code,
           gl_source_code_drvd
    from target_data a
    left semi join (
        select gl_application_area_code,
               gl_source_code_drvd
        from target_data
        where transaction_date_chk = 'true'
          and gl_reversal_date_chk = 'true'
          and ledger_name_drvd is not null
          and source_system_nm_drvd is not null
          and event_type_code_drvd is not null
          and subledger_short_name_drvd is not null
          and cast(nvl(transaction_date, 0) as INTEGER) > 0
          and substr(gl_source_code, 1, 3) not like '% %'
          and substr(secondary_ledger_code, 3, 2) not like '% %'
          and len(gl_application_area_code) > 0
          and len(gl_source_code) >= 3
          and len(secondary_ledger_code) >= 4
          and max_reinsuranceassumedcededflag_error_flag = 0
          and (
              cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
              and cast(nvl(transaction_date, 0) as INTEGER) <= cast(nvl(gl_reversal_date, 0) as INTEGER)
              or cast(nvl(gl_reversal_date, 0) as INTEGER) = 0
          )
        group by gl_application_area_code,
                 gl_source_code_drvd
    ) b using (gl_application_area_code, gl_source_code_drvd)
),
target_ignore as (
    select gl_application_area_code,
           transaction_date,
           gl_reversal_date
    from (
        select a.*,
               row_number() over (
                   partition by transaction_date,
                                gl_reversal_date,
                                gl_application_area_code,
                                data_type,
                                gl_source_code_drvd,
                                secondary_ledger_code_drvd,
                                ledger_name_drvd,
                                source_system_nm_drvd,
                                event_type_code_drvd,
                                subledger_short_name_drvd
                   order by null
               ) as fltr
        from target_data a
        left anti join target_force_ignore b using (gl_application_area_code, gl_source_code_drvd)
        where transaction_date_chk = 'true'
          and gl_reversal_date_chk = 'true'
          and ledger_name_drvd is not null
          and source_system_nm_drvd is not null
          and event_type_code_drvd is not null
          and subledger_short_name_drvd is not null
          and cast(nvl(transaction_date, 0) as INTEGER) > 0
          and substr(gl_source_code, 1, 3) not like '% %'
          and substr(secondary_ledger_code, 3, 2) not like '% %'
          and len(gl_application_area_code) > 0
          and len(gl_source_code) >= 3
          and len(secondary_ledger_code) >= 4
          and max_reinsuranceassumedcededflag_error_flag = 0
          and (
              cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
              and cast(nvl(transaction_date, 0) as INTEGER) <= cast(nvl(gl_reversal_date, 0) as INTEGER)
              or cast(nvl(gl_reversal_date, 0) as INTEGER) = 0
          )
    ) a
    where fltr > 1
),
source_ignore as (
    select gl_application_area_code,
           transaction_date,
           gl_reversal_date
    from (
        select a.*,
               row_number() over (
                   partition by gl_source_code,
                                secondary_ledger_code,
                                transaction_date,
                                gl_reversal_date,
                                gl_application_area_code,
                                data_type,
                                gl_source_code_drvd,
                                secondary_ledger_code_drvd,
                                ledger_name_drvd,
                                source_system_nm_drvd,
                                event_type_code_drvd,
                                subledger_short_name_drvd,
                                lkp_reinsuranceattributes_key
                   order by null
               ) as fltr
        from target_data a
        where transaction_date_chk = 'false'
          and gl_reversal_date_chk = 'false'
          or ledger_name_drvd is null
          or source_system_nm_drvd is null
          or event_type_code_drvd is null
          or subledger_short_name_drvd is null
          or gl_application_area_code is null
          or substr(gl_source_code, 1, 3) like '% %'
          or substr(secondary_ledger_code, 3, 2) like '% %'
          or len(gl_application_area_code) = 0
          or len(gl_source_code) < 3
          or len(secondary_ledger_code) < 4
          or max_reinsuranceassumedcededflag_error_flag = 1
          or reinsuranceassumedcededflag_error_flag = 1
          and (
              cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
              and cast(nvl(transaction_date, 0) as INTEGER) > cast(nvl(gl_reversal_date, 0) as INTEGER)
          )
    ) a
    where fltr > 1
)
select cast('{cycle_date}' as date) as cycle_date,
       cast({batchid} as int) batch_id,
       gl_application_area_code,
       cast(substr(transaction_date, 1, 4) as numeric) as transaction_date,
       cast(substr(gl_reversal_date, 1, 4) as numeric) as gl_reversal_date,
       measure_type_code
from (
    select gl_application_area_code,
           transaction_date,
           gl_reversal_date,
           'S' as measure_type_code
    from source_data
    union all
    select gl_application_area_code,
           transaction_date,
           gl_reversal_date,
           'SA' as measure_type_code
    from source_adj_data
    union all
    select gl_application_area_code,
           transaction_date,
           gl_reversal_date,
           'TI' as measure_type_code
    from target_ignore
    union all
    select gl_application_area_code,
           transaction_date,
           gl_reversal_date,
           'SI' as measure_type_code
    from source_ignore
) a;