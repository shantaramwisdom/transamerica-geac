with source_data as (
    select trim(fsi_gl_pt_effective_date) as transaction_date,
           trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
           trim(fsi_gl_bh_application_area) as gl_application_area_code,
           trim(fsi_gl_pt_source_code) as gl_source_code,
           trim(fsi_gl_pt_company) as secondary_ledger_code,
           trim(fsi_gl_pt_source_code) as gl_full_source_code,
           trim(fsi_gl_pt_center) as orig_gl_center,
           case
               when trim(fsi_gl_pt_account) like '9%'
                    and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
               else 'Financial'
           end as data_type
    from {source_database}.fctransstoupload_current
    where cycle_date = {cycledate}
    union
    select trim(fsi_gl_pt_effective_date) as transaction_date,
           trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
           trim(fsi_gl_bh_application_area) as gl_application_area_code,
           trim(fsi_gl_pt_source_code) as gl_source_code,
           trim(fsi_gl_pt_company) as secondary_ledger_code,
           trim(fsi_gl_pt_source_code) as gl_full_source_code,
           trim(fsi_gl_pt_center) as orig_gl_center,
           case
               when trim(fsi_gl_pt_account) like '9%'
                    and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
               else 'Financial'
           end as data_type
    from {source_database}.fctransmainframe_current
    where cycle_date = {cycledate}
    union
    select trim(fsi_gl_pt_effective_date) as transaction_date,
           trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
           trim(fsi_gl_bh_application_area) as gl_application_area_code,
           trim(fsi_gl_pt_source_code) as gl_source_code,
           trim(fsi_gl_pt_company) as secondary_ledger_code,
           trim(fsi_gl_pt_source_code) as gl_full_source_code,
           trim(fsi_gl_pt_center) as orig_gl_center,
           case
               when trim(fsi_gl_pt_account) like '9%'
                    and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
               else 'Financial'
           end as data_type
    from {source_database}.fctransdistributed_current
    where cycle_date = {cycledate}
),
source as (
    select count(*) as src_cnt
    from source_data
),
source_adj as (
    select count(*) as src_adj_cnt
    from source_data d
        left join lkp_actvty_gl_sbldgr_nm c on d.gl_application_area_code = c.ACTVITY_GL_APP_AREA_CD
        and substr(gl_source_code, 1, 3) = c.ACTVITY_GL_SRC_CD
        left join lkp_actvty_ldgr_nm a on substr(secondary_ledger_code, 3, 2) = d.actvty_gl_ldgr_cd
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
           o.oracle_fah_ldgr_nm as ledger_name_drvd,
           b.src_sys_nm_desc as source_system_nm_drvd,
           b.event_typ_cd as event_type_code_drvd,
           o.oracle_fah_subtgr_nm as subledger_short_name_drvd
    from source_data a
        left join lkp_actvty_gl_sbldgr_nm b on a.gl_application_area_code = b.ACTVITY_GL_APP_AREA_CD and substr(gl_source_code, 1, 3) = b.ACTVITY_GL_SRC_CD
        left join lkp_actvty_ldgr_nm o on substr(secondary_ledger_code, 3, 2) = o.actvty_gl_ldgr_cd and secondary_ledger_code = o.sec_ldgr_cd
        left join lkp_reinsuranceattributes d on substr(a.secondary_ledger_code, 1, 2) = d.legal_entity and a.orig_gl_center = d.gac_centers and substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
    where nvl(b.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
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
    select distinct gl_application_area_code, gl_source_code_drvd
    from target_data a
        left semi join (
            select gl_application_area_code, gl_source_code_drvd
            from target_data
            where transaction_date_chk = 'true'
        ) b
        using (gl_application_area_code, gl_source_code_drvd)
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
          (
              cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
              and cast(nvl(transaction_date, 0) as INTEGER) <= cast(nvl(gl_reversal_date, 0) as INTEGER)
          )
          or cast(nvl(gl_reversal_date, 0) as INTEGER) = 0
      )
    group by gl_application_area_code,
             gl_source_code_drvd
),
target_ignore as (
    select count(*) as target_ign_count
    from (
        select transaction_date,
               gl_reversal_date,
               gl_application_area_code,
               data_type,
               gl_source_code_drvd,
               secondary_ledger_code_drvd,
               ledger_name_drvd,
               source_system_nm_drvd,
               event_type_code_drvd,
               subledger_short_name_drvd
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
              and substr(gl_source_code, 1, 3) not like '% %'
              and substr(secondary_ledger_code, 3, 2) not like '% %'
              and len(gl_application_area_code) > 0
              and len(gl_source_code) >= 3
              and len(secondary_ledger_code) >= 4
              and max_reinsuranceassumedcededflag_error_flag = 0
              and (
                  (
                      cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
                      and cast(nvl(transaction_date, 0) as INTEGER) <= cast(nvl(gl_reversal_date, 0) as INTEGER)
                  )
                  or cast(nvl(gl_reversal_date, 0) as INTEGER) = 0
              )
        ) where fltr > 1
    )
),
new_errors as (
    select sum(err_cnt) as err_cnt
    from (
        select count(*) as err_cnt
        from target_data a
        where transaction_date_chk = 'false'
           or gl_reversal_date_chk = 'false'
           or ledger_name_drvd is null
           or source_system_nm_drvd is null
           or event_type_code_drvd is null
           or subledger_short_name_drvd is null
           or gl_application_area_code is null
           or substr(gl_source_code, 1, 3) like '% %'
           or substr(secondary_ledger_code, 3, 2) like '% %'
           or len(gl_application_area_code) < 1
           or len(gl_source_code) < 3
           or len(secondary_ledger_code) < 4
           or max_reinsuranceassumedcededflag_error_flag = 1
           or reinsuranceassumedcededflag_drvd = 1
           or (
               cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
               and cast(nvl(transaction_date, 0) as INTEGER) > cast(nvl(gl_reversal_date, 0) as INTEGER)
           )
        union all
        select count(*) as err_cnt
        from target_data a
            left semi join target_force_ignore b using (gl_application_area_code, gl_source_code_drvd)
        where transaction_date_chk = 'true'
          and gl_reversal_date_chk = 'true'
          and ledger_name_drvd is not null
          and source_system_nm_drvd is not null
          and event_type_code_drvd is not null
          and subledger_short_name_drvd is not null
          and substr(gl_source_code, 1, 3) not like '% %'
          and substr(secondary_ledger_code, 3, 2) not like '% %'
          and len(gl_application_area_code) > 0
          and len(gl_source_code) >= 3
          and len(secondary_ledger_code) >= 4
          and max_reinsuranceassumedcededflag_error_flag = 0
          and (
              (
                  cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
                  and cast(nvl(transaction_date, 0) as INTEGER) <= cast(nvl(gl_reversal_date, 0) as INTEGER)
              )
              or cast(nvl(gl_reversal_date, 0) as INTEGER) = 0
          )
    )
),
errors_cleared as (
    select count(*) clr_cnt
    from {curated_database}.currentbatch b
        join {curated_database}.{curated_table_name} a using (cycle_date, batch_id)
    where b.source_system = '{source_system_name}'
      and b.domain_name = '{domain_name}'
      and a.original_cycle_date is not null
      and a.original_batch_id is not null
      and (
          a.original_cycle_date != b.cycle_date
          or a.original_batch_id != b.batch_id
      )
      and b.batch_frequency = '{batch_frequency}'
      and b.cycle_date = '{cycle_date}'
      and b.batch_id = {batchid}
)
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       a.*
from (
    SELECT 'S' measure_src_tgt_adj_indicator,
           '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
           src_cnt measure_value
    FROM source
    UNION ALL
    SELECT 'SI' measure_src_tgt_adj_indicator,
           '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
           err_cnt measure_value
    FROM new_errors
    UNION ALL
    SELECT 'A' measure_src_tgt_adj_indicator,
           '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
           src_adj_cnt measure_value
    FROM source_adj
    UNION ALL
    SELECT 'TA' measure_src_tgt_adj_indicator,
           '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
           clr_cnt measure_value
    FROM errors_cleared
    UNION ALL
    SELECT 'TI' measure_src_tgt_adj_indicator,
           '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
           target_ign_count measure_value
    FROM target_ignore
) a