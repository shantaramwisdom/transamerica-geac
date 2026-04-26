with source_data as (
    select trim(transaction_date) as transaction_date,
        trim(gl_reversal_date) as gl_reversal_date,
        trim(gl_application_area_code) as gl_application_area_code,
        trim(gl_source_code) as gl_source_code,
        trim(secondary_ledger_code) as secondary_ledger_code,
        trim(gl_full_source_code) as gl_full_source_code,
        trim(orig_gl_center) as orig_gl_center,
        case
            when trim(data_type) like '9%'
                and not trim(data_type) like '99%' then 'Statistical'
            else 'Financial'
        end as data_type,
        cast(
            case
                when cast(nvl(trim(default_amount), 0) as numeric(20, 0)) <= 0 then 0
                else cast(nvl(trim(default_amount), 0) as numeric(20, 0)) / 100
            end as decimal(18, 2)
        ) as default_amount
    from (
        select fsi_gl_pt_effective_date as transaction_date,
            fsi_gl_bh_rev_effective_date as gl_reversal_date,
            fsi_gl_bh_application_area as gl_application_area_code,
            fsi_gl_pt_source_code as gl_source_code,
            fsi_gl_pt_company as secondary_ledger_code,
            fsi_gl_pt_account as data_type,
            fsi_gl_pt_amount as default_amount,
            fsi_gl_pt_source_code as gl_full_source_code,
            fsi_gl_pt_center as orig_gl_center
        from {source_database}.fctransstoupload_current
        where cycle_date = {cycledate}
        union all
        select fsi_gl_pt_effective_date as transaction_date,
            fsi_gl_bh_rev_effective_date as gl_reversal_date,
            fsi_gl_bh_application_area as gl_application_area_code,
            fsi_gl_pt_source_code as gl_source_code,
            fsi_gl_pt_company as secondary_ledger_code,
            fsi_gl_pt_account as data_type,
            fsi_gl_pt_amount as default_amount,
            fsi_gl_pt_source_code as gl_full_source_code,
            fsi_gl_pt_center as orig_gl_center
        from {source_database}.fctransmainframe_current
        where cycle_date = {cycledate}
        union all
        select fsi_gl_pt_effective_date as transaction_date,
            fsi_gl_bh_rev_effective_date as gl_reversal_date,
            fsi_gl_bh_application_area as gl_application_area_code,
            fsi_gl_pt_source_code as gl_source_code,
            fsi_gl_pt_company as secondary_ledger_code,
            fsi_gl_pt_account as data_type,
            fsi_gl_pt_amount as default_amount,
            fsi_gl_pt_source_code as gl_full_source_code,
            fsi_gl_pt_center as orig_gl_center
        from {source_database}.fctransdistributed_current
        where cycle_date = {cycledate}
    )
),
source as (
    select sum(default_amount) as src_default_amount
    from source_data
),
source_adj as (
    select sum(default_amount) as src_adj_default_amount
    from source_data a
        left join lkp_actvty_gl_sbldgr_nm c on a.gl_application_area_code = c.actvty_gl_app_area_cd
        and substr(gl_source_code, 1, 3) = c.actvty_gl_src_cd
        left join lkp_actvty_ldgr_nm d on substr(secondary_ledger_code, 3, 2) = d.actvty_gl_ldgr_cd
        and secondary_ledger_code = d.sec_ldgr_cd
    where c.include_exclude = 'Exclude'
        or d.include_exclude = 'Exclude'
),
target_data_temp as (
    select python_date_format_checker(transaction_date, '%Y%j') as transaction_date_chk,
        python_date_format_checker(gl_reversal_date, '%Y%j', true) as gl_reversal_date_chk,
        gl_source_code,
        secondary_ledger_code,
        transaction_date,
        gl_reversal_date,
        gl_application_area_code,
        data_type,
        substr(gl_source_code, 1, 3) as gl_source_code_drvd,
        substr(secondary_ledger_code, 3, 2) as secondary_ledger_code_drvd,
        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444') then nvl(d.reinsurance_assumed_ceded_flag, '@')
        end as reinsuranceassumedcededflag_drvd,
        decode(reinsuranceassumedcededflag_drvd, '@', 0, 1) as reinsuranceassumedcededflag_error_flag,
        c.oracle_fah_ldgr_nm as ledger_name_drvd,
        b.src_sys_nm desc as source_system_nm_drvd,
        b.evnt_typ_cd as event_type_code_drvd,
        b.oracle_fah_subldgr_nm as subledger_short_name_drvd,
        default_amount,
        monotonically_increasing_id() random_counter
    from source_data a
        left join lkp_actvty_gl_sbldgr_nm b on a.gl_application_area_code = b.actvty_gl_app_area_cd
        and substr(gl_source_code, 1, 3) = b.actvty_gl_src_cd
        left join lkp_actvty_ldgr_nm c on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
        and secondary_ledger_code = c.sec_ldgr_cd
        left join lkp_reinsurance_attributes d on substr(a.secondary_ledger_code, 1, 2) = d.legal_entity
        and a.orig_gl_center = d.geac_centers
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
                transaction_date,
                gl_reversal_date,
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
                and cast(nvl(transaction_date, 0) as integer) > 0
                and substr(gl_source_code, 1, 3) not like '% %'
                and substr(secondary_ledger_code, 3, 2) not like '% %'
                and len(gl_application_area_code) > 0
                and len(gl_source_code) >= 3
                and len(secondary_ledger_code) >= 4
                and max_reinsuranceassumedcededflag_error_flag = 0
                and (
            cast(nvl(gl_reversal_date, 0) as integer) > 0
            and cast(nvl(transaction_date, 0) as integer) <= cast(nvl(gl_reversal_date, 0) as integer)
            or cast(nvl(gl_reversal_date, 0) as integer) = 0
        )
    group by gl_application_area_code,
        gl_source_code_drvd
) b using (gl_application_area_code, gl_source_code_drvd)
where transaction_date_chk = 'false'
    or gl_reversal_date_chk = 'false'
    or (
        cast(nvl(gl_reversal_date, 0) as int) > 0
        and cast(nvl(transaction_date, 0) as int) > cast(nvl(gl_reversal_date, 0) as int)
    )
),
new_errors as (
    select sum(err_default_amount) as err_default_amount
    from (
            select sum(default_amount) as err_default_amount
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
                or len(gl_application_area_code) = 0
                or len(gl_source_code) < 3
                or len(secondary_ledger_code) < 4
                or max_reinsuranceassumedcededflag_error_flag = 1
                or reinsuranceassumedcededflag_error_flag = 1
                and (
                    cast(nvl(gl_reversal_date, 0) as integer) > 0
                    and cast(nvl(transaction_date, 0) as integer) > cast(nvl(gl_reversal_date, 0) as integer)
                )
        union all
        select sum(default_amount) as err_default_amount
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
                cast(nvl(gl_reversal_date, 0) as integer) > 0
                and cast(nvl(transaction_date, 0) as integer) <= cast(nvl(gl_reversal_date, 0) as integer)
                or cast(nvl(gl_reversal_date, 0) as integer) = 0
            )
    )
),
errors_cleared as (
    select sum(default_amount) as clr_default_amount
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
SELECT (batchid) batch_id,
    ('{cycle_date}') cycle_date,
    ('{domain_name}') domain,
    ('{source_system_name}') sys_nm,
    ('{source_system_name}') p_sys_nm,
    'CONTROL_TOTAL' measure_name,
    'curated' hop_name,
    'fsi_gl_pt_amount' measure_field,
    'INTEGER' measure_value_datatype,
    a.*
from (
        SELECT 'S' measure_src_tgt_adj_indicator,
            '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
            nvl(src_default_amount, 0) as measure_value
        from source
        union all
        SELECT 'SI' measure_src_tgt_adj_indicator,
            '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
            nvl(err_default_amount, 0) as measure_value
        from new_errors
        union all
        SELECT 'A' measure_src_tgt_adj_indicator,
            '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
            nvl(src_adj_default_amount, 0) as measure_value
        from source_adj
        union all
        SELECT 'TA' measure_src_tgt_adj_indicator,
            '{source_database}.fctransstoupload_current/fctransmainframe_current/fctransdistributed_current' measure_table,
            nvl(clr_default_amount, 0) as measure_value
        from errors_cleared
    ) a

                