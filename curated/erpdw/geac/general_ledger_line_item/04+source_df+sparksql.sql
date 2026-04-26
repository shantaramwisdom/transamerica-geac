with current_run_all_valid_headers as (
    select original_cycle_date,
        original_batch_id,
        transaction_number,
        ledger_name,
        gl_application_area_code,
        transaction_date,
        data_type,
        gl_reversal_date,
        secondary_ledger_code,
        source_system_nm,
        event_type_code,
        subledger_short_name,
        gl_source_code
    from {curated_database}.{source_system_name}_general_ledger_header
    where cycle_date = '{cycle_date}'
        and batch_id = {batchid}
        and original_cycle_date is not null
        and original_batch_id is not null
        and (
            original_batch_id != {batchid}
            or original_cycle_date != '{cycle_date}'
        )
    union all
    select cycle_date,
        batch_id,
        transaction_number,
        ledger_name,
        gl_application_area_code,
        transaction_date,
        data_type,
        gl_reversal_date,
        secondary_ledger_code,
        source_system_nm,
        event_type_code,
        subledger_short_name,
        gl_source_code
    from {curated_database}.{source_system_name}_general_ledger_header
    where cycle_date = '{cycle_date}'
        and batch_id = {batchid}
        and original_cycle_date is null
        and original_batch_id is null
),
input as (
    select *,
        from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as original_recorded_timestamp,
        CAST('{cycle_date}' AS DATE) as original_cycle_date,
        CAST({batchid} AS INT) as original_batch_id,
        cast(null as string) as old_error_message,
        cast(null as int) as error_record_aging_days
    from (
        select trim(fsi_gl_pt_effective_date) as transaction_date,
               trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
               trim(fsi_gl_bh_application_area) as gl_application_area_code,
               trim(fsi_gl_pt_source_code) as gl_source_code,
               trim(fsi_gl_pt_company) as secondary_ledger_code,
               case
                   when trim(fsi_gl_pt_account) like '9%'
                        and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
                   else 'Financial'
               end as data_type,
               trim(fsi_gl_pt_amount) as default_amount,
               case
                   when trim(fsi_gl_pt_dr_cr_code) in ('10', '13', 'DR') then 'Debit'
                   when trim(fsi_gl_pt_dr_cr_code) in ('60', '63', 'CR') then 'Credit'
               end as debit_credit_indicator,
               trim(fsi_gl_pt_source_document_id) as orig_gl_source_document_id,
               trim(fsi_gl_pt_company) as orig_gl_company,
               trim(fsi_gl_pt_account) as orig_gl_account,
               trim(fsi_gl_pt_center) as orig_gl_center,
               trim(fsi_gl_pt_source_code) as gl_full_source_code,
               ascii_ignore(trim(fsi_gl_pt_description_1)) as orig_gl_description_1,
               ascii_ignore(trim(fsi_gl_pt_description_2)) as orig_gl_description_2,
               ascii_ignore(trim(fsi_gl_pt_description_3)) as orig_gl_description_3,
               trim(fsi_gl_pt_project_code) as orig_gl_project_code
        from {source_database}.fctransupload_current
        where cycle_date = {cycledate}
        union all
        select trim(fsi_gl_pt_effective_date) as transaction_date,
               trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
               trim(fsi_gl_bh_application_area) as gl_application_area_code,
               trim(fsi_gl_pt_source_code) as gl_source_code,
               trim(fsi_gl_pt_company) as secondary_ledger_code,
               case
                   when trim(fsi_gl_pt_account) like '9%'
                        and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
                   else 'Financial'
               end as data_type,
               trim(fsi_gl_pt_amount) as default_amount,
               case
                   when trim(fsi_gl_pt_dr_cr_code) in ('10', '13', 'DR') then 'Debit'
                   when trim(fsi_gl_pt_dr_cr_code) in ('60', '63', 'CR') then 'Credit'
               end as debit_credit_indicator,
               trim(fsi_source_document_id) as orig_gl_source_document_id,
               trim(fsi_gl_pt_company) as orig_gl_company,
               trim(fsi_gl_pt_account) as orig_gl_account,
               trim(fsi_gl_pt_center) as orig_gl_center,
               trim(fsi_gl_pt_source_code) as gl_full_source_code,
               ascii_ignore(trim(fsi_gl_pt_description_1)) as orig_gl_description_1,
               ascii_ignore(trim(fsi_gl_pt_description_2)) as orig_gl_description_2,
               ascii_ignore(trim(fsi_gl_pt_description_3)) as orig_gl_description_3,
               trim(fsi_gl_pt_project_code) as orig_gl_project_code
        from {source_database}.fctransmainframe_current
        where cycle_date = {cycledate}
        union all
        select trim(fsi_gl_pt_effective_date) as transaction_date,
               trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
               trim(fsi_gl_bh_application_area) as gl_application_area_code,
               trim(fsi_gl_pt_source_code) as gl_source_code,
               trim(fsi_gl_pt_company) as secondary_ledger_code,
               case
                   when trim(fsi_gl_pt_account) like '9%'
                        and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
                   else 'Financial'
               end as data_type,
               trim(fsi_gl_pt_amount) as default_amount,
               case
                   when trim(fsi_gl_pt_dr_cr_code) in ('10', '13', 'DR') then 'Debit'
                   when trim(fsi_gl_pt_dr_cr_code) in ('60', '63', 'CR') then 'Credit'
               end as debit_credit_indicator,
               trim(fsi_gl_pt_source_document_id) as orig_gl_source_document_id,
               trim(fsi_gl_pt_company) as orig_gl_company,
               trim(fsi_gl_pt_account) as orig_gl_account,
               trim(fsi_gl_pt_center) as orig_gl_center,
               trim(fsi_gl_pt_source_code) as gl_full_source_code,
               ascii_ignore(trim(fsi_gl_pt_description_1)) as orig_gl_description_1,
               ascii_ignore(trim(fsi_gl_pt_description_2)) as orig_gl_description_2,
               ascii_ignore(trim(fsi_gl_pt_description_3)) as orig_gl_description_3,
               trim(fsi_gl_pt_project_code) as orig_gl_project_code
        from {source_database}.fctranslife_current
        where cycle_date = {cycledate}
    )
)
union all
select transaction_date,
    gl_reversal_date,
    gl_application_area_code,
    gl_source_code,
    secondary_ledger_code,
    data_type,
    default_amount,
    debit_credit_indicator,
    orig_gl_source_document_id,
    orig_gl_company,
    orig_gl_account,
    orig_gl_center,
    gl_full_source_code,
    orig_gl_description_1,
    orig_gl_description_2,
    orig_gl_description_3,
    orig_gl_project_code,
    original_recorded_timestamp,
    original_cycle_date,
    original_batch_id,
    error_message,
    error_record_aging_days
from error_line_item
),
master as (
    select a.*,
        case
            when gl_source_code is null
                or len(trim(substr(gl_source_code, 1, 3))) < 3
                or substr(gl_source_code, 1, 3) like '% %' then null
            else substr(gl_source_code, 1, 3)
        end as gl_source_code_drvd,
        case
            when secondary_ledger_code is null
                or len(trim(substr(secondary_ledger_code, 3, 2))) < 2 then null
            else substr(secondary_ledger_code, 3, 2)
        end as secondary_ledger_code_drvd,
        nvl(
            cast(
                case
                    when cast(nvl(default_amount, 0) as numeric(20, 0)) <= 0 then 0
                    else cast(nvl(default_amount, 0) as numeric(20, 0)) / 100
                end as decimal(18, 2)
            ),
            0
        ) as default_amount_drvd,
        c.oracle_fah_ldgr_nm as ledger_name_drvd,
        b.src_sys_nm desc as source_system_nm_drvd,
        b.evnt_typ_cd as event_type_code_drvd,
        b.oracle_fah_subldgr_nm as subledger_short_name_drvd,
        python_date_format_checker(transaction_date, '%Y%j') as transaction_date_chk,
        python_date_format_checker(gl_reversal_date, '%Y%j', true) as gl_reversal_date_chk,
        case
            when transaction_date_chk = 'true' then to_date(transaction_date, 'yyyyDDD')
        end as transaction_date_drvd,
        case
            when gl_reversal_date_chk = 'true' then to_date(gl_reversal_date, 'yyyyDDD')
        end as gl_reversal_date_drvd,
        case
            when transaction_date_chk = 'false'
                or gl_reversal_date_chk = 'false'
                or (
                    cast(nvl(gl_reversal_date, 0) as int) > 0
                    and cast(nvl(transaction_date, 0) as int) < cast(nvl(gl_reversal_date, 0) as int)
                ) then 'N'
            else 'Y'
        end as date_error,
        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
                then nvl(d.reinsurance_assumed_ceded_flag, '@')
        end as reinsuranceassumedcededflag_drvd,
        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
                then nvl(d.reinsurance_basis, '@')
        end as reinsurancetreatybasis_drvd,
        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
                then nvl(d.treaty_name, '@')
        end as reinsurancetreatynumber_drvd,
        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
                then nvl(d.type_of_business_reinsured, '@')
        end as typeofbusinessreinsured_drvd,
        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
                then nvl(d.reinsurance_group_individual_flag, '@')
        end as reinsurancegroupindividualflag_drvd,
        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444')
                then nvl(d.reinsurance_account_flag, '@')
        end as reinsuranceaccountflag_drvd,
        monotonically_increasing_id() random_counter
    from input a
        left join lkp_actvty_gl_sbldgr_nm b on a.gl_application_area_code = b.actvty_gl_app_area_cd
        and substr(gl_source_code, 1, 3) = b.actvty_gl_src_cd
        left join lkp_actvty_ldgr_nm c on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
        and secondary_ledger_code = c.sec_ldgr_cd
        left join lkp_reinsurance_attributes d on substr(a.secondary_ledger_code, 1, 2) = d.legal_entity
        and a.orig_gl_center = d.geac_centers
    where nvl(b.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
        and nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
),
final as (
    select a.*,
        b.transaction_number,
        trim(
            case
                when transaction_date_chk = 'false'
                    or gl_reversal_date_chk = 'false'
                    or (
                        cast(nvl(gl_reversal_date, 0) as integer) > 0
                        and cast(a.transaction_date as int) > cast(a.gl_reversal_date as int)
                    ) then 'Header Errored for Date Validation;'
                else ''
            end
            || case
                when c.original_cycle_date is not null
                    and a.date_error = 'N' then 'Forced Error. Date Errors Present in Other Headers for combination of gl_application_area_code ('
                    || nvl(a.gl_application_area_code, '')
                    || ') and gl_source_code ('
                    || nvl(a.gl_source_code, '')
                    || ');'
                else ''
            end
            || case
                when a.gl_application_area_code is null
                    or len(a.gl_application_area_code) = 0 then nvl(a.gl_application_area_code, 'BLANK/NULL')
                    || ' gl_application_area_code is Invalid from Source;'
                else ''
            end
            || case
                when a.gl_source_code is null
                    or substr(a.gl_source_code, 1, 3) like '% %'
                    or len(a.gl_source_code) < 3 then nvl(a.gl_source_code, 'BLANK/NULL')
                    || ' gl_source_code is Invalid from Source;'
                else ''
            end
            || case
                when a.secondary_ledger_code is null
                    or substr(a.secondary_ledger_code, 3, 2) like '% %'
                    or len(a.secondary_ledger_code) < 4 then nvl(a.secondary_ledger_code, 'BLANK/NULL')
                    || ' secondary_ledger_code is Invalid from Source;'
                else ''
            end
        ) as hard_error_message,
        trim(
            case
                when len(trim(hard_error_message)) = 0
                    and (
                        b.transaction_number is null
                        or source_system_nm_drvd is null
                        or ledger_name_drvd is null
                        or event_type_code_drvd is null
                        or subledger_short_name_drvd is null
                        or nvl(reinsuranceassumedcededflag_drvd, '') = '@'
                    ) then 'Header Errored for RDM Lookup;'
                else ''
            end
        ) as soft_error_message,
trim(
concat(
nvl(hard_error_message, ''),
nvl(soft_error_message, '')
)
) as error_message,
'${source_system_name}' as source_system_name,
case
when len(error_message) = 0 then row_number() over (
partition by b.transaction_number
order by default_amount_drvd,
debit_credit_indicator,
gl_full_source_code,
orig_gl_source_document_id,
orig_gl_company,
orig_gl_account,
orig_gl_center,
orig_gl_description_1,
orig_gl_description_2,
orig_gl_description_3,
orig_gl_project_code
)
end as line_number
from master a
left join current_run_all_valid_headers b on a.original_cycle_date = b.original_cycle_date
and a.original_batch_id = b.original_batch_id
and a.source_system_nm_drvd = b.source_system_nm
and a.event_type_code_drvd = b.event_type_code_drvd
and a.ledger_name_drvd = b.ledger_name_drvd
and a.subledger_short_name_drvd = b.subledger_short_name_drvd
and a.transaction_date_drvd = b.transaction_date_drvd
and a.gl_application_area_code = b.gl_application_area_code
and a.gl_source_code_drvd = b.gl_source_code_drvd
and a.secondary_ledger_code_drvd = b.secondary_ledger_code_drvd
and a.data_type = b.data_type
and coalesce(a.gl_reversal_date_drvd, '') = coalesce(b.gl_reversal_date_drvd, '')
left join (
select distinct original_cycle_date,
original_batch_id,
gl_application_area_code,
gl_source_code
from current_run_forced_header_errors
) c on a.original_cycle_date = c.original_cycle_date
and a.original_batch_id = c.original_batch_id
and a.gl_application_area_code = c.gl_application_area_code
;