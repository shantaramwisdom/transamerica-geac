with source_data as (
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
           trim(fsi_gl_pt_company) as orig_gl_company,
           trim(fsi_gl_pt_account) as orig_gl_account,
           trim(fsi_gl_pt_center) as orig_gl_center,
           trim(fsi_gl_pt_source_code) as gl_full_source_code
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
           trim(fsi_gl_pt_company) as orig_gl_company,
           trim(fsi_gl_pt_account) as orig_gl_account,
           trim(fsi_gl_pt_center) as orig_gl_center,
           trim(fsi_gl_pt_source_code) as gl_full_source_code
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
           trim(fsi_gl_pt_company) as orig_gl_company,
           trim(fsi_gl_pt_account) as orig_gl_account,
           trim(fsi_gl_pt_center) as orig_gl_center,
           trim(fsi_gl_pt_source_code) as gl_full_source_code
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
           trim(fsi_gl_pt_company) as orig_gl_company,
           trim(fsi_gl_pt_account) as orig_gl_account,
           trim(fsi_gl_pt_center) as orig_gl_center,
           trim(fsi_gl_pt_source_code) as gl_full_source_code
    from {source_database}.fctransdistributed_current
    where cycle_date = {cycledate}
),
source_adj_data as (
    select a.*
    from source_data a
             left join lkp_actvty_gl_sbldgr_nm c on a.gl_application_area_code = c.ACTVTY_GL_APP_AREA_CD
             left join lkp_actvty_ldgr_nm d on substr(secondary_ledger_code, 3, 2) = d.actvty_gl_ldgr_cd
    where c.include_exclude = 'Exclude'
      or d.include_exclude = 'Exclude'
)
select cast('{cycle_date}' as date) as cycle_date,
       cast({batchid} as int) batch_id,
       cast(
           case
               when cast(nvl(default_amount, 0) as numeric(20,0)) <= 0 then 0
               else cast(nvl(default_amount, 0) as numeric(20,0)) / 100
           end as decimal(18,2)
           ) as default_amount,
       debit_credit_indicator,
       orig_gl_company,
       orig_gl_account,
       orig_gl_center,
       gl_full_source_code,
       measure_type_code
from (
         select default_amount,
                debit_credit_indicator,
                orig_gl_company,
                orig_gl_account,
                orig_gl_center,
                gl_full_source_code,
                'S' as measure_type_code
         from source_data
         union all
         select default_amount,
                debit_credit_indicator,
                orig_gl_company,
                orig_gl_account,
                orig_gl_center,
                gl_full_source_code,
                'SA' as measure_type_code
         from source_adj_data
     ) a;
