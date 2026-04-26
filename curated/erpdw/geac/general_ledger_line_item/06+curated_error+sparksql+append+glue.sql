select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
case
when ('cycle_date') = original_cycle_date
and ('batchid') = original_batch_id then from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central')
else cast(original_recorded_timestamp as timestamp)
end as original_recorded_timestamp,
cast(original_cycle_date as date) as original_cycle_date,
cast(original_batch_id as int) as original_batch_id,
'Hard' as error_classification_name,
error_message,
datediff('${cycle_date}', original_cycle_date) as error_record_aging_days,
to_json(
struct(
gl_application_area_code,
gl_source_code,
secondary_ledger_code,
transaction_date,
gl_reversal_date,
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
random_counter,
to_json(
struct(
transaction_number,
source_system_nm_drvd,
ledger_name_drvd,
event_type_code_drvd,
subledger_short_name_drvd,
transaction_date_drvd,
gl_reversal_date_drvd,
gl_source_code_drvd,
secondary_ledger_code_drvd,
default_amount_drvd,
reinsuranceassumedcededflag_drvd,
reinsurancetreatybasis_drvd,
reinsurancetreatynumber_drvd,
typeofbusinessreinsured_drvd,
reinsurancegroupindividualflag_drvd,
reinsuranceaccountflag_drvd
)
) as drvd_data
)
) as error_record,
'${curated_table_name}' as table_name,
'N' reprocess_flag
from source_df
where len(hard_error_message) > 0
union all
select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
case
when ('cycle_date') = original_cycle_date
and ('batchid') = original_batch_id then from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central')
else cast(original_recorded_timestamp as timestamp)
end as original_recorded_timestamp,
cast(original_cycle_date as date) as original_cycle_date,
cast(original_batch_id as int) as original_batch_id,
'Cleared' as error_classification_name,
NULL as error_message,
0 as error_record_aging_days,
to_json(
struct(
gl_application_area_code,
gl_source_code,
secondary_ledger_code,
transaction_date,
gl_reversal_date,
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
random_counter,
to_json(
struct(
transaction_number,
activity_accounting_id,
line_number,
source_system_nm_drvd,
ledger_name_drvd,
event_type_code_drvd,
subledger_short_name_drvd,
transaction_date_drvd,
gl_reversal_date_drvd,
gl_source_code_drvd,
secondary_ledger_code_drvd,
default_amount_drvd,
reinsuranceassumedcededflag_drvd as reinsuranceassumedcededflag_drvd,
reinsurancetreatybasis_drvd as reinsurancetreatybasis_drvd,
reinsurancetreatynumber_drvd as reinsurancetreatynumber_drvd,
typeofbusinessreinsured_drvd as typeofbusinessreinsured_drvd,
reinsurancegroupindividualflag_drvd as reinsurancegroupindividualflag_drvd,
reinsuranceaccountflag_drvd as reinsuranceaccountflag_drvd
)
) as drvd_data,
cast('${cycle_date}' as date) as cycle_date,
cast(${batchid} as int) as batch_id,
old_error_message
) as error_record,
'${curated_table_name}' as table_name,
'R' reprocess_flag
from valid_records a
where original_cycle_date is not null
and original_batch_id is not null
and (original_cycle_date != '${cycle_date}'
or original_batch_id != ${batchid})
