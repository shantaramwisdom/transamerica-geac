select distinct from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
'{source_system_name}' as source_system_name,
cast(
    decode(error_cleared, 'Good - Reprocessed Error', original_cycle_date, null) as date
) as original_cycle_date,
cast(
    decode(error_cleared, 'Good - Reprocessed Error', original_batch_id, null) as int
) as original_batch_id,
transaction_number_drvd as transaction_number,
source_system_nm_drvd as source_system_nm,
ledger_name_drvd as ledger_name,
event_type_code_drvd as event_type_code,
subledger_short_name_drvd as subledger_short_name,
transaction_date_drvd as transaction_date,
gl_reversal_date_drvd as gl_reversal_date,
gl_application_area_code,
gl_source_code_drvd as gl_source_code,
secondary_ledger_code_drvd as secondary_ledger_code,
data_type,
cast('{cycle_date}' as date) as cycle_date,
cast({batchid} as int) as batch_id
from source_df a
where error_cleared like 'Good%';