select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
case
    when ('{cycle_date}' = original_cycle_date
          and '{batchid}' = original_batch_id) then from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central')
    else original_recorded_timestamp
end as original_recorded_timestamp,
original_cycle_date,
original_batch_id,
'Soft' as error_classification_name,
error_message,
datediff('{cycle_date}', original_cycle_date) as error_record_aging_days,
to_json(
    struct (
        gl_application_area_code,
        gl_source_code,
        secondary_ledger_code,
        transaction_date,
        gl_reversal_date,
        gl_full_source_code,
        orig_gl_center,
        data_type,
        to_json(
            struct (
                transaction_number_drvd,
                source_system_nm_drvd,
                ledger_name_drvd,
                event_type_code_drvd,
                subledger_short_name_drvd,
                transaction_date_drvd,
                gl_reversal_date_drvd,
                gl_source_code_drvd,
                secondary_ledger_code_drvd,
                reinsuranceassumedcededflag_drvd
            )
        ) as drvd_data
    )
) as error_record,
'{curated_table_name}' as table_name,
'Y' reprocess_flag
from source_df
where len(error_message) > 0
  and len(hard_error_message) = 0;
