select
    from_utc_timestamp(current_timestamp, 'US/Central') as recorded_timestamp,
    case
        when ('{cycle_date}' = original_cycle_date
              and '{batchid}' = original_batch_id)
            then from_utc_timestamp(current_timestamp, 'US/Central')
        else original_recorded_timestamp
    end as original_recorded_timestamp,
    original_cycle_date,
    original_batch_id,
    'Hard' as error_classification_name,
    error_message,
    datediff('{cycle_date}', original_cycle_date) as error_record_aging_days,
    to_json(
        struct(
            gl_application_area_code,
            gl_source_code,
            secondary_ledger_code,
            transaction_date,
            gl_reversal_date,
            gl_full_source_code,
            orig_gl_center,
            data_type,
            to_json(
                struct(
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
    'N' reprocess_flag
from source_df
where length(hard_error_message) > 0

union all

select
    from_utc_timestamp(current_timestamp, 'US/Central') as recorded_timestamp,
    case
        when ('{cycle_date}' = original_cycle_date
              and '{batchid}' = original_batch_id)
            then from_utc_timestamp(current_timestamp, 'US/Central')
        else original_recorded_timestamp
    end as original_recorded_timestamp,
    original_cycle_date,
    original_batch_id,
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
            gl_full_source_code,
            orig_gl_center,
            data_type,
            to_json(
                struct(
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
            ) as drvd_data,
            cast('{cycle_date}' as date) as cycle_date,
            cast({batchid} as int) as batch_id,
            old_error_message
        )
    ) as error_record,
    '{curated_table_name}' as table_name,
    'R' reprocess_flag
from source_df a
where length(error_message) = 0
  and original_cycle_date is not null
  and original_batch_id is not null
  and (
      original_cycle_date != '{cycle_date}'
      or original_batch_id != {batchid}
  );