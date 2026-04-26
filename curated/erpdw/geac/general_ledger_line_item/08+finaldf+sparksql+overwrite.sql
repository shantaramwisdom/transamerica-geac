select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
source_system_name,
case
when original_cycle_date = '${cycle_date}'
and original_batch_id = ${batchid} then null
else original_cycle_date
end as original_cycle_date,
case
when original_cycle_date = '${cycle_date}'
and original_batch_id = ${batchid} then null
else original_batch_id
end as original_batch_id,
activity_accounting_id,
transaction_number,
line_number,
default_amount_drvd as default_amount,
debit_credit_indicator,
gl_full_source_code,
orig_gl_source_document_id,
orig_gl_company,
orig_gl_account,
orig_gl_center,
orig_gl_description_1,
orig_gl_description_2,
orig_gl_description_3,
orig_gl_project_code,
contractsourcesystemname,
contractnumber,
groupcontractnumber,
activitywithholdingtaxjurisdiction,
statutoryresidentstatecode,
sourceagentidentifier,
activitydepositsourcebatchidentifier,
checknumber,
activitysourceclaimidentifier,
activitysourceoriginatinguserid,
reinsuranceassumedcededflag,
reinsurancetreatybasis,
reinsurancetreatynumber,
typeofbusinessreinsured,
reinsurancegroupindividualflag,
reinsuranceaccountflag,
reinsurancecounterpartytype,
sourcesystemreinsurancecounterpartycode,
cast('${cycle_date}' as date) as cycle_date,
cast(${batchid} as int) as batch_id
from valid_records
