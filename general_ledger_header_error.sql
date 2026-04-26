-- dw_materialized_views.general_ledger_header_error source

CREATE MATERIALIZED VIEW dw_materialized_views.general_ledger_header_error
BACKUP NO
DISTSTYLE KEY DISTKEY(table_name)
SORTKEY(original_cycle_date, original_batch_id, error_classification_name)
AS
SELECT
	recorded_timestamp,
	original_cycle_date,
	original_batch_id,
	original_recorded_timestamp,
	cast(split_part(table_name, '_', 1) as varchar(50)) as source_system_name,
	table_name,
	error_classification_name,
	error_message,
	error_record_aging_days,
	reprocess_flag,
	error_json.source_system_nm,
	error_json.ledger_name,
	error_json.subledger_short_name,
	error_json.event_type_code,
	error_json.transaction_date,
	error_json.gl_reversal_date,
	error_json.gl_application_area_code,
	error_json.gl_source_code,
	error_json.secondary_ledger_code,
	error_json.data_type,
	error_json.sourceactivityparentid,
	error_json.header_description,
	error_json.activitytypegroup,
	error_json.activitytype,
	error_json.activityamounttype,
	error_json.fundclassindicator,
	error_json.ifrs17grouping,
	error_json.ifrs17portfolio,
	error_json.ifrs17profitability,
	error_json.ifrs17cohort,
	error_json.plancode,
	error_json.fundnumber,
	error_json.sourcelegalentitycode,
	error_json.orig_gl_account,
	error_json.orig_gl_company,
	error_json.orig_gl_center,
	error_json.gl_full_source_code,
	error_json.sourcesystemgeneralledgeraccountnumber,
	error_json.contractissuedate,
	error_json.src_desc,
	error_json.reinsurancetreatynumber,
	error_json as error_record
FROM (
	SELECT *,
		json_parse(error_record) as error_json
	FROM financedwcurated.curated_error
	WHERE reprocess_flag in ('Y', 'N')
		AND table_name like '%general_ledger_header%'
);
