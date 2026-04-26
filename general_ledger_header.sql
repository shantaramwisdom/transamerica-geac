-- erpdw_views.general_ledger_header source

CREATE OR REPLACE VIEW erpdw_views.general_ledger_header AS
SELECT
	transaction_number
	,secondary_ledger_code
	,ledger_name
	,subledger_short_name
	,event_type_code
	,source_system_nm
	,data_type
	,transaction_date
	,header_description
	,gl_application_area_code
	,gl_reversal_date
	,gl_source_code
	,source_system_name
	,cycledate
	,batchid
	,original_cycle_date
	,original_batch_id
	,inserttimestamp
	,updatetimestamp
	,sourceactivityparentid
	,reinsurancetreatynumber
from
	erpdw.general_ledger_header
WITH NO SCHEMA BINDING;
