select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
source_system_name,
original_cycle_date,
original_batch_id,
activity_accounting_id,
transaction_number,
row_number() over (
partition by transaction_number
order by default_amount,
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
activity_accounting_id
) as line_number,
default_amount,
default_amount_drvd,
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
cast('cycle_date' as date) as cycle_date,
cast(batchid as int) as batch_id,
gl_application_area_code,
gl_source_code,
secondary_ledger_code,
gl_reversal_date,
data_type,
source_system_nm_drvd,
ledger_name_drvd,
event_type_code_drvd,
subledger_short_name_drvd,
transaction_date_drvd,
gl_reversal_date_drvd,
gl_source_code_drvd,
secondary_ledger_code_drvd,
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
contractsourcesystemname,
reinsurancecounterpartytype,
sourcesystemreinsurancecounterpartycode,
random_counter,
old_error_message,
error_record_aging_days,
original_recorded_timestamp
from (
select *,
(gl_activityaccountingid)
from (
select source_system_name,
cast(original_cycle_date as date) as original_cycle_date,
cast(original_batch_id as int) original_batch_id,
line_number,
transaction_number,
default_amount,
default_amount_drvd,
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
gl_application_area_code,
gl_source_code,
secondary_ledger_code,
transaction_date,
gl_reversal_date,
data_type,
source_system_nm_drvd,
ledger_name_drvd,
event_type_code_drvd,
subledger_short_name_drvd,
transaction_date_drvd,
gl_reversal_date_drvd,
gl_source_code_drvd,
secondary_ledger_code_drvd,
reinsuranceassumedcededflag_drvd as reinsuranceassumedcededflag,
reinsurancetreatybasis_drvd as reinsurancetreatybasis,
reinsurancetreatynumber_drvd as reinsurancetreatynumber,
typeofbusinessreinsured_drvd as typeofbusinessreinsured,
reinsurancegroupindividualflag_drvd as reinsurancegroupindividualflag,
reinsuranceaccountflag_drvd as reinsuranceaccountflag,
case
when left(gl_full_source_code, 3) = '545' then left(orig_gl_description_1, 10)
when left(gl_full_source_code, 3) = '038' and left(orig_gl_account, 1) = '1' and orig_gl_account != '2872400' then trim(left(orig_gl_description_1, 10))
when left(gl_full_source_code, 3) in ('863', '864') then trim(left(orig_gl_description_1, 12))
when left(gl_full_source_code, 3) = 'HIP' and left(orig_gl_account, 1) = '2' then trim(left(orig_gl_description_1, 15))
when left(gl_full_source_code, 3) = '049' then trim(left(orig_gl_description_1, 28))
when left(gl_full_source_code, 3) = 'A4V' and left(orig_gl_account, 4) = 'VOID' then trim(substring(orig_gl_description_1, 6, 10))
when left(gl_full_source_code, 3) = '041' then orig_gl_description_1
when left(gl_full_source_code, 3) = '041' and orig_gl_account = '2861600' then orig_gl_description_1
when left(gl_full_source_code, 3) = '041' and orig_gl_account in ('2872500','2886200','2885800','2890000') then orig_gl_description_1
when left(gl_full_source_code, 3) = '041' then
case
when orig_gl_account = '2861600' then orig_gl_description_2
else orig_gl_description_1
end
when left(gl_full_source_code, 3) in ('062','122','123') and orig_gl_account != '2861600' then orig_gl_description_1
when left(gl_full_source_code, 3) in ('165','147','430') and left(orig_gl_account, 1) = '2' then orig_gl_description_1
when left(gl_full_source_code, 3) = '043' then
case
--
when orig_gl_account not in ('2871100','2890200','2891900','2898000') and left(orig_gl_account,1)='2' then orig_gl_description_1
when orig_gl_account='2898000' then orig_gl_description_2
end
when left(gl_full_source_code,3)='075' and left(orig_gl_account,1)='2' and orig_gl_account='2891900' then orig_gl_description_1
when left(gl_full_source_code,3)='074' and orig_gl_description_1 like '%OTCKC%' then orig_gl_description_3
when left(gl_full_source_code,3) in ('296','456','464','851','959') then orig_gl_description_1
when left(gl_full_source_code,3)='143' and left(orig_gl_account,2) not in ('19','99') then orig_gl_description_2
when left(gl_full_source_code,3)='069' and left(orig_gl_description_2,2) in ('TA','N') then orig_gl_description_2
when left(gl_full_source_code,3)='057' then
case
when left(orig_gl_account,1) in ('4','6','7') or left(orig_gl_account,3)='829' or left(orig_gl_account,1)='2' 
or (left(orig_gl_account,1)='1' and orig_gl_account not in ('2861600','2871000','2872600','2872700','2872700','2872700','2872700','2872700','2872700','2872700','2872700','2872700','2872700','2872700')) 
then orig_gl_description_1
when orig_gl_account in ('1110900','1806200','2861600','2872700','2890100','2898000') then orig_gl_description_2
end
when left(gl_full_source_code,3)='145' then
case
when left(orig_gl_account,2) in ('19','28') and orig_gl_account!='2893600' then orig_gl_description_1
when left(orig_gl_account,1) in ('4','7') or orig_gl_account='2893600' then orig_gl_description_2
end
end as contractnumber,
case
when left(gl_full_source_code,3)='146' then
case
when orig_gl_account='10A1200' then trim(left(orig_gl_description_1,13))
when left(orig_gl_account,1)='2' and orig_gl_account='2873600' then orig_gl_source_document_id
end
when left(gl_full_source_code,3)='143' and left(orig_gl_account,2) in ('19','28') then orig_gl_source_document_id
end as groupcontractnumber,
case
when left(gl_full_source_code,3) in ('051','052','122','123') and orig_gl_account='2861600' then orig_gl_description_1
when left(gl_full_source_code,3)='549' and left(orig_gl_account,1)='2' then trim(substring(orig_gl_description_1,29,2))
when left(gl_full_source_code,3) in ('452','453','454','458','460','462','463') and left(orig_gl_account,1)='1' then trim(substring(orig_gl_description_1,29,2))
when left(gl_full_source_code,3)='143' and left(orig_gl_account,1)='1' then trim(substring(orig_gl_description_1,29,2))
when left(gl_full_source_code,3)='863' and left(orig_gl_account,1)='2' then trim(substring(orig_gl_description_1,13,2))
end as activitywithholdingtaxjurisdiction,
case
when left(gl_full_source_code,3)='141' then
case
when orig_gl_account='1117300' then orig_gl_description_2
when orig_gl_account in ('1120300','1120500','2870500','2801800') then orig_gl_description_1
end
when left(gl_full_source_code,3)='146' and orig_gl_account='1120300' then orig_gl_description_1
when left(gl_full_source_code,3)='038' and orig_gl_account='2872400' then trim(left(orig_gl_description_1,10))
when left(gl_full_source_code,3)='073' and orig_gl_account in ('2871300','2888900') then orig_gl_description_2
when left(gl_full_source_code,3)='145' and orig_gl_account='2861600' then orig_gl_description_1
when left(gl_full_source_code,3)='039' then
case
when orig_gl_description_1 like '%DTCCK%' then orig_gl_description_2
else orig_gl_description_3
end
when left(gl_full_source_code,3)='668' and orig_gl_description_1 not like '%NSCCK%' then orig_gl_description_1
when left(gl_full_source_code,3)='057' then
case
when left(orig_gl_account,1)='5' or left(orig_gl_account,3) in ('825','826','849') or orig_gl_account in ('1120400','2871100') then orig_gl_description_1
end
when left(gl_full_source_code,3) in ('039','077') then orig_gl_description_3
end as sourceagentidentifier,
case
when left(gl_full_source_code,3)='051' and orig_gl_account!='2861600' then orig_gl_description_2
when left(gl_full_source_code,3)='146' then
case
when orig_gl_account='10A1200' then trim(substring(orig_gl_description_1,14,17))
when orig_gl_account in ('1004100','1090900','1090900','1090600') then orig_gl_description_1
when orig_gl_account in ('2890400','1120300','1997000','1910000','2870500','2878180','2890400','2891900') then orig_gl_description_2
when left(orig_gl_account,4) in ('4111','4121') then right(orig_gl_source_document_id,8)
when left(orig_gl_account,1)='7' then orig_gl_source_document_id
end
when left(gl_full_source_code,3)='402' then
case
when orig_gl_account in ('10A1200','2871300') then orig_gl_description_1
when orig_gl_account='2893700' then orig_gl_description_2
end
when left(gl_full_source_code,3)='057' and orig_gl_account in ('1110900','1806200','2872600','2872700','2886500','2890100','2898000') then orig_gl_description_1
when left(gl_full_source_code,3)='043' then
case
when orig_gl_account in ('1110900','2898000') then orig_gl_description_1
when left(orig_gl_account,2) in ('10','28') then orig_gl_description_2
end
when left(gl_full_source_code,3)='038' then
case
when left(orig_gl_account,1)='1' then orig_gl_description_1
else orig_gl_description_2
end
when left(gl_full_source_code,3) in ('075','109') then orig_gl_description_2
when left(gl_full_source_code,3)='545' then trim(substring(orig_gl_description_1,16,15))
end as activitydepositsourcebatchidentifier,
case
when left(gl_full_source_code,3)='017' and orig_gl_description_1 not like '%DTCCK%' then orig_gl_description_2
when left(gl_full_source_code,3) in ('039','049','863','864') then orig_gl_description_2
when left(gl_full_source_code,3) in ('456','464') then orig_gl_description_3
end as checknumber,
case
when left(gl_full_source_code,3)='HIP' and left(orig_gl_account,1)='2' then trim(substring(orig_gl_description_1,16,15))
end as activitysourceclaimidentifier,
case
when left(gl_full_source_code,3)='038' and left(orig_gl_account,1)!='1' then trim(substring(orig_gl_description_1,11,20))
when left(gl_full_source_code,3)='141' then
case
when left(orig_gl_account,1)='7' then orig_gl_description_2
else orig_gl_project_code
end
when left(gl_full_source_code,3)='545' then trim(substring(orig_gl_description_1,11,5))
end as activitysourceoriginatinguserid,
case
when left(gl_full_source_code,3)='903' then 'External SCOR'
when left(gl_full_source_code,3) in ('851','444') then 'Wilton External'
end as contractsourcesystemname,
case
when left(gl_full_source_code,3) in ('903','851','444') then 'External'
end as reinsurancecounterpartytype,
case
when left(gl_full_source_code,3) in ('903','851','444') then orig_gl_center
end as sourcesystemreinsurancecounterpartycode,
random_counter,
old_error_message,
error_record_aging_days,
original_recorded_timestamp
from source_df
where len(error_message)=0
)
---


