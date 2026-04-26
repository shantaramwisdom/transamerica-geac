CREATE OR REPLACE PROCEDURE financedwcontrols.sp_oc_findw_tgt(domain_name varchar,hop_name varchar,srcsysname varchar)
LANGUAGE plpgsql
AS $$
DECLARE DomainNm Varchar(100);
DECLARE MaxRowNum int;
DECLARE chvwMeasureType varchar(50);
DECLARE chvwMeasureField Varchar(150);
DECLARE DomainTypeNm Varchar(100);
DECLARE chvwOperationType varchar(max);
DECLARE Counter int;
DECLARE GroupByClause Varchar(255);
DECLARE qry_ovrd2 varchar(255);
DECLARE SrcSchema varchar(max);
DECLARE TgtTable varchar(255);
DECLARE WrkTable varchar(255);
DECLARE WhereClause varchar(255);
DECLARE OperationType Varchar(255);
DECLARE SummationType Varchar(255);
DECLARE UnionClause varchar(max);
DECLARE FromClause varchar(max);
DECLARE varmeasuretype varchar(max);
DECLARE SelectScript varchar(max);
DECLARE InsertScript varchar(max);
DECLARE dummy varchar(10);
DECLARE ld_pattern varchar(max);
DECLARE tjt_schema varchar(max);
DECLARE stop_date_format VARCHAR(50);
DECLARE qry_ovrd varchar(255):='naturalkeyhashvalue';
DECLARE qry_ovrd3 varchar(255):='sourcesystemname';
DECLARE SrcTable varchar(255);
DECLARE SrcSchma varchar(max);
DECLARE wrktable varchar(max);
DECLARE domain_ovrd varchar(255);
DECLARE working_schema varchar(max);
DECLARE staging_schema varchar(max);
BEGIN
DomainNm := domain_name;
SrcSchema := 'financedw';
hop := 'oc_findw_tgt' || hop_name;
CASE WHEN domain_name = 'party' and srcsysname = 'BANCS'
THEN domain_ovrd := 'party_bancs';
WHEN domain_name = 'partycontractoptionfund' and srcsysname like 'VantagePK'
THEN domain_ovrd := 'partycontractoptionfund_vantage';
WHEN domain_name = 'partycontractrelationship' and srcsysname = 'Bestow'
THEN domain_ovrd := 'partycontractrelationship_bestow';
ELSE domain_ovrd := domain_name;
END CASE;

select top 1 decode(do_intraday_multibatch_run_flag, 'Y', 'TIMESTAMP', 'DATE') into stop_date_format
from financedworchestration.domain_tracker
where a.source_system_name = srcsysname and a.domain_name = domain_name;

SELECT load_pattern, target_schema INTO ld_pattern, tjt_schema
FROM financedworchestration.hash_definition a
WHERE a.domain_name = domain_ovrd;

working_schema := tjt_schema || '_working';
staging_schema := tjt_schema || '_staging';

IF domain_name = 'financeactivity' THEN
qry_ovrd := 'activityaccountingid';
ELSIF srcsysname = 'refdata' or tjt_schema = 'erpdw' THEN
qry_ovrd2 := 'source_system_name';
END IF;

-- batch tracking call with in-progress status
CALL financedworchestration.sp_batch_tracking(srcsysname,hop,domain_name,'in-progress','insert','+');

DROP TABLE IF EXISTS temp_current_batch;
BEGIN
CREATE TEMP TABLE temp_current_batch AS
(SELECT * FROM financedworchestration.current_batch a
where a.source_system_name = srcsysname and a.domain_name = domain_name);
commit;
END;

InsertScript := 'N'
select count(*) from financedwcontrols.controllogging
where measure_src_tgt_adj_indicator in ('T','TA')
and hop_name = '''||hop_name||'''
and (batch_id, cycle_date, domain, sys_nm) in
(select distinct batch_id, cycle_date, domain_name, source_system_name
from temp_current_batch
where domain_name = '''||domain_name||''' and source_system_name = '''||srcsysname||''')';
BEGIN
SELECT 'X' INTO dummy;
COMMIT;
END;
BEGIN
EXECUTE InsertScript into Counter;
COMMIT;
END;

IF Counter > 0
THEN
InsertScript := 'N'
delete from financedwcontrols.controllogging
where measure_src_tgt_adj_indicator in ('T','TA')
and hop_name = '''||hop_name||'''
and (batch_id, cycle_date, domain, sys_nm) in
(select distinct batch_id, cycle_date, domain_name, source_system_name
from temp_current_batch
where domain_name = '''||domain_name||''' and source_system_name = '''||srcsysname||''')';

CALL financedworchestration.sp_dynamic_sql_log(srcsysname,domain_name,hop,REPLACE(InsertScript,'''',''''''),'DeleteOldRecs');
BEGIN
SELECT 'X' INTO dummy;
COMMIT;
END;
BEGIN
--LOCK TABLE financedwcontrols.controllogging;
EXECUTE InsertScript;
COMMIT;
END;
END IF;

TgtTable = case when hop_name = 'gdtranspose' then 'financedwgdtranspose.' || lower(srcsysname) || '_' || domain_name
when hop_name = 'staging' then staging_schema || '.' || lower(srcsysname) || '_' || domain_name
when hop_name = 'working' then working_schema || '.' || lower(srcsysname) || '_' || domain_name
when hop_name = 'datawarehouse' then tjt_schema || '.' || domain_name
else '' end;

IF hop_name = 'datawarehouse' THEN
WrkTable = working_schema || '.' || lower(srcsysname) || '_' || domain_name;
END IF;

IF domain_name NOT IN ('general_ledger_header','general_ledger_line_item') THEN
TgtTable := replace(replace(TgtTable,'vantagegp5sp1','gp'),'vantagegp5sp1','p');
WrkTable := replace(replace(WrkTable,'vantagegp5sp1','gp'),'vantagegp5sp1','p');
END IF;

DROP TABLE IF EXISTS tgttab1;
CREATE TEMPORARY TABLE tgttab1
(
domain VARCHAR(255),
operation VARCHAR(255),
control_id VARCHAR(255),
fact_col_name VARCHAR(255),
group_col_name VARCHAR(255),
group_col_val VARCHAR(255),
fact_val VARCHAR,
cycle_date Date,
source_system_name VARCHAR(255),
batch_id VARCHAR(255),
control_type VARCHAR(255),
feed_id VARCHAR(255),
src_tgt_adj VARCHAR(255)
);

DROP TABLE IF EXISTS getmeasures;
CREATE TEMPORARY table getmeasures as (
select domain as domaintype,Measure_name as measuretype,Measure_field as measurefield,operation_type,
row_number() over(order by Measure_name) as rownum
from ( select distinct domain,Measure_name,lower(measure_field) as Measure_field,operation_type
from financedwcontrols.lkup_control_measures
where domain = DomainNm
and sys_nm = srcsysname and measure_src_tgt_adj_indicator = 'T'));

SELECT MAX(RowNum) into MaxRowNum FROM getmeasures;
Counter = 1;

while (Counter <= MaxRowNum)
LOOP
SELECT measuretype, measurefield, domaintype,operation_type
into chvwMeasureType, chvwMeasureField, DomainTypeNm,chvwOperationType
FROM getmeasures
WHERE rownum=Counter;

IF chvwOperationType = 'SumTotaltoYear' THEN
SummationType := 'sum(convert(bigint,coalesce(EXTRACT(YEAR FROM a.'||chvwMeasureField||'),0)))';
ELSIF chvwOperationType = 'DistinctCount' THEN
SummationType := 'count(distinct('||chvwMeasureField||'))';
ELSE
SummationType := 'coalesce(sum('||chvwMeasureField||'),0)';
END IF;

varmeasuretype := case when chvwMeasureType = 'BALANCING_COUNT' then 'Count()'
when chvwMeasureType = 'CONTROL_TOTAL' then SummationType else '' end;
GroupByClause :=
case when srcsysname = 'aah' and DomainTypeNm in ('financetransactionalaccountingview','aahjournalentrydetaileddomain') then
' group by a.batchid,a.pointofviewstartdate,coalesce(a.sourcesystembatchidentifier,'''') '
when srcsysname in ('refdata') or tgt_schema = 'erpdw' then
' group by a.batchid,cast(a.POVstartdate as date),a.source_system_name '
when DomainTypeNm in ('financetransactionalaccountingview') then
' group by a.batchid,a.pointofviewstartdate,a.source_system_name '
else
' group by a.batchid,cast(a.POVstartdate as date),a.sourcesystemname '
end;

WhereClause := case when hop_name = 'datawarehouse' and srcsysname not in ('financetransactionalaccountingview','aahjournalentrydetaileddomain')
then ' where (a.batchid,a.source_system_name) in (select distinct batch_id, source_system_name from temp_current_batch
where source_system_name = '''||srcsysname||''' and domain_name = '''||domain_name||''') '
when hop_name = 'datawarehouse' and DomainTypeNm in ('financetransactionalaccountingview','aahjournalentrydetaileddomain')
then 'where (a.batchid,a.source_system_name) in (select distinct batch_id, source_system_name from temp_current_batch
where source_system_name = '''||srcsysname||''' and domain_name = '''||domain_name||''') '
else '' end;

UnionClause := case when hop_name = 'datawarehouse' and ld_pattern = 'type2' and stop_date_format = 'DATE'
then 'union all
select
'''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name , NULL as group_col_val,
'''||SummationType||''' as fact_value,
Cast(a.POVstartdate as Date) as cycle_date,
'||qry_ovrd2||' as sourcesystemname,
a.batchid,
'''||ChvwMeasureType||''' as control_type,
NULL as sourcesystembatchidentifier,
''TA'' as src_tgt_adj
from (select cast(a.pointofviewstopdate as date) as POVstartdate,cb.batch_id as batchid,a.sourcesystemname '||qry_ovrd2||' a.'||ChvwMeasureField||'
from (select * , Null as recordcount from '||TgtTable||') a
join temp_current_batch cb on cast(a.pointofviewstopdate as date)=a.cycle_date and cb.source_system_name = '''||srcsysname||'''
and cb.domain_name = '''||domain_name||'''
join '||WrkTable||' wrk on wrk.ovrdr_cb = a.ovrdr_cb and wrk.source_system_name = a.source_system_name
and wrk.domain_name = a.domain_name and wrk.activitytype = ''DELETE''
where cast(wrk.pointofviewstopdate as date)!= cast(wrk.pointofviewstartdate as date)) a '
||GroupByClause||''
when hop_name = 'datawarehouse' and ld_pattern = 'type2' and stop_date_format = 'TIMESTAMP'
then 'union all
select
'''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name , NULL as group_col_val,
'''||SummationType||''' as fact_value,
Cast(a.POVstartdate as Date) as cycle_date,
'||qry_ovrd2||' as sourcesystemname,
a.batchid,
'''||ChvwMeasureType||''' as control_type,
NULL as sourcesystembatchidentifier,
''TA'' as src_tgt_adj
from (select DATEADD(minute,1,cast(a.pointofviewstopdate as date)) as POVstartdate,cb.batch_id as batchid,a.sourcesystemname '||qry_ovrd2||' a.'||ChvwMeasureField||'
from (select * , Null as recordcount from '||TgtTable||') a
join temp_current_batch cb on cast(a.pointofviewstopdate as date)=a.cycle_date and cb.source_system_name = '''||srcsysname||'''
and cb.domain_name = '''||domain_name||'''
join '||WrkTable||' wrk on wrk.ovrdr_cb = a.ovrdr_cb and wrk.source_system_name = a.source_system_name
and wrk.domain_name = a.domain_name and wrk.activitytype = ''DELETE''
where cast(wrk.pointofviewstopdate as date)!= cast(wrk.pointofviewstartdate as date)) a '
||GroupByClause||''
ELSE '' END;

fromClause := case when hop_name = 'working' and ld_pattern = 'type2' and stop_date_format = 'DATE'
then '(select batchid,pointofviewstartdate as POVstartdate, '||qry_ovrd2||' , '||ChvwMeasureField||'
from (select * , Null as recordcount from '||TgtTable||') a
join temp_current_batch cb on cast(a.pointofviewstopdate as date)=a.cycle_date and cb.source_system_name = '''||srcsysname||'''
and cb.domain_name = '''||domain_name||''') '
union all
select cb.batchid,a.pointofviewstopdate as POVstartdate,a.'||qry_ovrd2||' , a.'||ChvwMeasureField||'
from (select * , Null as recordcount from '||TgtTable||') a
join temp_current_batch cb on cast(a.pointofviewstopdate as date)=a.cycle_date and cb.source_system_name = '''||srcsysname||'''
and cb.domain_name = '''||domain_name||''') '
when hop_name = 'working' and ld_pattern = 'type2' and stop_date_format = 'TIMESTAMP'
then '(select batchid,pointofviewstartdate as POVstartdate, '||qry_ovrd2||' , '||ChvwMeasureField||'
from (select * , Null as recordcount from '||TgtTable||') a
join temp_current_batch cb on cast(a.pointofviewstopdate as date)=a.cycle_date and cb.source_system_name = '''||srcsysname||'''
and cb.domain_name = '''||domain_name||''') '
union all
select cb.batchid,DATEADD(minute,1,a.pointofviewstopdate) as POVstartdate,a.'||qry_ovrd2||' , a.'||ChvwMeasureField||'
from (select * , Null as recordcount from '||TgtTable||') a
join temp_current_batch cb on cast(a.pointofviewstopdate as date)=a.cycle_date and cb.source_system_name = '''||srcsysname||'''
and cb.domain_name = '''||domain_name||''') '
ELSE '' END;

TgtSelectScript := case when srcsysname = 'aah' and DomainTypeNm in ('financetransactionalaccountingview','aahjournalentrydetaileddomain') then
'INSERT INTO tgttab1
select
'''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name , NULL as group_col_name , NULL as group_col_val,
'''||varmeasuretype||''' as fact_value,
Cast(a.pointofviewstartdate as date) as cycle_date,
a.'||qry_ovrd2||' as sourcesystemname , a.batchid,
'''||ChvwMeasureType||''' as control_type,
coalesce(a.sourcesystembatchidentifier,'''') as sourcesystembatchidentifier
from a as src_tgt_adj'
when DomainTypeNm in ('financetransactionalaccountingview') then
'INSERT INTO tgttab1
select
'''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name , NULL as group_col_name , NULL as group_col_val,
'''||varmeasuretype||''' as fact_value,
Cast(a.pointofviewstartdate as date) as cycle_date,
a.sourcesystemname , a.batchid,
'''||ChvwMeasureType||''' as control_type,
NULL as sourcesystembatchidentifier
from a as src_tgt_adj'
ELSE
'INSERT INTO tgttab1
select
'''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name , NULL as group_col_name , NULL as group_col_val,
'''||varmeasuretype||''' as fact_value,
Cast(a.POVstartdate as date) as cycle_date,
a.'||qry_ovrd2||' , a.batchid,
'''||ChvwMeasureType||''' as control_type,
NULL as sourcesystembatchidentifier
from a as src_tgt_adj' END;
