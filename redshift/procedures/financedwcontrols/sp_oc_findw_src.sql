CREATE OR REPLACE PROCEDURE financdwcontrols.sp_oc_findw_src(domain_name varchar, hop_name varchar, srcsysname varchar)
LANGUAGE plpgsql
AS $$
DECLARE DomainNm varchar(100);
DECLARE MaxRowNum int;
DECLARE chvwMeasureType varchar(50);
DECLARE chvwMeasureField varchar(150);
DECLARE chvwMeasureField_temp varchar(150);
DECLARE DomainPhyEnv varchar(50);
DECLARE SysSysNm varchar(50);
DECLARE chvwSourceQry varchar(max);
DECLARE Counter INT;
DECLARE GroupByClause varchar(255);
DECLARE SrcTable varchar(100);
DECLARE WrkTable varchar(100);
DECLARE ttsrc varchar(max);
DECLARE aahsrc varchar(max);
DECLARE dsrc varchar(max);
DECLARE deriveddomainsrc varchar(max);
DECLARE chvwOperationType varchar(255);
DECLARE SummationType varchar(255);
DECLARE UnionClause varchar(max);
DECLARE wrnameasetype varchar(255);
DECLARE SrcSelectScript varchar(max);
DECLARE InsertScript varchar(max);
DECLARE gdqsourcetable varchar(255);
DECLARE dummy varchar(10);
DECLARE qry_control_log_id numeric(18,0);
DECLARE qry_ovrd varchar(max);
DECLARE qry_ovrd1 varchar(max);
DECLARE qry_ovrd2 varchar(max);
DECLARE qry_ovrd3 varchar(max);
DECLARE ttd_schema varchar(max);
DECLARE ttd_srcschema varchar(max);
DECLARE ttd_stschema varchar(max);
DECLARE ovrrd_db varchar(max) := 'source_system_name';
DECLARE ovrrd_tt varchar(max) := 'source_system_name';
DECLARE cc_flag VARCHAR(1) := 'N';
DECLARE wrkctlschema VARCHAR(MAX);
DECLARE staging_schema VARCHAR(MAX);
DECLARE stop_date_format VARCHAR(50);
BEGIN
DomainNm = domain_Name;
hop = 'oc_findw_src_' || hop_name;
CASE WHEN domain_name = 'party' and srcsysname = 'BaNCS'
THEN
domain_ovrrd = 'party_bancs';
WHEN domain_name = 'party' and srcsysname like 'VantageP%'
THEN
domain_ovrrd = 'party_vantage';
WHEN domain_name = 'contractpolfund' and srcsysname = 'BaNCS'
THEN
domain_ovrrd = 'contractpolfund_bancs';
WHEN domain_name = 'contractpolfund' and srcsysname like 'VantageP%'
THEN
domain_ovrrd = 'contractpolfund_vantage';
WHEN domain_name = 'partycontractrelationship' and srcsysname = 'Bestow'
THEN
domain_ovrrd = 'partycontractrelationship_bestow';
ELSE
domain_ovrrd = domain_name;
END CASE;
select top 1 decode(do_intraday_multibatch_run_flag,'Y','TIMESTAMP','DATE') into stop_date_format
from financedworchestration.domain_tracker a
where a.source_system_name = srcsysname
and a.domain_name = domain_name;
IF srcsysname = 'refdata'
THEN
qry_ovrd1 = a.source_system_name;
END IF;
IF srcsysname in ('refdata') and hop_name = 'staging'
THEN
qry_ovrd2 = source_system_name;
ELSIF domain_name not in ('aahjournalentrydetaildomain','financetransactionalaccountingview')
and (domain_name in ('finonecontract','contractparty','finoneactivity') and hop_name != 'staging')
THEN
qry_ovrd3 = 'SS_name';
END IF;
-- batch tracking call with in-progress status
CALL financedworchestration.sp_batch_tracking(srcsysname, hop, domain_name, 'in-progress', 'insert', '');
DROP TABLE IF EXISTS temp_current_batch;
BEGIN
CREATE TEMP TABLE temp_current_batch AS
(SELECT * FROM financedworchestration.current_batch
where a.source_system_name = srcsysname and a.domain_name = domain_name);
commit;
END;
SELECT load_pattern, target_schema into ld_pattern, tgt_schema
FROM financedworchestration.hash_definition a
WHERE a.domain_name = domain_ovrrd;
working_schema = tgt_schema = working;
staging_schema = tgt_schema = 'staging';
InsertScript = '
select count(*) from financdwcontrols.controllogging
where measure_src_tgt_adj_indicator in (''S'',''A'',''ST'')
and hop_name = ''' || hop_name || '''
and (batch_id, cycle_date, domain, sys_nm) in
(select distinct batch_id, cycle_date, domain, sys_nm
from temp_current_batch
where domain_name = ''' || domain_name || ''' and source_system_name = ''' || srcsysname || ''')';
BEGIN
SELECT 'X' INTO dummy;
COMMIT;
END;
BEGIN
EXECUTE InsertScript into Counter;
COMMIT;
END;
-- delete logging data if batch_id already exists
IF Counter > 0
THEN
InsertScript = 'N'
delete from financdwcontrols.controllogging
where measure_src_tgt_adj_indicator in (''S'',''A'',''ST'')
and hop_name = ''' || hop_name || '''
and (batch_id, cycle_date, domain, sys_nm) in
(select distinct batch_id, cycle_date, domain, sys_nm
from temp_current_batch
where domain_name = ''' || domain_name || ''' and source_system_name = ''' || srcsysname || ''')';
CALL financedworchestration.sp_dynamic_sql_log(srcsysname, domain_name, hop, REPLACE(InsertScript, '''', ''''), 'DeleteOldRecs');
BEGIN
SELECT 'X' INTO dummy;
COMMIT;
END;
END IF;
gdqsourcetable = 'gdq_' || lower(srcsysname) || domain_name;
if hop_name = 'staging' and srcsysname in ('aah','refdata') then
select distinct t.measure_table into SrcTable
from financdwcontrols.lkup_control_measures where domain = ''' || DomainNm || '''
and sys_nm = ''' || srcsysname || ''' and measure_table like 'dc%'' into SrcTable;
END if;
If SrcTable is null and DomainNm in ('financetransactionalaccountingview') then
SrcTable = case when srcsysname = 'aah' and DomainNm = 'financetransactionalaccountingview' and hop_name = 'staging' then 'financdw.aahjournalentrydetaileddomain'
when DomainNm = 'financetransactionalaccountingview' and hop_name = 'staging' then 'erpdw.subledgerjournallinedetail'
when hop_name = 'datawarehouse' then working_schema = 'wrk_' || lower(srcsysname) || '_' || domain_name
else
end;
ELSIF SrcTable is null and srcsysname = 'BaNCS' then
SrcTable = case when srcsysname = 'BaNCS' then 'financemasterdatastore.gdqdatastorage'
when hop_name = 'staging' then 'financemasterdatastore.gdqdatastorage_' || lower(srcsysname) || '_' || domain_name
when hop_name = 'working' then 'financemasterdatastore.gdqdatastorage_' || lower(srcsysname) || '_' || domain_name
when hop_name = 'datawarehouse' then working_schema = 'wrk_' || lower(srcsysname) || '_' || domain_name
else
end;
END IF;
if hop_name = 'working' then
WrkTable = working_schema || '.wrk_' || lower(srcsysname) || '_' || domain_name;
END IF;
if hop_name = 'staging' then
If domain_name IN ('general_ledger_header','general_ledger_line_item') THEN
SrcTable = replace(replace(SrcTable,'vantagep65psql','vantagep6p'),'p5','p');
END IF;
END IF;
If domain_name IN ('general_ledger_header','general_ledger_line_item') THEN
WrkTable = replace(replace(WrkTable,'vantagep65psql','vantagep6p'),'p5','p');
END IF;
DROP TABLE IF EXISTS srctemptab1;
CREATE TEMPORARY TABLE srctemptab1
(
    domain VARCHAR(255),
    operation VARCHAR(255),
    control_log_id VARCHAR(255),
    fact_col_name VARCHAR(255),
    group_col_1 VARCHAR(255),
    group_col_val VARCHAR(255),
    cycle_date DATE,
    source_system_name VARCHAR(255),
    batch_id VARCHAR(255),
    control_type VARCHAR(255),
    feed_id VARCHAR(255),
    src_tgt_adj VARCHAR(255)
);

if hop_name = 'staging' then
    qry_ovrd5 = '';
elseif hop_name = 'staging' and srcsysname in ('refdata') then
    qry_ovrd5 = 'sys_nm' and measure_table like 'dc%';
else
    qry_ovrd5 = '';
end if;

DROP TABLE IF EXISTS getmeasures;

Execute 'CREATE TEMPORARY TABLE getmeasures as (
    select domain as domainType, measure_name as measuretype, measure_field as measurefield, operation_type as operation_type,
    row_number() over(order by measure_name) as rownum, sys_nm as source_system
    from ( select distinct domain, measure_name, lower(measure_field) as measure_field, operation_type, sys_nm
           from financdwcontrols.lkup_control_measures
           where sys_nm = ''' || srcsysname || ''' and domain = ''' || DomainNm || '''
           and measure_src_tgt_adj_indicator like ' || qry_ovrd5 || '))';

if hop_name = 'staging' and ld_pattern = 'type2' then
    select cdc_flag into cc_flag
    from financdworchestration.domain_tracker a
    where a.source_system_name = srcsysname and a.domain_name = domain_name;
end if;

SELECT MAX(RowNum) into MaxRowNum FROM getmeasures;
Counter = 1;

while (Counter <= MaxRowNum) loop
    LOOP
        SELECT measuretype, lower(measurefield), domainType, operation_type, source_system
        from getmeasures where rownum=Counter
        into chvwMeasureType, chvwMeasureField, DomainTypeNm, chvwOperationType, SrcSysNm;

        IF chvwOperationType = 'SumTotaltoYear'
        THEN
            SummationType := 'sum(convert(bigint, coalesce(EXTRACT(YEAR FROM a.'||chvwMeasureField||'),0)))';
            data_type := 'int';
        ELSIF chvwOperationType = 'DistinctCount'
        THEN
            SummationType := 'count(distinct(a.'||chvwMeasureField||'))';
            data_type := 'varchar';
        ELSE
            SummationType := 'coalesce(sum(a.'||chvwMeasureField||'::numeric(36,10)),0)';
        END IF;

        varmeasuretype := case when chvwMeasureType = 'BALANCING_COUNT' then 'Count()'
                               when chvwMeasureType = 'CONTROL_TOTAL' then SummationType else '' end;

        GroupByClause := case
            when srcsysname = 'aah' and DomainTypeNm in ('financetransactionalaccountingview','aahjournalentrydetaileddomain') then
                ' group by a.batchid,a.cycledate,a.source_system_name,a.pointofviewstartdate,a.source_system_name'
            when srcsysname in ('refdata') and hop_name='staging' then
                ' group by a.batchid,a.cycledate,a.source_system_name'
            when hop_name = 'datapoint' then
                ' group by a.batchid,a.source_system_name,a.pointofviewstartdate as date'
            else
                ' group by a.batchid,a.source_system_name,a.pointofviewstartdate as date'
            end;

        UnionClause := CASE WHEN hop_name = 'working' and ld_pattern = 'type2' and stop_date_format = 'DATE'
            THEN
                ' union all
                select '''||DomainTypeNm||''' as Domain, '''||chvwOperationType||''' as Operation,
                '''||chvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
                '''||varmeasuretype||''' as fact_value,
                cast(a.POVstartdate as date) as cycle_date, a.source_system_name, a.batchid,
                '''||chvwMeasureType||''' as control_type, NULL as feed_id, ''SA'' as src_tgt_adj,
                from ( select '''||qry_ovrd3||''' as source_system_name, a.batch_id, a.pointofviewstartdate as POVstartdate, a.'||chvwMeasureField||','''||qry_ovrd4||''' as SS_NAME
                       from temp_current_batch cb
                       join cb.domain_name = '''||domain_name||''') as a'
                || GroupByClause || ' Target' ;
            WHEN hop_name = 'working' and ld_pattern = 'type2' and stop_date_format = 'TIMESTAMP'
            THEN
                ' union all
                select '''||DomainTypeNm||''' as Domain, '''||chvwOperationType||''' as Operation,
                '''||chvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
                '''||varmeasuretype||''' as fact_value,
                cast(a.POVstartdate as date) as cycle_date, a.source_system_name, a.batchid,
                '''||chvwMeasureType||''' as control_type, NULL as feed_id, ''SA'' as src_tgt_adj,
                from ( select '''||qry_ovrd3||''' as source_system_name, a.batch_id, a.pointofviewstartdate as POVstartdate, a.'||chvwMeasureField||','''||qry_ovrd4||''' as SS_NAME
                       from temp_current_batch cb
                       join cb.domain_name = '''||domain_name||''') as a'
                || GroupByClause || ' Target';
            WHEN hop_name = 'datawarehouse' and ld_pattern = 'type2'
            THEN
                ' union all
                select '''||DomainTypeNm||''' as Domain, '''||chvwOperationType||''' as Operation,
                '''||chvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
                '''||varmeasuretype||''' as fact_value,
                cast(a.POVstartdate as date) as cycle_date, a.source_system_name, a.batchid,
                '''||chvwMeasureType||''' as control_type, NULL as feed_id, ''SA'' as src_tgt_adj,
                from ( select '''||qry_ovrd3||''' as source_system_name, a.batch_id, a.pointofviewstartdate as POVstartdate, a.'||chvwMeasureField||','''||qry_ovrd4||''' as SS_NAME
                       from temp_current_batch cb
                       join cb.domain_name = '''||domain_name||''') as a'
                || GroupByClause || ' Target';
            ELSE ''
        END;

        FromClause := CASE WHEN hop_name='datawarehouse' and ld_pattern='type2' and stop_date_format='DATE'
            THEN
                ' ( select batchid, pointofviewstartdate as POVstartdate, sourcesystemname,'||chvwMeasureField||', a.source_system_name as SS_NAME
                    from (Select * from '||SrcTable||') a
                    where (batchid,source_system_name) in (select distinct batch_id,source_system_name from temp_current_batch where source_system_name='''||srcsysname||''' and domain_name='''||domain_name||'''))'
            WHEN hop_name='datawarehouse' and ld_pattern='type2' and stop_date_format='TIMESTAMP'
            THEN
                ' ( select batchid, pointofviewstartdate as POVstartdate, sourcesystemname,'||chvwMeasureField||', a.source_system_name as SS_NAME
                    from (Select * from '||SrcTable||') a
                    where (batchid,source_system_name) in (select distinct batch_id,source_system_name from temp_current_batch where source_system_name='''||srcsysname||''' and domain_name='''||domain_name||'''))'
            ELSE ''
        END;

        deriveddomainsrc = CASE WHEN DomainTypeNm = 'financeactivity' AND (srcsysname = 'BAICS' or srcsysname like 'VantagePK%') AND hop_name = 'staging' THEN
'(select * , decode('||chvwMeasureField||','reccount',null,'||chvwMeasureField||') , a.batchid,
cb.cycle_date POVstartdate, a.sourcesystemname as SS_NAME
from temp_current_batch cb
join financedwh.activity a on cb.batch_id=a.batchid and cb.cycle_date=a.cycledate
and a.sourcesystemname = cb.source_system_name and a.domain_name = '||DomainTypeNm||'
and a.source_system_name = '''||srcsysname||''' and activityamounttype='Forecast Premium')
group by a.batchid, a.POVstartdate, SS_NAME'

WHEN DomainTypeNm = 'financecontract' AND srcsysname not like 'VantageK%' and srcsysname = 'BAICS' AND hop_name = 'staging' THEN
'(select * , decode('||chvwMeasureField||','reccount',null,'||chvwMeasureField||') , cb.batch_id as batchid,
cb.cycle_date POVstartdate, a.sourcesystemname as SS_NAME
from temp_current_batch cb
join financedwh.contract a on cb.cycle_date between a.pointofviewstartdate and a.pointofviewstopdate
and a.source_system_name = cb.source_system_name and a.domain_name = '||DomainTypeNm||'
and a.documentid in (
select a.documentid
from temp_current_batch cb
join financedwh.contract a on cb.batch_id=a.batchid
and cb.source_system_name = a.source_system_name
where domain_name='''||DomainTypeNm||''' and cb.source_system_name = '''||srcsysname||''')
union
select a.documentid
from temp_current_batch cb
join financedwh.partycontractrelationship a on cb.batch_id = a.batchid
and cb.source_system_name = a.source_system_name
where domain_name='''||DomainTypeNm||''' and cb.source_system_name = '''||srcsysname||'''
union
(select cp.fkcontractdocumentid
from temp_current_batch cb
join financedwh.partycontractrelationship cp on cp.source_system_name = cb.source_system_name
and cb.cycle_date between cp.pointofviewstartdate and cp.pointofviewstopdate
and cb.relationshtype = 'Primary Insured'
where cb.domain_name = '''||DomainTypeNm||''' and cb.source_system_name = '''||srcsysname||'''
and (cp.fkpartycontractid in (
select a.documentid
from temp_current_batch cb
join financedwh.party a on cb.batchid = a.batchid and a.source_system_name = cb.source_system_name
where domain_name='''||DomainTypeNm||''' and cb.source_system_name = '''||srcsysname||''')))
group by a.batchid, a.POVstartdate, SS_NAME'

WHEN DomainTypeNm in ('contractparty','financecontract','financeactivity') AND (srcsysname = 'BAICS' or srcsysname like 'VantagePK%') AND hop_name = 'staging' THEN
'(select * , decode('||chvwMeasureField||','reccount',null,'||chvwMeasureField||') , cb.batch_id as batchid,
cb.cycle_date POVstartdate, a.sourcesystemname as SS_NAME
from temp_current_batch cb
join financedwh.contract a on cb.cycle_date between a.pointofviewstartdate and a.pointofviewstopdate
and a.source_system_name = cb.source_system_name
and cb.source_system_name = '''||srcsysname||'''
and a.documentid in (select a.documentid
from temp_current_batch cb
join financedwh.contract a on cb.batch_id=a.batchid
and cb.source_system_name = a.source_system_name
where domain_name='''||DomainTypeNm||''' and cb.source_system_name = '''||srcsysname||''')
union
select cp.fkcontractdocumentid
from temp_current_batch cb
join financedwh.partycontractrelationship cp on cp.source_system_name = cb.source_system_name
and cb.cycle_date between cp.pointofviewstartdate and cp.pointofviewstopdate
where cb.domain_name = '''||DomainTypeNm||''' and cb.source_system_name = '''||srcsysname||'''
and (cp.fkpartycontractid in (
select a.documentid
from temp_current_batch cb
join financedwh.party a on cb.batchid = a.batchid and a.source_system_name = cb.source_system_name
where domain_name='''||DomainTypeNm||''' and cb.source_system_name = '''||srcsysname||''')))
group by a.batchid, a.POVstartdate, SS_NAME'
ELSE '' END;

aahsrc = case when hop_name = 'staging' and DomainTypeNm = 'aahjournalentrydetaileddomain'
then '(SELECT '||chvwMeasureField||',sourcesystemname,batchid,cycletdate,sourcesystembatchidentifier
FROM ( SELECT amount as activityamount , null as reccount
, source_system_nm as sourcesystemname , batchid , feed_id as sourcesystembatchidentifier
, load_date as cycledate, accounting_date as activityreporteddate
FROM financedwstage.dc.aahsvc_jrnlentry_detail
WHERE cast(batchid as bigint) in ( select distinct batch_id from temp_current_batch
where domain_name = '''||DomainNm||''' and source_system_name='''||srcsysname||''')
) DC'
when hop_name in ('working','datawarehouse') and DomainTypeNm = 'aahjournalentrydetaileddomain' then 'SrcTable' end;

ftavsrc = case when hop_name = 'staging' and srcsysname = 'aah' and DomainTypeNm = 'financetransactionalaccountingview'
then '(select cast(AAH.activityamount as numeric(18,6)) as activityamount,
AAH.pointofviewstartdate,AAH.pointofviewstopdate,AAH.pointofviewstartdate as cycledate
, AAH.batchid,AAH.aahjournalentrydetailsourcesystem as sourcesystemname,null as reccount
from financedw.aahjournalentrydetaileddomain AAH
where cast(AAH.batchid as bigint) in (select distinct batch_id from temp_current_batch
where domain_name='''||DomainNm||''' and source_system_name='''||srcsysname||''')
and lower(AAH.aahjournalentrydetailsaccountingbasis)='cash')'
when hop_name = 'staging' and srcsysname = 'aah' and DomainTypeNm = 'financetransactionalaccountingview'
then '(select cast(generalledgertransactioncurrencyamount as numeric(18,6)) as activityamount
, cycletdate as pointofviewstartdate , 9999-12-31::DATE as pointofviewstopdate, cycledate
, sjd.batchid,sjd.sourcesystemname,null as reccount
from erpdw.subledgerjournaldetail sjd
where (batchid, cycletdate) in (select distinct batch_id, cycle_date from temp_current_batch
where domain_name='''||DomainTypeNm||''' and source_system_name='''||srcsysname||''')
and sjd.sourcesystemname = ''LIFG''
and sjd.source_system_name = '''||srcsysname||''')'
when hop_name in ('working','datawarehouse') and DomainTypeNm = 'financetransactionalaccountingview' then '(select * from '||SrcTable||') a' end;

chvwMeasureField_temp := chvwMeasureField;
if hop_name = 'staging' and srcsysname in ('refdata') and trim(chvwMeasureType) = 'CONTROL_TOTAL' then
execute 'select distinct lower(measure_field) from financedtcontrols.lkup_control_measures where domain = '''||DomainNm||'''
and sys_nm = '''||srcsysname||''' and measure_src_tgt_adj_indicator = ''S'' and measure_table like ''dc%''' into chvwMeasureField_temp;
execute 'select distinct lower(measure_field) from financedtcontrols.lkup_control_measures where domain = '''||DomainNm||'''
and sys_nm = '''||srcsysname||''' and measure_src_tgt_adj_indicator = ''R''' into chvwMeasureField;
end if;

dcsrcc = case when hop_name = 'staging' and srcsysname in ('refdata') and trim(chvwMeasureType) = 'BALANCING_COUNT'
then '(SELECT '||chvwMeasureField_temp||', source_system_name, batchid, cycletdate as POVstartdate
FROM ( SELECT ''refdata'' as source_system_name, batchid
, load_date as cycletdate, null reccount
FROM '||SrcTable||'
WHERE cast(batchid as bigint) in ( select distinct batch_id from temp_current_batch
where domain_name='''||DomainNm||''' and source_system_name='''||srcsysname||''')
) DC'
when hop_name = 'staging' and srcsysname in ('refdata') and trim(chvwMeasureType) = 'CONTROL_TOTAL'
then '(SELECT '||chvwMeasureField_temp||', source_system_name, batchid, cycletdate as POVstartdate
FROM ( SELECT ''refdata'' as source_system_name, batchid
, load_date as cycletdate, null reccount
FROM '||SrcTable||'
WHERE cast(batchid as bigint) in ( select distinct batch_id from temp_current_batch
where domain_name='''||DomainNm||''' and source_system_name='''||srcsysname||''')
) DC'
when hop_name in ('working','datawarehouse') and srcsysname in ('refdata')
then 'SrcTable' end;

SrcSelectScript= case when srcsysname = 'aah' and DomainTypeNm in ('aahjournalentrydetaileddomain','financetransactionalaccountingview') then
'INSERT INTO srctemptab1
select '''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
'''||varmeasurestype||''' as fact_value,
cast(cycledate as date) as cycle_date,
'''||srcsysname||''' as source_system_name ,a.batchid,
'''||ChvwMeasureType||''' as control_type, sourcesystembatchidentifier as feed_id,''S'' as src_tgt_adj'
when DomainTypeNm in ('financetransactionalaccountingview') then
'INSERT INTO srctemptab1
select '''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
'''||varmeasurestype||''' as fact_value,
cast(a.POVstartdate as date) as cycle_date , a.batchid,
'''||qry_ovrd3||''' as source_system_name, NULL as feed_id,''S'' as src_tgt_adj'
else
'INSERT INTO srctemptab1
select '''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
'''||varmeasurestype||''' as fact_value,
cast(a.POVstartdate as Date) as cycle_date, a.batchid,
'''||qry_ovrd3||''' as source_systemname , NULL as feed_id,''S'' as src_tgt_adj'
END;

-- AAH & FTAV Section

IF DomainTypeNm = 'aahjournalentrydetaileddomain' THEN
chvwSourceQry := SrcSelectScript || aahsrc || GroupByClause;
ELSIF DomainTypeNm = 'financetransactionalaccountingview' THEN
chvwSourceQry := SrcSelectScript || ftavsrc || GroupByClause;
ELSIF srcsysname in ('refdata') and hop_name = 'staging' THEN
chvwSourceQry := SrcSelectScript || dcsrcc || GroupByClause;
ELSIF DomainTypeNm in ('financeactivity','financecontract','contractparty') and hop_name='staging' THEN
chvwSourceQry := SrcSelectScript || deriveddomainsrc || GroupByClause;
ELSIF upper(trim(chvwMeasureType)) in ('CONTROL_TOTAL','BALANCING_COUNT')
and hop_name in ('edattranspose') THEN
chvwSourceQry := SrcSelectScript || FromClause || GroupByClause || UnionClause;
ELSIF ltrim(rtrim(chvwMeasureType)) = 'CONTROL_TOTAL' and hop_name = 'gdtranspose' THEN
chvwSourceQry :=
'INSERT INTO srctemptab1
select '''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
'''||ChvwMeasureField||''' as control_id,
'''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
'''||SummationType||''' as fact_value, cast(cycledate as date) as cycledate,a.batchid,
'''||qry_ovrd3||''' as sourcesystemname,''S'' as src_tgt_adj, '''||data_type||''' as ChvwMeasureField
FROM (
SELECT cycledate,batchid,sourcesystemname,
naturakey,cdekay, cast(NULLIF(COALESCE(col_o,col_t,col_s,'')) AS '||data_type||') AS '||ChvwMeasureField||'
FROM (
SELECT naturakey,cdekay, cast(load_date as date) as cycledate,cast(batchid as bigint) as batchid, sourcesystem as sourcesystemname,
MAX(case when lower(edqcolumnname)='''||ChvwMeasureField||''' and lower(transformtype)=''source'' then cdevalue else null end) as col_s,
MAX(case when lower(edqcolumnname)='''||ChvwMeasureField||''' and lower(transformtype)=''transform'' then cdevalue else null end) as col_t,
MAX(case when lower(edqcolumnname)='''||ChvwMeasureField||''' and lower(transformtype)=''override'' then cdevalue else null end) as col_o
-- For Transpose Layer
ELSIF ltrim(rtrim(chvwMeasureType)) = 'CONTROL_TOTAL' AND hop_name = 'gdtranspose' THEN
    chvwSourceQry :=
    'INSERT INTO srctemptab1
     select '''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
     '''||ChvwMeasureField||''' as control_id,
     '''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
     '''||SummationType||''' as fact_value, cast(a.pointofviewstartdate as date) as cycle_date, a.sourcesystemname, a.batchid,
     '''||ChvwMeasureType||''' as control_type, NULL as feed_id, ''S'' as src_tgt_adj,
     from ( SELECT cycledate as pointofviewstartdate, batchid, sourcesystemname,
     naturakey, cdekey, cast(NULLIF(COALESCE(col_o,col_t,col_s,'')) AS '||data_type||') AS '||ChvwMeasureField||'
     FROM (
        SELECT naturakey, cdekey, cast(load_date as date) as cycledate, cast(batchid as bigint) as batchid, sourcesystem as sourcesystemname,
        MAX(case when lower(edqcolumnname)= '''||ChvwMeasureField||''' and lower(transformtype)=''source'' then cdevalue else null end) as col_s,
        MAX(case when lower(edqcolumnname)= '''||ChvwMeasureField||''' and lower(transformtype)=''transform'' then cdevalue else null end) as col_t,
        MAX(case when lower(edqcolumnname)= '''||ChvwMeasureField||''' and lower(transformtype)=''override'' then cdevalue else null end) as col_o
        FROM '||SrcTable||'
        WHERE lower(tablename) in ('''||gdqsourceTable||''')
        AND batchid in (select distinct batch_id from temp_current_batch cb where source_system_name='''||srcsysname||'''
        and domain_name='''||domain_name||''')
        GROUP BY naturakey,cdekey, load_date,batchid ,sourcesystem
     ) a )
     ' || GroupByClause || '';

ELSIF ltrim(rtrim(chvwMeasureType)) = 'BALANCING_COUNT' AND hop_name = 'gdtranspose' THEN
    chvwSourceQry :=
    'INSERT INTO srctemptab1
     select '''||DomainTypeNm||''' as Domain, '''||ChvwOperationType||''' as Operation,
     '''||ChvwMeasureField||''' as control_id,
     '''||ChvwMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
     '''||SummationType||''' as fact_value,
     cast(a.pointofviewstartdate as date) as cycle_date, a.sourcesystemname, a.batchid,
     '''||ChvwMeasureType||''' as control_type, NULL as feed_id, ''S'' as src_tgt_adj,
     from ( SELECT cycledate as pointofviewstartdate, batchid, sourcesystemname
     FROM (
        SELECT naturakey, cdekey, cast(load_date as date) as cycledate, cast(batchid as bigint) as batchid, sourcesystem as sourcesystemname
        FROM '||SrcTable||'
        WHERE lower(tablename) in ('''||gdqsourceTable||''')
        AND batchid in (select distinct batch_id from temp_current_batch cb
        where source_system_name='''||srcsysname||'''
        and domain_name='''||domain_name||''')
        GROUP BY naturakey, cdekey, load_date, batchid, sourcesystem
     ) a )
     ' || GroupByClause || '';
END IF;

-- Updating the DynamicSQL table
CALL financedworchestration.sp_dynamic_sql_log(srcsysname, domain_name, hop, REPLACE(chvwSourceQry, '''', ''''''), 'chvwSourceQry');
BEGIN
    EXECUTE(chvwSourceQry);
    COMMIT;
END;

Counter = Counter + 1;
END LOOP;  -- Inner while loop end

IF DomainTypeNm in ('financeactivity','financecontract','contractparty') AND hop_name = 'staging' THEN
    qry_ovrd2 = decode(DomainTypeNm, 'financeactivity','activity','contract');
ELSE
    qry_ovrd2 = SUBSTRING(''||SrcTable||'', charindex(''||SrcTable||'',1),LEN(''||SrcTable||''));
END IF;

-- Insert Logging data from temp table
InsertScript :=
'INSERT INTO financedwcontrols.controllogging
(batch_id, sys_nm, measure_name, measure_value,
 measure_value_datatype, measure_table, measure_table_description,
 measure_src_tgt_adj_indicator, hop_name, cycle_date, domain, measure_field, p.sys_nm, feed_id)
select distinct CAST(a.batch_id as bigint), cb.source_system_name as sys_nm,
 b.measure_name, CAST(a.fact_val AS DECIMAL(18,10)), b.measure_value_datatype,
 '''||qry_ovrd2||''' AS measure_table,
 initcap(split_part(b.measure_name,'':'', 2)) || ''_'' || initcap(b.domain) || ''_'' ||
 CASE WHEN b.sys_nm='''||srcsysname||''' THEN
 CASE WHEN '''||hop_name||'''='''||hop_name||''' THEN ''Src'' when ''Tt'' then ''Tt'' when ''TtAdj'' then ''TtAdj''
 ELSE ''Tt_Ignore'' end else ''Src_Adj'' end as measure_table_description,
 a.src_tgt_adj, '''||hop_name||''', a.cycle_date, b.domain, b.measure_field, b.sys_nm, a.feed_id
from srctemptab1 a
inner join temp_current_batch cb
on cb.batch_id = a.batchid and cb.source_system_name = '''||srcsysname||'''
and cb.domain_name = '''||domain_name||'''
inner join financedwcontrols.lkup_control_measures b
on b.domain = a.domain and b.sys_nm = '''||srcsysname||'''
and control_type = b.measure_name
and b.measure_src_tgt_adj_indicator in (''S'');';

-- Updating Dynamic SQL log for InsertScript
CALL financedworchestration.sp_dynamic_sql_log(srcsysname, domain_name, hop, REPLACE(InsertScript, '''', ''''''), 'InsertScript');

BEGIN
    SELECT 'X' INTO dummy;
    COMMIT;
END;

BEGIN
    --LOCK TABLE financedwcontrols.controllogging;
    EXECUTE(InsertScript);
    COMMIT;
END;

-- Batch tracking call with successful status
CALL financedworchestration.sp_batch_tracking(srcsysname, hop, domain_name, 'complete', 'update', '+');

EXCEPTION
WHEN OTHERS THEN
    -- Batch tracking call with failed status
    CALL financedworchestration.sp_batch_tracking(srcsysname, hop, domain_name, 'failed', 'update', SQLERRM);
    RAISE INFO 'error message SQLERRM %', SQLERRM;
    RAISE INFO 'error message SQLSTATE %', SQLSTATE;

