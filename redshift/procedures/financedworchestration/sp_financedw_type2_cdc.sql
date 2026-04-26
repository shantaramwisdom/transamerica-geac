CREATE OR REPLACE PROCEDURE financedorchestration.sp_financedw_type2_cdc(source_system_name varchar, domain_name varchar)
LANGUAGE plpgsql
AS $$
--Declare Local Variables
DECLARE working_columns VARCHAR(MAX);
DECLARE working_columns_temp VARCHAR(MAX);
DECLARE update_qry VARCHAR(MAX);
DECLARE update_revert_qry VARCHAR(MAX);
DECLARE update_revert_qry2 VARCHAR(MAX);
DECLARE insert_qry VARCHAR(MAX);
DECLARE delete_rebuild_qry VARCHAR(MAX);
DECLARE delete_rebuild_qry2 VARCHAR(MAX);
DECLARE delete_count integer;
DECLARE find_cnt integer;
DECLARE wrk_cnt integer;
DECLARE target_table VARCHAR;
DECLARE target_schema VARCHAR;
DECLARE working_table VARCHAR := 'wrk.'||LOWER(source_system_name)||'_'||LOWER(domain_name);
DECLARE working_schema VARCHAR(MAX);
DECLARE dummy varchar(10);
DECLARE qry_ovrrd VARCHAR(MAX) := ' where source_system_name = '''||source_system_name||'''';
DECLARE qry_ovrrd1 VARCHAR(MAX);
DECLARE kc2_cnt integer;
DECLARE domain_ovrrd VARCHAR(MAX) := domain_name;

BEGIN

    working_table := replace(replace(working_table,'vantagp65spl','spl'),'vantagp','p');

    if domain_name in ('financencontract','contractparty') and source_system_name like 'VantageP%'
    then
        qry_ovrrd1 := ' WHERE domain = ''contract''';
    else
        qry_ovrrd1 := ' WHERE domain = '''||domain_name||'''';
    end if;

    --Inserting the new record to Batch Tracking
    CALL financedorchestration.sp_batch_tracking(source_system_name,'datawarehouse',domain_name,'in-progress','insert','');

    CASE WHEN domain_name = 'party' and source_system_name = 'BaNCS'
        THEN
            domain_ovrrd := 'party_bancs';
        WHEN domain_name = 'party' and source_system_name like 'VantageP%'
        THEN
            domain_ovrrd := 'party_vantage';
        WHEN domain_name = 'contractoptionfund' and source_system_name = 'BaNCS'
        THEN
            domain_ovrrd := 'contractoptionfund_bancs';
        WHEN domain_name = 'contractoptionfund' and source_system_name like 'VantageP%'
        THEN
            domain_ovrrd := 'contractoptionfund_vantage';
        WHEN domain_name = 'partycontractrelationship' and source_system_name = 'Bestow'
        THEN
            domain_ovrrd := 'partycontractrelationship_bestow';
        ELSE
            domain_ovrrd := domain_name;
    END CASE;

    SELECT target_schema
    INTO target_schema
    FROM financedorchestration.hash_definition hash
    WHERE hash.domain_name = domain_ovrrd;

    target_table := target_schema||'.'||LOWER(domain_name);
    working_schema := target_schema||'.working';

    DROP TABLE IF EXISTS temp_current_batch;

BEGIN
    CREATE TEMP TABLE temp_current_batch AS
    (SELECT * FROM financedorchestration.current_batch a
      where a.source_system_name = source_system_name and a.domain_name = domain_name);
    commit;
END;

--To get the list of CDE
SELECT
    LISTAGG(distinct ''''||attname||'''',', ')
    WITHIN GROUP (ORDER BY attname DESC)
    INTO working_columns
FROM pg_attribute a, pg_namespace ns, pg_class c, pg_type t, stv_tbl_perm p, pg_database db
WHERE t.oid=a.atttypid AND a.attrelid=p.id AND a.attrelid=c.oid AND c.relnamespace=ns.oid AND db.oid=p.db_id AND c.oid=a.attrelid
AND attname NOT IN ('deleted','insertid')
AND ns.nspname = working_schema
AND TRIM(relname) = working_table
AND attname NOT IN ('actiontype','sourcetable','inserttimestamp','updatetimestamp')
group BY relname;

BEGIN
    SELECT 'X' INTO dummy;
    COMMIT;
END;

BEGIN
    execute 'SELECT count(*) FROM '||working_schema||'.'||working_table||' w
             join '||target_schema||'.'||domain_name||' b using (naturalkeyhashvalue, pointofviewstartdate, pointofviewstopdate, batchid)
             WHERE w.actiontype = ''INSERT''' into find_cnt;
    COMMIT;
END;

execute 'SELECT count(*) FROM '||working_schema||'.'||working_table||' w WHERE w.actiontype = ''INSERT''' into wrk_cnt;

if find_cnt != wrk_cnt and find_cnt = 0
then
    working_columns_temp := working_columns;
    if domain_name = 'contractpayout' and source_system_name = 'BaNCS'
    then
        --Temporary fix for Adding #contractdocumentid in contractpayout
        working_columns_temp := working_columns||','||'financedorchestration.uuid5('||source_system_name||','||contractnumber||','||contractadministrationlocationcode||'_BaNCS'')';
        working_columns := working_columns||',#contractdocumentid';
    end if;

    --Insert the data to Datawarehouse from working tables with action_type=insert
    insert_qry := 'INSERT into '||target_schema||'.'||domain_name||' ('||working_columns||') SELECT '||
                  working_columns_temp||' FROM '||working_schema||'.'||working_table||
                  ' WHERE actiontype=''INSERT''';
elsif find_cnt = wrk_cnt
then
    RAISE EXCEPTION 'The Batch Already Loaded In FiNDW Domain. Please do the cleanups and reload';
else
    RAISE INFO 'No New Records will be Inserted Into the FiNDW Domain';
end if;

--Update the Datawarehouse Table point_of_view_stop_date from Working Table
--It will have start date 1 from staging
update_qry := 'UPDATE '||target_schema||'.'||domain_name||' t SET pointofviewstopdate = working.pointofviewstopdate,
updatetimestamp = (current_timestamp AT TIME ZONE ''US/Central'')
FROM '||target_schema||'.'||domain_name||' t,'||working_schema||'.'||working_table||' working
WHERE Target.naturalkeyhashvalue = working.naturalkeyhashvalue
AND Target.pointofviewstartdate = working.pointofviewstartdate
AND working.actiontype = ''UPDATE''
AND Target.pointofviewstopdate = ''9999-12-31 00:00:00.000'' ';

--Updated the DynamicSQL table
CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'datawarehouse',REPLACE(update_qry,'''','""""'),'type2_update_qry');

--Execute Query
--Locking target table to prevent conflicts while concurrently inserting and selecting from the warehouse table
--Potential conflicts could arise when there are users selecting the DW tables, on if another source system is running in parallel

BEGIN
    SELECT 'X' INTO dummy;
    COMMIT;
END;

BEGIN
    --EXECUTE 'LOCK '||target_table;
    EXECUTE update_qry;
    COMMIT;
END;

if insert_qry != ''
then
    --Update the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'datawarehouse',REPLACE(insert_qry,'''','""""'),'type2_insert_qry');

    --Execute Query
    --Locking target table to prevent conflicts while concurrently inserting and selecting from the warehouse table
    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        --EXECUTE 'LOCK '||target_table;
        EXECUTE insert_qry;
        COMMIT;
    END;
end if;

--QC Controls Execution
call financecontrols.sp_qc_find_src(domain_name,'datawarehouse',source_system_name);
call financecontrols.sp_qc_find_tgt(domain_name,'datawarehouse',source_system_name);
call financecontrols.sp_qc_find_controlresults(domain_name,'datawarehouse',source_system_name);

--KC Controls Execution
EXECUTE 'select count(*) from financecontrols.lkp_kc2_control_measures
         where source_system_name = '''||source_system_name||''' and is_active = ''Y'' and key_control_type = ''kc2'''
         INTO kc2_cnt;
IF kc2_cnt > 0
THEN
    CALL financecontrols.sp_kc2_find_tgt(source_system_name,domain_name,'datawarehouse');
    CALL financecontrols.sp_kc2_find_controlresults(source_system_name,domain_name,'datawarehouse');
END IF;

IF (domain_name in ('contractoption','contractoptionbenefit') and upper(source_system_name) in ('LTGG','LTGGHYBRID'))
   or (domain_name in ('contract','contractoptionfund','party','financencontract','contractparty') and source_system_name like 'VantageP%')
   or (domain_name in ('oracleworkerdetails') and source_system_name in ('sailpoint'))
THEN
    delete_count = 0;
    DROP TABLE IF EXISTS deleted_contracts;

    IF domain_name in ('financencontract','contractparty') and source_system_name like 'VantageP%'
    THEN
        qry_ovrrd := ' where source_system_name = '''||source_system_name||''' ';
        delete_rebuild_qry := 'create temp table deleted_contracts as (
                               with deletes as
                               (select contractnumber, contractadministrationlocationcode, documentid
                                from financedw_views.contract_historical '||qry_ovrrd||' and pointofviewstopdate != ''9999-12-31'')
                               minus
                               select contractnumber, contractadministrationlocationcode, documentid
                               from financedw_views.contract '||qry_ovrrd||'
                               )
                               select documentid, source_system_name, contractnumber, contractadministrationlocationcode,
                               new_stop_dt, pointofviewstartdate, pointofviewstopdate
                               from '||target_schema||'.'||domain_name||' e
                               join (select documentid, pointofviewstartdate cntrct_pointofviewstartdate, pointofviewstopdate as new_stop_dt,
                               row_number() over (partition by documentid order by pointofviewstopdate desc) as fltr
                               from financedw_views.contract_historical f
                               join deletes b using (documentid) '||qry_ovrrd||' )
                               where fltr = 1 and f.pointofviewstopdate != new_stop_dt
                               and f.source_system_name like ''VantageP%'' and f.pointofviewstopdate = ''9999-12-31'' ';
        delete_rebuild_qry2 := 'UPDATE '||target_schema||'.'||domain_name||' e SET
                                pointofviewstopdate = e.new_stop_dt,
                                updatetimestamp = current_timestamp AT TIME ZONE ''US/Central''
                                FROM '||target_schema||'.'||domain_name||' e, deleted_contracts E
                                WHERE e.naturalkeyhashvalue = E.documentid
                                AND e.pointofviewstopdate = E.pointofviewstopdate
                                AND e.pointofviewstartdate = E.pointofviewstartdate';
    ELSEIF (domain_name in ('contract','contractoption','contractoptionfund','party') and source_system_name like 'VantageP%')
       or (domain_name in ('contractoptionbenefit') and source_system_name in ('ltgg','ltgghybrid'))
       or (domain_name in ('oracleworkerdetails') and source_system_name in ('sailpoint'))
    THEN
        delete_rebuild_qry := 'create temp table deleted_contracts as
                               select e.naturalkeyhashvalue, c.pointofviewstopdate, c.pointofviewstartdate, e.pointofviewstopdate as new_stop_dt
                               from '||target_schema||'.'||domain_name||' e
                               join (select * from financedorchestration.current_batch a
                                     WHERE source_system_name = '''||source_system_name||''' and batch_frequency = ''DAILY'' and domain_name = '''||domain_name||''') c
                               on e.source_system_name = c.source_system_name and e.domain_name = c.domain_name ';
        delete_rebuild_qry2 := 'UPDATE '||target_schema||'.'||domain_name||' e SET
                                pointofviewstopdate = e.new_stop_dt,
                                updatetimestamp = current_timestamp AT TIME ZONE ''US/Central''
                                FROM '||target_schema||'.'||domain_name||' e, deleted_contracts E
                                WHERE e.naturalkeyhashvalue = E.naturalkeyhashvalue
                                AND e.pointofviewstopdate = E.pointofviewstopdate
                                AND e.pointofviewstartdate = E.pointofviewstartdate';
    END IF;

    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'datawarehouse',REPLACE(delete_rebuild_qry,'''','""""'),'type2_delete_rebuild_qry_identify');

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE delete_rebuild_qry;
        COMMIT;
    END;

    EXECUTE 'select count(*) from deleted_contracts' into delete_count;

    IF delete_count > 0
    THEN
        RAISE INFO 'Stopping Deleted Records in the FiNDW Domain';
        --Updated the DynamicSQL table
        CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'datawarehouse',REPLACE(delete_rebuild_qry2,'''','""""'),'type2_delete_rebuild_qry');

        --Locking target table to prevent conflicts while concurrently inserting and selecting from the warehouse table
        BEGIN
            SELECT 'X' INTO dummy;
            COMMIT;
        END;

        BEGIN
            --EXECUTE 'LOCK '||target_table;
            EXECUTE delete_rebuild_qry2;
            COMMIT;
        END;
    ELSE
        RAISE INFO 'The Delete Count was Zero so Skipped Updating Stop Date in Final Domain Table';
    END IF;
END IF;

--Inserting the new record to Batch Tracking
CALL financedorchestration.sp_batch_tracking(source_system_name,'datawarehouse',domain_name,'complete','update','');

EXCEPTION
    WHEN OTHERS THEN
        CALL financedorchestration.sp_batch_tracking(source_system_name,'datawarehouse',domain_name,'failed','update',SQLERR);
        RAISE INFO 'error message SQLERR %', SQLERR;
        RAISE INFO 'error message SQLSTATE %', SQLSTATE;
END;
$$
