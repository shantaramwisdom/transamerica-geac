CREATE OR REPLACE PROCEDURE financedorchestration.sp_financedw_append(source_system_name varchar,domain_name varchar)
LANGUAGE plpgsql
AS $$
DECLARE
    working_columns VARCHAR(MAX);
    update_qry VARCHAR(MAX);
    integer_var BIGINT;
    insert_qry VARCHAR(MAX);
    delete_qry VARCHAR(MAX);
    delete_cont integer;
    target_table VARCHAR := LOWER(domain_name);
    target_schema VARCHAR;
    working_table VARCHAR := 'wrk.'||LOWER(source_system_name)||'_'||LOWER(domain_name);
    working_schema VARCHAR(MAX);
    dummy VARCHAR;
    delete_where_cond VARCHAR(MAX);
    delete_where_cond2 VARCHAR(MAX);
    domain_ovrrd VARCHAR := domain_name;
    kc2_cnt integer;

BEGIN

    if domain_name not in ('general_ledger_header','general_ledger_line_item') then
        working_table := replace(replace(working_table,'vantagp65spl','spl'),'vantagp','p');
    end if;

    --Inserting the new record to Batch Tracking
    CALL financedorchestration.sp_batch_tracking(source_system_name,'datawarehouse',domain_name,'in-progress','insert','');
    delete_where_cond := 'batchid,source_system_name,cycledate';
    delete_where_cond2 := 'batch_id,source_system_name,cycle_date';

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

    working_schema := target_schema||'.working';

    --Delete existing data in rerun scenario
    delete_qry := 'delete from '||target_schema||'.'||target_table||
                  ' where ('||delete_where_cond||') in (select distinct '||delete_where_cond2||' from financedorchestration.current_batch
                  where domain_name = '''||domain_name||''' and source_system_name = '''||source_system_name||''')';

    EXECUTE 'select count(*) from '||target_schema||'.'||target_table||
            ' where ('||delete_where_cond||') in (select distinct '||delete_where_cond2||' from financedorchestration.current_batch
             where domain_name = '''||domain_name||''' and source_system_name = '''||source_system_name||''')'
             INTO delete_count;

    if delete_count > 0 then
        CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'datawarehouse',REPLACE(delete_qry,'''','""""'),'DeleteOldRecs');
        BEGIN
            SELECT 'X' INTO dummy;
            COMMIT;
        END;
        BEGIN
            EXECUTE 'LOCK '||target_schema||'.'||target_table;
            EXECUTE delete_qry;
            COMMIT;
        END;
    end if;

    --To get the list of CDE
    SELECT
        LISTAGG(distinct CAST(attname AS VARCHAR(MAX)), ',')
        WITHIN GROUP (ORDER BY attname DESC)
        INTO working_columns
    FROM pg_attribute a, pg_namespace ns, pg_class c, stv_tbl_perm p, pg_database db
    WHERE a.attrelid = c.oid AND a.attrelid = p.id AND c.relnamespace = ns.oid AND db.oid = p.db_id AND c.oid = a.attrelid
      AND nspname = working_schema
      AND TRIM(relname) = working_table
      AND attname NOT IN ('actiontype','sourcetable','inserttimestamp','updatetimestamp')
    GROUP BY relname;

    if working_columns not like '%activity105exchangeindicator%' then
        working_columns := REPLACE(working_columns,'105exchangeindicator','105exchangeindicator');
    end if;

    --Insert the data to Datamart from working tables with action_type=insert
    insert_qry := 'INSERT INTO '||target_schema||'.'||target_table||
                   '('||working_columns||') SELECT '||working_columns||' FROM '||working_schema||'.'||working_table||
                   ' WHERE actiontype=''INSERT'' ';

    --Updated the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'datawarehouse',REPLACE(insert_qry,'''','""""'),'append_query');

    --Execute Query
    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE insert_qry;
        COMMIT;
    END;

    --Controls execution
    CALL financecontrols.sp_qc_find_src(domain_name,'datawarehouse',source_system_name);
    CALL financecontrols.sp_qc_find_tgt(domain_name,'datawarehouse',source_system_name);
    CALL financecontrols.sp_qc_find_controlresults(domain_name,'datawarehouse',source_system_name);

    --KC Controls Execution
    EXECUTE 'select count(*) from financecontrols.lkp_kc2_control_measures
             where source_system_name = '''||source_system_name||'''
             and domain_name = '''||domain_name||''' and is_active = ''Y'' and key_control_type = ''kc2'''
             INTO kc2_cnt;

    IF kc2_cnt > 0 THEN
        CALL financecontrols.sp_kc2_find_tgt(source_system_name,domain_name,'datawarehouse');
        CALL financecontrols.sp_kc2_find_controlresults(source_system_name,domain_name,'datawarehouse');
    END IF;

    --Mark batch completion
    CALL financedorchestration.sp_batch_tracking(source_system_name,'datawarehouse',domain_name,'complete','update','');

EXCEPTION
    WHEN OTHERS THEN
        CALL financedorchestration.sp_batch_tracking(source_system_name,'datawarehouse',domain_name,'failed','update',SQLERR);
        RAISE INFO 'error message SQLERR %', SQLERR;
        RAISE INFO 'error message SQLSTATE %', SQLSTATE;
END;

$$
