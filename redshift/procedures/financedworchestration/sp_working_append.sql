CREATE OR REPLACE PROCEDURE financedorchestration.sp_working_append(source_system_name varchar,domain_name varchar)
LANGUAGE plpgsql
AS $$
--Declare Local Variables
DECLARE staging_columns VARCHAR(MAX);
DECLARE target_columns VARCHAR(MAX);
DECLARE target_columns_select VARCHAR(MAX);
DECLARE staging_qry VARCHAR(MAX);
DECLARE stage_table VARCHAR(MAX) := 'stg_'||LOWER(source_system_name)||'_'||LOWER(domain_name);
DECLARE target_table VARCHAR(MAX) := 'wrk_'||LOWER(source_system_name)||'_'||LOWER(domain_name);
DECLARE working_schema VARCHAR(MAX);
DECLARE staging_schema VARCHAR(MAX);
DECLARE domain_ovrrd VARCHAR(MAX) := domain_name;

BEGIN
    IF domain_name NOT IN ('general_ledger_header', 'general_ledger_line_item') THEN
        stage_table := replace(replace(stage_table,'vantagep55pl','spl'),'vantagep','p');
        target_table := replace(replace(target_table,'vantagep55pl','spl'),'vantagep','p');
    END IF;

    CASE WHEN domain_name = 'party' and source_system_name = 'BaNCS'
        THEN
            domain_ovrrd = 'party_bancs';
        WHEN domain_name = 'party' and source_system_name like 'VantageP%'
        THEN
            domain_ovrrd = 'party_vantage';
        WHEN domain_name = 'contractoptionfund' and source_system_name = 'BaNCS'
        THEN
            domain_ovrrd = 'contractoptionfund_bancs';
        WHEN domain_name = 'contractoptionfund' and source_system_name like 'VantageP%'
        THEN
            domain_ovrrd = 'contractoptionfund_vantage';
        WHEN domain_name = 'partycontractrelationship' and source_system_name = 'Bestow'
        THEN
            domain_ovrrd = 'partycontractrelationship_bestow';
        ELSE
            domain_ovrrd = domain_name;
    END CASE;

    SELECT target_schema || 'working', target_schema || 'staging' INTO working_schema, staging_schema
    FROM financedorchestration.hash_definition a
    WHERE a.domain_name = domain_ovrrd;

    --Inserting the new record to Batch Tracking
    CALL financedorchestration.sp_batch_tracking(source_system_name,'working', domain_name,'in-progress','insert','');

    --Truncate Working Tables before loading
    EXECUTE 'TRUNCATE TABLE ' || working_schema || '.' || target_table ;

    --To get the list of CDE
    SELECT
        LISTAGG(distinct CAST(attname AS VARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY attname) INTO staging_columns
    FROM pg_attribute a, pg_namespace ns, pg_class c, pg_type t, stv_tbl_perm p, pg_database db
    WHERE t.oid=a.atttypid AND a.attrelid=p.id AND ns.oid=c.relnamespace AND db.oid=p.db_id AND c.oid=a.attrelid
    AND a.attname NOT IN ('deletedxid','insertxid')
    AND ns.nspname = staging_schema
    AND TRIM(relname) = stage_table
    AND attname NOT IN ('inserttimestamp');

    --Target column list
    SELECT
        LISTAGG(distinct CAST(attname AS VARCHAR(MAX)), ', '), LISTAGG(distinct CAST('TG.'||attname AS VARCHAR(MAX)), ', ')
        WITHIN GROUP (ORDER BY attname) INTO target_columns,target_columns_select
    FROM pg_attribute a, pg_namespace ns, pg_class c, pg_type t, stv_tbl_perm p, pg_database db
    WHERE t.oid=a.atttypid AND a.attrelid=p.id AND ns.oid=c.relnamespace AND db.oid=p.db_id AND c.oid=a.attrelid
    AND a.attname NOT IN ('deletedxid','insertxid')
    AND ns.nspname = staging_schema
    AND TRIM(relname) = stage_table
    AND attname NOT IN ('inserttimestamp')
    group by relname;

    IF staging_columns NOT LIKE '%activity1035exchangeindicator%' THEN
        staging_columns = REPLACE(staging_columns,'1035exchangeindicator','1035exchangeindicator');
    END IF;

    --Insert to the working tables by copying the data from Staging
    staging_qry := 'INSERT INTO ' || working_schema || '.' || target_table || '(' || staging_columns || ',actiontype,sourcetable)' ||
                   ' SELECT ' || staging_columns || ',''INSERT'',''Staging'' FROM ' || staging_schema || '.' || stage_table ;

    --Update the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'working',REPLACE(staging_qry,'''',''''''),'append_query');

    --Execute query
    EXECUTE staging_qry;

    --Controls execution
    CALL financedwcontrols.sp_oc_findh_src(domain_name,'working',source_system_name);
    CALL financedwcontrols.sp_oc_findh_tgt(domain_name,'working',source_system_name);
    CALL financedwcontrols.sp_oc_findh_controlresults(domain_name,'working',source_system_name);

    --Inserting the new record to Batch Tracking
    CALL financedorchestration.sp_batch_tracking(source_system_name,'working', domain_name,'complete','update','');

EXCEPTION
WHEN OTHERS THEN
    CALL financedorchestration.sp_batch_tracking(source_system_name,'working', domain_name,'failed','update',SQLERRM);
    RAISE INFO 'error message SQLERRM %', SQLERRM;
    RAISE INFO 'error message SQLSTATE %', SQLSTATE;
END;
$$;
