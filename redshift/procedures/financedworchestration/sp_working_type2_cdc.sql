CREATE OR REPLACE PROCEDURE financedorchestration.sp_working_type2_cdc(source_system_name varchar, domain_name varchar)
LANGUAGE plpgsql
AS $$
--Declare Local Variables
DECLARE staging_columns VARCHAR(MAX);
DECLARE target_columns VARCHAR(MAX);
DECLARE target_columns_select VARCHAR(MAX);
DECLARE target_qry VARCHAR(MAX);
DECLARE pov_qry VARCHAR(MAX);
DECLARE action_type_qry VARCHAR(MAX);
DECLARE action_type_tgt_qry NVARCHAR(MAX);
DECLARE stage_table NVARCHAR(MAX) := 'stg_'||LOWER(source_system_name)||'_'||LOWER(domain_name);
DECLARE target_table NVARCHAR(MAX) := 'wrk_'||LOWER(source_system_name)||'_'||LOWER(domain_name);
DECLARE qry NVARCHAR(MAX);
DECLARE delete_nk_qry varchar(max);
DECLARE delete_rebuild_qry varchar(max);
DECLARE delete_count integer;
DECLARE dummy_varchar(10);
DECLARE tgt_schema VARCHAR;
DECLARE domain_ovrrd VARCHAR(MAX) := domain_name;
DECLARE working_schema VARCHAR;
DECLARE staging_schema VARCHAR;
DECLARE stop_dt_ovrrd VARCHAR(50);
DECLARE stop_date_format VARCHAR(50);
DECLARE stop_start_stop_update_qry NVARCHAR(MAX);
DECLARE pov_stop_date_update_qry NVARCHAR(MAX);
DECLARE multi_batch_count NVARCHAR(MAX);
declare multibatch_run_flag NVARCHAR(MAX);

BEGIN

    stage_table := replace(replace(stage_table,'vantagep55pl','spl'),'vantagep','p');
    target_table := replace(replace(target_table,'vantagep55pl','spl'),'vantagep','p');

    --Inserting the new record to Batch Tracking
    CALL financedorchestration.sp_batch_tracking(source_system_name,'working', domain_name,'in-progress','insert','') ;

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

    select top 1 decode(do_intraday_multibatch_run_flag, 'Y', 'TIMESTAMP', 'DATE'), do_intraday_multibatch_run_flag into stop_date_format, multibatch_run_flag
    from financedorchestration.domain_tracker a
    where a.source_system_name = source_system_name
    and a.domain_name = domain_name;

    SELECT target_schema, target_schema || 'working', target_schema || 'staging'
    INTO tgt_schema, working_schema, staging_schema
    FROM financedorchestration.hash_definition hash
    WHERE hash.domain_name = domain_ovrrd;

    --EXECUTE 'select distinct case when extract(epoch from pointofviewstartdate::date) != extract(epoch from pointofviewstartdate)* 
    --then 'datetime' else 'date' end' from financedw.' || stage_table into date_vs_datetime;*/

    if stop_date_format is null or upper(stop_date_format) = 'DATE' then
        stop_dt_ovrrd := COALESCE('(LEAD(pointofviewstartdate) over (partition by naturalkeyhashvalue order by pointofviewstartdate, pointofviewstopdate)) - 1, ''9999-12-31'') AS new_point_of_view_stop_date ';
    else
        stop_dt_ovrrd := COALESCE(dateadd('minute', -1, (LEAD(pointofviewstartdate) over (partition by naturalkeyhashvalue order by batchid, pointofviewstartdate, pointofviewstopdate))), '9999-12-31') AS new_point_of_view_stop_date ';
    end if;

    DROP TABLE IF EXISTS temp_current_batch;

    BEGIN
        CREATE TEMP TABLE temp_current_batch AS
        (SELECT * FROM financedorchestration.current_batch b
         where a.source_system_name = source_system_name and a.domain_name = domain_name);
        commit;
    END;

    --Truncate Working Tables before loading
    EXECUTE 'TRUNCATE TABLE ' || working_schema || '.' || target_table ;

    --To get the list of CDE
    SELECT
        LISTAGG(distinct '"' || attname || '"', ', ') WITHIN GROUP (ORDER BY attname) INTO staging_columns
    FROM pg_attribute a, pg_namespace ns, pg_class c, pg_type t, stv_tbl_perm p, pg_database db
    WHERE t.oid=a.atttypid AND a.attrelid=p.id AND ns.oid=c.relnamespace AND db.oid=p.db_id AND c.oid=a.attrelid
    AND typname NOT IN ('oid','xid','tid','cid')
    AND attname not in ('deletedxid','insertxid')
    AND ns.nspname = staging_schema
    AND TRIM(relname) = stage_table
    AND attname NOT IN ('inserttimestamp','updatetimestamp')
    group by relname;

    --Target column list
    SELECT
        LISTAGG(distinct '"' || attname || '"', ', '), LISTAGG(distinct 'TGT.' || attname || '','', '')
        WITHIN GROUP (ORDER BY attname) INTO target_columns,target_columns_select
    FROM pg_attribute a, pg_namespace ns, pg_class c, pg_type t, stv_tbl_perm p, pg_database db
    WHERE t.oid=a.atttypid AND a.attrelid=p.id AND ns.oid=c.relnamespace AND db.oid=p.db_id AND c.oid=a.attrelid
    AND typname NOT IN ('oid','xid','tid','cid')
    AND attname not in ('deletedxid','insertxid')
    AND ns.nspname = staging_schema
    AND TRIM(relname) = stage_table
    AND attname NOT IN ('cycledate','inserttimestamp')
    group by relname;

    --Insert to the Working tables by copying the data from Staging
    qry := 'INSERT INTO ' || working_schema || '.' || target_table || '(' || staging_columns || ',sourcetable)'
         || ' SELECT ' || staging_columns || ',''Staging'' FROM ' || staging_schema || '.' || stage_table ;

    --Update the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'working',REPLACE(qry,'''',''''''),'type2_source_insert_query');

    BEGIN
        EXECUTE qry;
        COMMIT;
    END;

    --Insert to the Working tables by copying the data from FinDW if NaturalHashKey values matches for both FinDW and Staging with PointofViewStartDate=9999-12-31
    target_qry := 'INSERT INTO ' || working_schema || '.' || target_table || '(' || target_columns || ',cycledate,sourcetable)'
                || ' SELECT ' || target_columns_select || ', TGT.cycledate, ''Target'' FROM ' || tgt_schema || '.' || domain_name || ' TGT'
                || ' INNER JOIN (Select min(cycledate) cycledate,naturalkeyhashvalue
                                FROM ' || staging_schema || '.' || stage_table || '
                                Group by naturalkeyhashvalue) Staging
                    on Staging.naturalkeyhashvalue = TGT.naturalkeyhashvalue
                    AND TGT.pointofviewstopdate = ''9999-12-31'' ';

    --Update the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'working',REPLACE(target_qry,'''',''''''),'type2_target_insert_query');

    BEGIN
        EXECUTE target_qry;
        COMMIT;
    END;

    --Updating the action_type column, Update -> finde, Delete -> from staging if hash value matches with next record ,Insert -> For record from staging
    action_type_qry := 'UPDATE ' || working_schema || '.' || target_table || ' set actiontype=tmp.actiontype FROM ' || working_schema || '.' ||
                       target_table || ' ,(SELECT naturalkeyhashvalue, pointofviewstartdate, pointofviewstopdate, batchid,
                       CASE WHEN sourcetable = ''Target'' THEN ''UPDATE'' WHEN hashvalue = LAG(hashvalue) Over(Partition by naturalkeyhashvalue order by pointofviewstartdate, batchid)
                       THEN ''DELETE'' ELSE ''INSERT'' END AS actiontype FROM ' || working_schema || '.' || target_table || ') tmp
                       WHERE ' || target_table || '.naturalkeyhashvalue=tmp.naturalkeyhashvalue
                       AND ' || target_table || '.pointofviewstartdate=tmp.pointofviewstartdate
                       AND ' || target_table || '.pointofviewstopdate=tmp.pointofviewstopdate
                       AND ' || target_table || '.batchid = tmp.batchid';

    --Update the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'working',REPLACE(action_type_qry,'''',''''''),'type2_action_type_query');

    --Execute query
    BEGIN
        EXECUTE action_type_qry;
        COMMIT;
    END;

    --Update the point_of_view_stop_date in Working Table
    --Start date from FinDW will become first record and its stop date will be next record start date -1
    --Start date from FinDW will become 2nd record and -1 will return null and end date will become 9999-12-31
    --same for single record from staging  i.e. record end date will become 9999-12-31
        pov_qry := 'UPDATE ' || working_schema || '.' || target_table || ' SET pointofviewstopdate= tmp.new_point_of_view_stop_date FROM ' || working_schema || '.' ||
                target_table || ' a, (SELECT naturalkeyhashvalue, pointofviewstartdate, pointofviewstopdate, batchid, ' || stop_dt_ovrrd ||
                ' FROM ' || working_schema || '.' || target_table || ' WHERE actiontype <> ''DELETE'') tmp
                WHERE a.naturalkeyhashvalue = tmp.naturalkeyhashvalue 
                AND a.pointofviewstartdate = tmp.pointofviewstartdate
                AND a.pointofviewstopdate = tmp.pointofviewstopdate
                AND a.batchid = tmp.batchid';

    --Update the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name,domain_name,'working',REPLACE(pov_qry,'''',''''''),'type2_pov_update_query');

    --Execute query
    BEGIN
        EXECUTE pov_qry;
        COMMIT;
    END;

    --Update the action_type with DELETES
    action_type_tgt_qry := 'UPDATE ' || working_schema || '.' || target_table || ' SET actiontype = ''DELETE'' WHERE sourcetable = ''Target'' AND pointofviewstopdate = ''9999-12-31'' ';

    --Update the DynamicSQL table
    CALL financedorchestration.sp_dynamic_sql_log(source_system_name, domain_name, 'working', REPLACE(action_type_tgt_qry,'''',''''''),'type2_action_type_update_query');

    --Execute query
    BEGIN
        EXECUTE(action_type_tgt_qry);
        COMMIT;
    END;

    if multibatch_run_flag = 'Y'
    then
        DROP TABLE IF EXISTS temp_correct_start_stop;
        pov_start_stop_date_update_qry := 'create temp table temp_correct_start_stop as
        SELECT naturalkeyhashvalue, actiontype, sourcetable, cycledate, batchid,
        lag(pointofviewstartdate) over (partition by naturalkeyhashvalue order by batchid) as lag_pointofviewstartdate,
        lag(sourcetable) over (partition by naturalkeyhashvalue order by batchid) as lag_sourcetable
        from ' || working_schema || '.' || target_table || ' where (naturalkeyhashvalue, cycledate)
        in (select naturalkeyhashvalue, cycledate
            from ' || working_schema || '.' || target_table || '
            where actiontype = ''DELETE'' group by 1, 2 having count(*) > 1)';

        CALL financedorchestration.sp_dynamic_sql_log(source_system_name, domain_name, 'working', REPLACE(pov_start_stop_date_update_qry,'''',''''''),'temp_correct_start_stop');

        BEGIN
            EXECUTE(pov_start_stop_date_update_qry);
            COMMIT;
        END;

        EXECUTE 'select count(*) from temp_correct_start_stop' into multi_batch_count;

        if multi_batch_count > 0
        then
            pov_start_stop_date_update_qry := 'update ' || working_schema || '.' || target_table || ' wrk set pointofviewstartdate = ts.new_startdate, pointofviewstopdate = ts.new_stopdate from
            (select naturalkeyhashvalue, pointofviewstartdate, pointofviewstopdate, hashvalue, batchid,
            case 
                when wrk_sourcetable = ''Staging'' then
                    pointofviewstartdate::date + interval ''00 hours'' + ((batch_rank_target + batch_rank - 1) * interval ''30 minutes'')
                else pointofviewstartdate
            end as new_startdate,
            case 
                when (wrk_sourcetable = ''Staging'' and pointofviewstopdate != ''9999-12-31'') or (wrk_sourcetable = ''Target'') then
                    new_startdate + interval ''00 hours'' + (interval ''30 minutes'') - interval ''1 minute''
                else pointofviewstopdate
            end as new_stopdate
            from (select *, w.sourcetable as wrk_sourcetable, row_number() over (partition by cycledate, naturalkeyhashvalue order by batchid) as batch_rank,
            case when lag_sourcetable = ''Target'' then nvl(FLOOR(DATEDIFF(MINUTE, DATE_TRUNC(''day'', lag_pointofviewstartdate), lag_pointofviewstartdate)/30), 0)
            else 0 end as batch_rank_target
            from ' || working_schema || '.' || target_table || ' w
            join temp_correct_start_stop using (naturalkeyhashvalue, actiontype, sourcetable, cycledate, batchid)) ranked_updates ts
            where wrk.naturalkeyhashvalue = ts.naturalkeyhashvalue and wrk.pointofviewstartdate = ts.pointofviewstartdate
            and wrk.pointofviewstopdate = ts.pointofviewstopdate and wrk.hashvalue = ts.hashvalue
            and wrk.batchid = ts.batchid';

            CALL financedorchestration.sp_dynamic_sql_log(source_system_name, domain_name, 'working', REPLACE(pov_start_stop_date_update_qry,'''',''''''),'type2_pov_start_stop_date_update_qry');

            --Execute query
            BEGIN
                EXECUTE(pov_start_stop_date_update_qry);
                COMMIT;
            END;
        end if;
    end if;

    --OC Controls Execution
    call financecontrols.sp_oc_findw_src(domain_name,'working',source_system_name);
    call financecontrols.sp_oc_findw_tgt(domain_name,'working',source_system_name);
    call financecontrols.sp_oc_findw_controlresults(domain_name,'working',source_system_name);

    --Inserting the new record to Batch Tracking
    CALL financedorchestration.sp_batch_tracking(source_system_name,'working', domain_name,'complete','update','');

EXCEPTION
    WHEN OTHERS THEN
        CALL financedorchestration.sp_batch_tracking(source_system_name,'working', domain_name,'failed','update',SQLERRM);
        RAISE INFO 'error message SQLERRM %', SQLERRM;
        RAISE INFO 'error message SQLSTATE %', SQLSTATE;
END;
$$

