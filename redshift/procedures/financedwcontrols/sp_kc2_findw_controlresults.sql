CREATE OR REPLACE PROCEDURE financedwcontrols.sp_kc2_findw_controlresults(source_system varchar, domain_name varchar, hop_name varchar)
LANGUAGE plpgsql
AS $$
DECLARE DomainNm Varchar(100);
DECLARE SrcSysNm Varchar(50);
DECLARE chvnTargetQry nvarchar(max);
DECLARE TgtQry nvarchar(max);
DECLARE Hop_src Varchar(50);
DECLARE Hop_tgt Varchar(50);
DECLARE KC2Ind Varchar(50);
DECLARE Flag int;
DECLARE ctrltblNm varchar(max);
DECLARE hop varchar(100);
DECLARE dummy varchar(10);

BEGIN
    hop := 'kc2_findw_controlresults_' || hop_name;
    SrcSysNm := source_system;
    DomainNm := domain_name;
    Hop_tgt := hop_name;
    ctrltblNm := 'financedwcontrols.kc2_controllogging';
    Hop_src := 'curated';

    -- batch tracking call with in-progress status
    CALL financedworchestration.sp_batch_tracking(source_system, hop, domain_name, 'in-progress', 'insert', '');

    DROP TABLE IF EXISTS temp_current_batch;

    BEGIN
        CREATE TEMP TABLE temp_current_batch AS
        (SELECT * FROM financedworchestration.current_batch a
         where a.source_system_name = source_system and a.domain_name = domain_name);
        commit;
    END;

    chvnTargetQry := N'
    SELECT COUNT(*) from financedwcontrols.kc2_control_results
    where kc2kc6_indicator = ''kc2'' and
    hop_name = ''' || Hop_src || '_' || Hop_tgt || '''
    and (batch_id, cycle_date, domain, source_system_name) in
    (select distinct batch_id, cycle_date, domain_name, source_system_name
     from temp_current_batch
     where domain_name = ''' || domain_name || ''' and source_system_name= ''' || source_system || ''')';

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE chvnTargetQry INTO Flag;
        COMMIT;
    END;

    -- delete logging data if batch_id already exists
    IF Flag > 0 THEN
        chvnTargetQry := N'
        delete from financedwcontrols.kc2_control_results
        where kc2kc6_indicator = ''kc2'' and
        hop_name = ''' || Hop_src || '_' || Hop_tgt || '''
        and (batch_id, cycle_date, domain, source_system_name) in
        (select distinct batch_id, cycle_date, domain_name, source_system_name
         from temp_current_batch
         where domain_name = ''' || domain_name || ''' and source_system = ''' || source_system || ''')';

        CALL financedworchestration.sp_dynamic_sql_log(source_system, domain_name, hop, REPLACE(chvnTargetQry, '''', '"'), 'DeleteOldRecs');

        BEGIN
            SELECT 'X' INTO dummy;
            COMMIT;
        END;

        BEGIN
            --LOCK TABLE financedwcontrols.kc2_control_results;
            EXECUTE chvnTargetQry;
        END;
    END IF;

    chvnTargetQry := N'
    INSERT INTO financedwcontrols.kc2_control_results(domain, operation, control_id, fact_column_name, group_column_name, group_column_value,
    source_fact_value, target_fact_value, fact_value_variance, match_indicator, cycle_date, comments, source_system_name,
    kc2kc6_indicator, hop_name, batch_id, src_control_log_id, tgt_control_log_id)
    select * from (
    WITH cte_stg AS
    (
        SELECT a.*, c.reconciliation_indicator
        FROM ' || ctrltblNm || ' a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        and a.domain = b.domain_name and a.source_system_name = b.source_system_name
        inner join financedwcontrols.lkup_kc2_control_measures c on a.domain = c.domain_name
        and c.measure_type = a.operation and c.source_system_name = a.source_system_name and c.key_control_type = ''kc2''
        and a.control_id = c.domain_name || ''_'' || c.measure_field
        where a.hop_name like ''' || Hop_src || '_' || domain_name || ''' and a.source_system_name = ''' || SrcSysNm || '''
        and a.group_col_name != ''NULL'' OR a.group_col_name != NULL
    ),
    cte_stgAd AS
    (
        SELECT a.*, c.reconciliation_indicator
        FROM ' || ctrltblNm || ' a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        and a.domain = b.domain_name and a.source_system_name = b.source_system_name
        inner join financedwcontrols.lkup_kc2_control_measures c on a.domain = c.domain_name
        and c.measure_type = a.operation and c.source_system_name = a.source_system_name and c.key_control_type = ''kc2''
        and a.control_id = c.domain_name || ''_'' || c.measure_field
        where a.hop_name like ''' || Hop_tgt || '_' || domain_name || ''' and a.source_system_name = ''' || SrcSysNm || '''
        and a.group_col_name != ''NULL'' OR a.group_col_name != NULL
        and c.reconciliation_indicator = ''Y''
    ),
    cte_ad AS
    (
        SELECT a.*, c.reconciliation_indicator
        FROM ' || ctrltblNm || ' a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        and a.domain = b.domain_name and a.source_system_name = b.source_system_name
        inner join financedwcontrols.lkup_kc2_control_measures c on a.domain = c.domain_name
        and c.measure_type = a.operation and c.source_system_name = a.source_system_name and c.key_control_type = ''kc2''
        and a.control_id = c.domain_name || ''_'' || c.measure_field
        where a.hop_name like ''' || Hop_tgt || '_' || domain_name || ''' and a.source_system_name = ''' || SrcSysNm || '''
        and c.reconciliation_indicator = ''Y''
    )
    SELECT
        domain, operation, control_id, fact_col_name, group_col_name, group_col_value,
        CAST(nvl(source_fact_value,'0') AS numeric(22,6)), CAST(nvl(target_fact_value,'0') AS numeric(22,6)),
        CASE WHEN (CAST(nvl(source_fact_value,'0') AS numeric(22,6)) - CAST(nvl(target_fact_value,'0') AS numeric(22,6))) = 0
        THEN ''Y'' ELSE ''N'' END as match_indicator, cycle_date, comments, source_system_name, kc2kc6_indicator,
        HopType as hop_name, batch_id, src_control_log_id, tgt_control_log_id
    FROM ...';
END;
$$;
        FROM cte_stg a
        LEFT JOIN cte_stgAd c
        ON a.domain = c.domain
        AND a.group_col_name = c.group_col_name
        AND ISNULL(a.group_col_val,'') = ISNULL(c.group_col_val,'')
        AND a.source_system_name = c.source_system_name
        AND a.cycle_date = c.cycle_date and a.batch_id = c.batch_id
        FULL OUTER JOIN cte_ad b
        ON a.domain = b.domain
        AND a.group_col_name = b.group_col_name
        AND ISNULL(a.group_col_val,'') = ISNULL(b.group_col_val,'')
        AND a.source_system_name = b.source_system_name
        AND a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
    );

    --Updating the DynamicSQL table
    CALL financedworchestration.sp_dynamic_sql_log(source_system,domain_name,hop ,REPLACE(chvnTargetQry,'''',''''''),'chvnTargetQry');

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        --LOCK financedwcontrols.kc2_control_results;
        EXECUTE chvnTargetQry;
        COMMIT;
    END;

    chvnTargetQry1 := N'
    INSERT INTO financedwcontrols.kc2_control_results (domain,operation,control_id,fact_column_name,group_column_name,group_column_value,source_fact_value,target_fact_value,fact_value_variance,match_indicator,cycle_date,comments,source_system_name,kc2kc6_indicator,hop_name,batch_id,src_control_log_id,tgt_control_log_id)
    select * from (
    WITH cte_stg AS
    (
        SELECT a.*,c.reconciliation_indicator
        FROM ' || ctrltblNm || ' a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        and a.domain = b.domain_name and a.source_system_name = b.source_system_name
        inner join financedwcontrols.lkup_kc2_control_measures c on a.domain = c.domain_name
        and c.measure_type = a.operation and c.source_system_name = a.source_system_name and c.key_control_type = ''kc2''
        and a.control_id = c.domain_name || ''_'' || c.measure_field
        where a.hop_name like ''' || Hop_src || ''' and a.domain = ''' || DomainNm || ''' and a.source_system_name = ''' || SrcSysNm || '''
        and c.reconciliation_indicator = ''Y'' and a.fact_col_name != ''NULL'' OR a.fact_col_name != NULL
    ),
    cte_stgAd AS
    (
        SELECT a.*,c.reconciliation_indicator
        FROM ' || ctrltblNm || ' a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        and a.domain = b.domain_name and a.source_system_name = b.source_system_name
        inner join financedwcontrols.lkup_kc2_control_measures c on a.domain = c.domain_name
        and c.measure_type = a.operation and c.source_system_name = a.source_system_name and c.key_control_type = ''kc2''
        and a.control_id = c.domain_name || ''_'' || c.measure_field
        where a.hop_name like ''' || Hop_src || ''' and a.domain = ''' || DomainNm || ''' and a.source_system_name = ''' || SrcSysNm || '''
        and c.reconciliation_indicator = ''Y'' and a.fact_col_name != ''NULL'' OR a.fact_col_name != NULL
    ),
    cte_ad AS
    (
        SELECT a.*,c.reconciliation_indicator
        FROM ' || ctrltblNm || ' a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        and a.domain = b.domain_name and a.source_system_name = b.source_system_name
        inner join financedwcontrols.lkup_kc2_control_measures c on a.domain = c.domain_name
        and c.measure_type = a.operation and c.source_system_name = a.source_system_name and c.key_control_type = ''kc2''
        and a.control_id = c.domain_name || ''_'' || c.measure_field
        where a.hop_name like ''' || Hop_tgt || ''' and a.domain = ''' || DomainNm || ''' and a.source_system_name = ''' || SrcSysNm || '''
        and c.reconciliation_indicator = ''Y'' and a.fact_col_name != ''NULL'' OR a.fact_col_name != NULL
    )
    SELECT
        domain,operation,control_id,fact_col_name,group_col_name,group_col_val,
        CAST(nvl(source_fact_val,'0') AS numeric(22,6)),CAST(nvl(target_fact_val,'0') AS numeric(22,6)) AS fact_value_variance,
        CASE WHEN (CAST(nvl(source_fact_val,'0') AS numeric(22,6)) - CAST(nvl(target_fact_val,'0') AS numeric(22,6))) = 0
        THEN ''Y'' ELSE ''N'' END as MatchIndicator,
        cycle_date,comments,source_system_name,kc2kc6_indicator,HopType as hop_name,batch_id,src_control_log_id,tgt_control_log_id
    FROM
    (
        SELECT
        nvl(a.domain,b.domain) as domain, nvl(a.Operation,b.Operation) Operation,
        nvl(a.control_id,b.control_id) control_id, nvl(a.fact_col_name,b.fact_col_name) fact_col_name,
        nvl(a.group_col_name,b.group_col_name) group_col_name, nvl(a.group_col_val,b.group_col_val) group_col_val,
        CAST(nvl(a.fact_val,'0') AS numeric(22,6)) AS source_fact_value, CAST(nvl(b.fact_val,'0') AS numeric(22,6)) AS target_fact_value,
        nvl(a.cycle_date,b.cycle_date) as cycle_date, NULL as comments,
        nvl(a.source_system_name,b.source_system_name) source_system_name,
        ''kc2'' as KC2KC6indicator,
        nvl(a.hop_name,'curated') || ''_To_'' || nvl(b.hop_name,'datawarehouse') as HopType,
        nvl(a.batch_id,b.batch_id) as batch_id,
        a.control_log_id as src_control_log_id,
        b.control_log_id as tgt_control_log_id
        FROM cte_stg a
        LEFT JOIN cte_stgAd c
        ON a.domain = c.domain
        AND a.fact_col_name = c.fact_col_name
        AND a.source_system_name = c.source_system_name
        AND a.cycle_date = c.cycle_date and a.batch_id = c.batch_id
        OUTER JOIN cte_ad b
        ON a.domain = b.domain
        AND a.fact_col_name = b.fact_col_name
        AND a.source_system_name = b.source_system_name
        AND a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
    );

    --Updating the DynamicSQL table
    CALL financedworchestration.sp_dynamic_sql_log(source_system,domain_name,hop ,REPLACE(chvnTargetQry1,'''',''''''),'chvnTargetQry1');

    --key_controls_kc2 is being inserted in multiple places and being selected in this statement. The lock in here makes all the ops. on this table wait until lock is released.
    --kc2_control_results is being inserted(in this statement) and selected(below statement). Locking the table here to prevent conflicts when this SP runs parallelly.

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        --LOCK financedwcontrols.kc2_control_results;
        EXECUTE chvnTargetQry1;
        COMMIT;
    END;

    --Check If there are any Counts Mismatch
    --The same lock is not applied here, as we already put a lock on kc2_control_results in above statement.
    BEGIN
        Flag := (
            select count(*) as cnt
            from financedwcontrols.kc2_control_results a
            inner join temp_current_batch b on a.cycle_date = b.cycle_date
            and a.batch_id = b.batch_id and a.domain = b.domain_name and a.source_system_name = b.source_system_name
            where a.kc2kc6_indicator = 'kc2'
            and b.source_system_name = source_system
            and b.domain_name = DomainNm
            and a.hop_name = 'curated_to_datawarehouse'
            and a.match_indicator = 'N'
        );
        COMMIT;
    END;

    IF(Flag > 0)
    THEN
        RAISE EXCEPTION 'Records and Measures Mismatch in key controls kc2' ;
    END IF;

    -- batch tracking call with complete status
    CALL financedworchestration.sp_batch_tracking(source_system,hop, domain_name, 'complete', 'update','') ;

EXCEPTION
WHEN OTHERS THEN
    CALL financedworchestration.sp_batch_tracking(source_system,hop, domain_name, 'failed','update',SQLERRM) ;
    RAISE INFO 'error message SQLERRM %', SQLERRM;
    RAISE INFO 'error message SQLSTATE %', SQLSTATE;
End;
$$;
