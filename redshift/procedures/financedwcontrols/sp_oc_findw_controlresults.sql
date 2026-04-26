CREATE OR REPLACE PROCEDURE financedwcontrols.sp_oc_findw_controlresults(domain_name varchar, hop_name varchar, srcsysname varchar)
LANGUAGE plpgsql
AS $$
DECLARE DomainNm varchar(100);
DECLARE chvnValidationQry varchar(max);
DECLARE Flag int;
DECLARE feedidfilter varchar(50);
DECLARE hop varchar(100);
DECLARE dummy varchar(10);
DECLARE query nvarchar(max);
BEGIN
    hop := 'oc_findw_controlresults_' || hop_name;
    DomainNm := domain_name;

    -- batch tracking call with in-progress status
    CALL financedworchestration.sp_batch_tracking(srcsysname, hop, domain_name, 'in-progress', 'insert', '');

    DROP TABLE IF EXISTS temp_current_batch;
    BEGIN
        CREATE TEMP TABLE temp_current_batch AS
        (SELECT * FROM financedworchestration.current_batch a
         WHERE a.source_system_name = srcsysname AND a.domain_name = domain_name);
        COMMIT;
    END;

    chvnValidationQry := 'N
        select count(*) from financedwcontrols.control_results
        where hop_name = ''' || hop_name || '''
        and (batch_id, cycle_date, domain, sys_nm) in
        (select distinct batch_id, cycle_date, domain_name, source_system_name
         from temp_current_batch
         where domain_name = ''' || domain_name || ''' and source_system_name = ''' || srcsysname || ''')';

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE chvnValidationQry INTO Flag;
        COMMIT;
    END;

    -- delete logging data if batch_id already exists
    IF Flag > 0 THEN
        chvnValidationQry := 'N
            delete from financedwcontrols.control_results
            where hop_name = ''' || hop_name || '''
            and (batch_id, cycle_date, domain, sys_nm) in
            (select distinct batch_id, cycle_date, domain_name, source_system_name
             from temp_current_batch
             where domain_name = ''' || domain_name || ''' and source_system_name = ''' || srcsysname || ''')';

        CALL financedworchestration.sp_dynamic_sql_log(srcsysname, domain_name, hop, REPLACE(chvnValidationQry, '''',''''''), 'DeleteOldRecs');

        BEGIN
            SELECT 'X' INTO dummy;
            COMMIT;
        END;

        BEGIN
            --LOCK TABLE financedwcontrols.control_results;
            EXECUTE chvnValidationQry;
            COMMIT;
        END;
    END IF;

    feedidfilter := case when srcsysname = 'aah' and DomainNm in ('aahjournalentrydetaileddomain','financetransactionalaccountingview')
                     then 'and az.feed_id = bz.feed_id'
                     else '' end;

    DROP TABLE IF EXISTS temp_control_logging;

    query := 'create temp table temp_control_logging as (
        select * from financedwcontrols.controllogging
        where domain = ''' || DomainNm || '''
        and hop_name = ''' || hop_name || '''
        and (batch_id, sys_nm) in (select distinct batch_id, source_system_name from temp_current_batch
                                   where source_system_name = ''' || srcsysname || ''' and domain_name = ''' || domain_name || ''') )';

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE query;
        COMMIT;
    END;

    chvnValidationQry := '
        INSERT INTO financedwcontrols.control_results
        (batch_id, module_name, measure_group, measure_name, measure_table, src_measure_value,
         tgt_measure_value, tgt_adj_measure_value, variance, load_status, is_match, cycle_date,
         domain, duration, measure_field, sys_nm, feed_id, src_control_log_id, tgt_control_log_id,
         hop_name, src_adj_control_log_id, src_ignore_control_log_id, tgt_adj_control_log_id)
        with cte_src as (
            select * from temp_control_logging where measure_src_tgt_adj_indicator = ''S''
        ),
        cte_src_adj as (
            select * from temp_control_logging where measure_src_tgt_adj_indicator = ''SA''
        ),
        cte_src_ignored as (
            select * from temp_control_logging where measure_src_tgt_adj_indicator = ''SI''
        ),
        cte_tgt as (
            select * from temp_control_logging where measure_src_tgt_adj_indicator = ''T''
        ),
        cte_tgt_adj as (
            select * from temp_control_logging where measure_src_tgt_adj_indicator = ''TA''
        )
        select nvl(az.batch_id, bz.batch_id) as batch_id,
               nvl(az.module_name, bz.module_name) as module_name,
               nvl(az.measure_group, bz.measure_group) as measure_group,
               nvl(az.measure_name, bz.measure_name) as measure_name,
               nvl(az.measure_table, bz.measure_table) as measure_table,
               nvl(az.measure_value, 0) as src_measure_value,
               nvl(bz.measure_value, 0) as tgt_measure_value,
               nvl(bz.tgt_adj_mv, 0) as tgt_adj_measure_value,
               (nvl(az.src_measure_value, 0) + nvl(az.src_adj_mv, 0)) - (nvl(bz.measure_value, 0) + nvl(bz.tgt_adj_mv, 0)) as variance,
               nvl(az.load_status, bz.load_status) as load_status,
               case when (nvl(az.src_measure_value,0) + nvl(az.src_adj_mv,0)) = (nvl(bz.measure_value,0) + nvl(bz.tgt_adj_mv,0) + nvl(az.src_ignore_mv,0)) then ''Y'' else ''N'' end as is_match,
               cast(nvl(az.cycle_date, bz.cycle_date) as date) as cycle_date,
               nvl(az.domain, bz.domain) as domain,
               nvl(az.duration, bz.duration) as duration,
               nvl(az.measure_field, bz.measure_field) as measure_field,
               nvl(az.sys_nm, bz.sys_nm) as sys_nm,
               nvl(az.feed_id, bz.feed_id) as feed_id,
               az.src_control_log_id as src_control_log_id,
               bz.tgt_control_log_id as tgt_control_log_id,
               nvl(az.hop_name, bz.hop_name) as hop_name,
               az.src_adj_control_log_id as src_adj_control_log_id,
               az.src_ignore_control_log_id as src_ignore_control_log_id,
               bz.tgt_adj_control_log_id as tgt_adj_control_log_id
        from (select a.*, nvl(b.measure_value,0) as src_adj_mv, nvl(c.measure_value,0) as src_ignore_mv
              from cte_src a
                   left join cte_src_adj b on a.domain=b.domain and a.measure_name=b.measure_name and lower(a.sys_nm)=lower(b.sys_nm)
                   left join cte_src_ignored c on a.domain=c.domain and a.measure_name=c.measure_name and lower(a.sys_nm)=lower(c.sys_nm)
             ) az
        full outer join
             (select a.*, nvl(b.measure_value,0) as tgt_adj_mv
              from cte_tgt a
                   left join cte_tgt_adj b on a.domain=b.domain and a.measure_name=b.measure_name and lower(a.sys_nm)=lower(b.sys_nm)
             ) bz
        on az.domain=bz.domain
        and az.measure_field=bz.measure_field
        and az.measure_name=bz.measure_name
        and az.batch_id=bz.batch_id
        and lower(az.sys_nm)=lower(bz.sys_nm)
        and az.hop_name=bz.hop_name ' || feedidfilter;

    --Updating the DynamicSQL table
    CALL financedworchestration.sp_dynamic_sql_log(srcsysname, domain_name, hop, REPLACE(chvnValidationQry, '''',''''''), 'insertquery');

    -- There is a chance for controllogging to get inserted(in SP_OC_findw_src, SP_OC_FINDW_tgt) and selected(in SP_OC_FINDW_controlresults)
    -- concurrently. The lock below is to serialize Txs when they happen concurrently avoiding serialization conflicts.
    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        --LOCK financedwcontrols.control_results;
        EXECUTE chvnValidationQry;
        COMMIT;
    END;

    -- Check for any mismatches between source and target
    -- There could be a potential conflict with control_results being selected here and inserted in above statement, when we run the procedures
    -- parallely. However, the AccessExclusive Lock above holds all operations (even read) against it, making these 2 Tx serial anyway.
    BEGIN
        Flag := (
            select count(*) as cnt
            from financedwcontrols.control_results a
            where a.domain = DomainNm and a.hop_name = hop_name and sys_nm = srcsysname
            and batch_id in ( select distinct batch_id from temp_current_batch cb where cb.domain_name = domain_name and cb.source_system_name = srcsysname )
            and is_match = 'N');
        COMMIT;
    END;

    -- Raise error if any mismatches
    IF (Flag > 0) THEN
        RAISE EXCEPTION 'records and measures mismatch in findw oc controls';
    END IF;

    -- batch tracking call with complete status
    CALL financedworchestration.sp_batch_tracking(srcsysname, hop, domain_name, 'complete', 'update', '');

EXCEPTION
    WHEN OTHERS THEN
        -- batch tracking call with failed status
        CALL financedworchestration.sp_batch_tracking(srcsysname, hop, domain_name, 'failed', 'update', SQLERRM);
        RAISE INFO 'error message SQLERRM : %', SQLERRM;
        RAISE INFO 'error message SQLSTATE : %', SQLSTATE;
END;
$$

