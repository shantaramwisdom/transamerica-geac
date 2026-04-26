CREATE OR REPLACE PROCEDURE financedorchestration.sp_load_current_batch(source_system varchar, domain_name varchar, batch_frequency varchar)
LANGUAGE plpgsql
AS $$
DECLARE qry NVARCHAR(MAX);
DECLARE cnt INTEGER;
DECLARE delete_count integer;
DECLARE dummy varchar(10);
DECLARE id_pattern varchar(10);
DECLARE ovrrd varchar(255) := '';
DECLARE ovrrd_cnt INTEGER := 1;
DECLARE ovrrd_cnt2 INTEGER := 0;
DECLARE domain_ovrrd VARCHAR(MAX) := domain_name;
DECLARE tgt_schema varchar(50);
DECLARE qry_ovrrd varchar(max) := '';
DECLARE qry_ovrrd2 varchar(max) := '';
DECLARE multibatch_run_flag varchar(10) := 'N';
BEGIN

    --Delete the records from current batch for the key
    EXECUTE 'select count(*) FROM financedorchestration.current_batch WHERE source_system_name = '''||source_system||''' and domain_name = '''||domain_name||'''' into delete_count;

    --Delete the records in current batch
    --Block all ops on current_batch to avoid conflicts while its being deleted. While delete obtains less restrictive locks, having explicit Lock
    --creates AccessExclusiveLock which block all other lock attempts until this Tx is completed

    if delete_count > 0
    then
        BEGIN
            SELECT 'X' INTO dummy;
            COMMIT;
        END;

        BEGIN
            --LOCK financedorchestration.current_batch;
            EXECUTE 'DELETE FROM financedorchestration.current_batch WHERE source_system_name = '''||source_system||''' and domain_name = '''||domain_name||'''';
            COMMIT;
        END;
    end if;

    select top 1 do_intraday_multibatch_run_flag into multibatch_run_flag
    from
    financedorchestration.domain_tracker a
    where a.source_system_name = source_system
    and a.domain_name = domain_name
    and a.batch_frequency = batch_frequency;

    if multibatch_run_flag = 'Y'
    then
        qry_ovrrd2 := ', a.batch_id';
    end if;

    DROP TABLE IF EXISTS temp_batch_tracking;
    if domain_name in ('financeactivity','financecontract','contractparty','financetransactionalaccountingview')
    then
        if domain_name = 'financeactivity' then
            ovrrd := '(''activity'')';
        elseif source_system = 'aah' and domain_name = 'financetransactionalaccountingview' then
            ovrrd := '(''aahjournalentrydetaileddomain'')';
        elseif source_system in ('oraclefah','ERPDW') and domain_name = 'financetransactionalaccountingview' then
            ovrrd := '(''subledgerjournallinedetail'')';
        elseif domain_name in ('financecontract','contractparty') and (source_system = 'BaNCS' or source_system like 'VantageP%') then
            ovrrd := '(''party'',''contract'')';
            ovrrd_cnt := 2;
        elseif domain_name in ('financecontract','contractparty') and source_system != 'BaNCS' and source_system not like 'VantageP%' then
            ovrrd := '(''party'',''contract'',''partycontractrelationship'')';
            ovrrd_cnt := 3;
        end if;
    end if;

    qry := 'CREATE TEMPORARY table temp_batch_tracking as(
            WITH
            datawarehouse AS
            (select *
             from
             (select source_system_name, domain_name, batch_id, cycle_date, phase_name, load_status,
              row_number() over(partition by source_system_name,
              domain_name, phase_name order by batch_id desc, insert_timestamp desc) fltr
              from financedorchestration.batch_tracking bt
              where phase_name = ''datawarehouse''
              and source_system_name = '''||source_system||'''
              and domain_name = '''||domain_name||''')
              where fltr=1 and load_status = ''complete'')
            select source_system_name, domain_name, batch_id, cycle_date, phase_name, load_status, 0 as fltr
            from datawarehouse
            union
            select distinct a.source_system_name, a.domain_name, a.batch_id, a.cycle_date, a.phase_name, a.load_status
            , row_number() over (partition by a.cycle_date '||qry_ovrrd2||' order by null) fltr
            from financedorchestration.batch_tracking a
            left join datawarehouse b on a.source_system_name = b.source_system_name
            where a.phase_name = ''datawarehouse'' and a.load_status = ''complete''
            and a.source_system_name = '''||source_system||'''
            and a.domain_name in '||ovrrd||'
            and cast(a.batch_id as bigint) > COALESCE(b.batch_id, 0));';
else
    qry := 'CREATE TEMPORARY table temp_batch_tracking as(
            select *
            from
            (select source_system_name, domain_name, batch_id, cycle_date, phase_name, load_status,
             row_number() over(partition by source_system_name,
             domain_name, phase_name order by batch_id desc, insert_timestamp desc) fltr
             from financedorchestration.batch_tracking bt
             where phase_name = ''datawarehouse'' and domain_name = '''||domain_name||'''
             and source_system_name = '''||source_system||''')
             where fltr=1 and load_status = ''complete'');';
end if;

BEGIN
    SELECT 'X' INTO dummy;
    COMMIT;
END;

BEGIN
    EXECUTE qry;
    COMMIT;
END;

if (domain_name in ('aahjournalentrydetaileddomain') or source_system = 'aah')
then
    qry := 'SELECT cast(batchid as bigint) as batch_id, cast(cycle_date as date) cycle_date, source_system = '''||source_system||''', '''||domain_name||''' as domain_name
            FROM
            (select distinct dc.batchid ,bc.cycle_date, dc.source_system from
             financedwstage.dccompletedbatchinfo dc
             inner join financedwstage.batchcycleidinfo bc
             on cast(dc.batchid as bigint)=cast(bc.batchid as bigint) and
             dc.source_system = '''||source_system||'''
             LEFT JOIN (
             SELECT source_system_name, MAX(batch_id) AS batch_id, domain_name
             FROM temp_batch_tracking
             WHERE domain_name = '''||domain_name||'''
             GROUP BY source_system_name,domain_name
             ) b ON
             a.source_system = b.source_system_name
             WHERE cast(a.batchid as bigint) > COALESCE(b.batch_id, 0)';
elseif source_system = 'BaNCS' and domain_name in ('party','contract','contractpayout','contractoption','contractoptionfund','activity')
then
    qry := 'select distinct a.batchid::int as batch_id, '''||source_system||''' as source_system, '''||domain_name||'''
             as domain_name, a.rtaa_effective_date as cycle_date
             from financemasteradstage.gdcompletedbatchinfo a
             left join
             (select max(batch_id) batch_id, source_system_name, domain_name
              from temp_batch_tracking
              where domain_name = '''||domain_name||'''
              group by source_system_name, domain_name) b on lower(a.source_system) = lower(b.source_system_name)
              where lower(a.source_system) = lower('''||source_system||''') and a.batchid::int > COALESCE(b.batch_id, 0)';
--Various Derived Domains from base Domains in FINANCEDW
elseif domain_name in ('financecontract','financeactivity','contractparty','financetransactionalaccountingview')
then
    qry := 'SELECT distinct a.batch_id,a.cycle_date,a.source_system_name source_system, '''||domain_name||''' as domain_name
             from temp_batch_tracking a
             left join (select max(batch_id) batch_id, source_system_name
                        from temp_batch_tracking aa
                        where aa.domain_name != '''||domain_name||''' group by source_system_name) b on a.source_system_name = b.source_system_name
             WHERE a.domain_name != '''||domain_name||''' and fltr != ovrrd_cnt
             and a.batch_id > COALESCE(b.batch_id, 0)';
else
    qry := 'SELECT DISTINCT a.batch_id,a.source_system, a.domain_name, a.cycle_date
             FROM financecurated.completedbatch a
             LEFT JOIN (SELECT max(batch_id) batch_id, source_system_name, domain_name
                        FROM temp_batch_tracking
                        WHERE domain_name = '''||domain_name||'''
                        AND batch_frequency = '''||batch_frequency||'''
                        GROUP BY source_system_name,domain_name) b ON a.source_system_name = a.source_system
             WHERE a.source_system = '''||source_system||''' and a.domain_name = '''||domain_name||'''';
end if;
DROP TABLE IF EXISTS temp_current_batch;
qry := 'CREATE TEMPORARY table temp_current_batch as (' || qry || ')';

BEGIN
    SELECT 'X' INTO dummy;
    COMMIT;
END;

BEGIN
    EXECUTE qry;
    COMMIT;
END;

qry := 'INSERT INTO financedorchestration.current_batch (batch_id ,cycle_date, source_system_name, domain_name)
        (SELECT batch_id ,cycle_date, source_system_name, domain_name FROM temp_current_batch)';

BEGIN
    SELECT 'X' INTO dummy;
    COMMIT;
END;

BEGIN
    --LOCK financedorchestration.current_batch;
    EXECUTE qry;
    COMMIT;
END;

CASE WHEN domain_name = 'party' and source_system = 'BaNCS'
    THEN
        domain_ovrrd = 'party_bancs';
    WHEN domain_name = 'party' and source_system like 'VantageP%'
    THEN
        domain_ovrrd = 'party_vantage';
    WHEN domain_name = 'contractoptionfund' and source_system = 'BaNCS'
    THEN
        domain_ovrrd = 'contractoptionfund_bancs';
    WHEN domain_name = 'contractoptionfund' and source_system like 'VantageP%'
    THEN
        domain_ovrrd = 'contractoptionfund_vantage';
    WHEN domain_name = 'partycontractrelationship' and source_system = 'Bestow'
    THEN
        domain_ovrrd = 'partycontractrelationship_bestow';
    ELSE
        domain_ovrrd = domain_name;
END CASE;

SELECT load_pattern, target_schema
INTO ld_pattern, tgt_schema
FROM financedorchestration.hash_definition hash
WHERE hash.domain_name = domain_ovrrd;

-- Check to see if same cycle date has come again for TYPE 2 Tables or ERP GL Header/Line
IF ld_pattern = 'type2' or domain_name in ('general_ledger_header', 'general_ledger_line_item')
THEN
    DROP TABLE IF EXISTS temp_domain_min_max_cycle_date;

    SELECT count(distinct domain_name) INTO ovrrd_cnt2
    FROM financedorchestration.domain_tracker dt
    WHERE dt.domain_name = domain_ovrrd and dt.source_system_name = source_system
    and dt.do_intraday_multibatch_run_flag = 'Y';

    IF ovrrd_cnt2 = 1
    THEN
        --Allow IntraDay MultiBatch
        qry_ovrrd = ' and batch_id between bt.min_batchid and bt.max_batchid ';
    END IF;

    IF domain_name in ('general_ledger_header', 'general_ledger_line_item') and ovrrd_cnt2 = 0
    THEN
        qry_ovrrd = ' or batch_id between bt.min_batchid and bt.max_batchid ';
    END IF;

    qry := 'CREATE TEMPORARY table temp_domain_min_max_cycle_date as (
            select source_system_name,
            max(cycledate) max_cycle_date, min(cycledate) min_cycle_date,
            max(batchid) max_batchid, min(batchid) min_batchid
            from ' || tgt_schema || '.' || domain_name || '
            where source_system_name = ''' || source_system || '''
            group by source_system_name)';

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE qry;
        COMMIT;
    END;

    qry := 'SELECT count(*)
            FROM financedorchestration.current_batch cb
            join temp_domain_min_max_cycle_date bt on cb.source_system_name = bt.source_system_name
            and (cycle_date between bt.min_cycle_date and bt.max_cycle_date' || qry_ovrrd || ')
            where cb.source_system_name = ''' || source_system || ''' and cb.domain_name = ''' || domain_name || '''';

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE qry INTO cnt;
        COMMIT;
    END;

    IF cnt > 0
    THEN
        RAISE EXCEPTION 'The cycle date is already processed in datawarehouse. Please check BatchTracking and do necessary cleanups';
    END IF;
END IF;

-- Check to see if MultiBatch Present from Curated Process for Vantage Systems or ALM
IF (source_system like 'VantageP%' or source_system = 'ALM') and domain_name not in ('financeactivity', 'financecontract', 'contractparty', 'general_ledger_header', 'general_ledger_line_item')
THEN
    qry := 'SELECT count(*)
            FROM financedorchestration.current_batch cb
            where cb.source_system_name = ''' || source_system || ''' and cb.domain_name = ''' || domain_name || '''';

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE qry INTO cnt;
        COMMIT;
    END;

    IF cnt > 1
    THEN
        RAISE EXCEPTION 'There are multiple Batches to be processed for Curated Domain. Only One Batch Allowed per Domain for Vantage Systems or ALM';
    END IF;
END IF;

EXCEPTION
    WHEN OTHERS THEN
        RAISE INFO 'error message SQLERRM %', SQLERRM;
        RAISE INFO 'error message SQLSTATE %', SQLSTATE;
END;
$$
;
