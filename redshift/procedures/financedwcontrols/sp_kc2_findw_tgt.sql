CREATE OR REPLACE PROCEDURE financedwcontrols.sp_kc2_findw_tgt(srcsysname varchar, domain_name varchar, hop_name varchar)
LANGUAGE plpgsql
AS $$
DECLARE DomainNm Varchar(100);
DECLARE MaxRowNum Int;
DECLARE chvnMeasureType Varchar(50);
DECLARE chvnMeasureField Varchar(150);
DECLARE DomainTypeNm Varchar(100);
DECLARE chvnTargetQry Varchar(max);
DECLARE loadTargetData nvarchar(max);
DECLARE Counter Int;
DECLARE HopNm Varchar(50);
DECLARE HopSchName Varchar(50);
DECLARE JoinCondition Varchar(255);
DECLARE GroupByClause Varchar(255);
DECLARE OptionalColumn Varchar(255);
DECLARE measures varchar(255);
DECLARE deletecontrols varchar(max);
DECLARE hop varchar(100);
DECLARE dummy varchar(1);
DECLARE domain_ovrrd VARCHAR(MAX) = domain_name;
BEGIN
    hop = 'kc2_findw_tgt_' + hop_name;
    HopNm = hop_name;
    DomainNm = domain_name;

    CASE WHEN domain_name = 'party_banCs' and srcsysname = 'BanCS'
        THEN
            domain_ovrrd = 'party_banCs';
        WHEN domain_name = 'party_banCs' and srcsysname like 'VantagePt%'
        THEN
            domain_ovrrd = 'party_vantage';
        WHEN domain_name = 'contractoptionfund_banCs' and srcsysname = 'BanCS'
        THEN
            domain_ovrrd = 'contractoptionfund_banCs';
        WHEN domain_name = 'contractoptionfund_banCs' and srcsysname like 'VantagePt%'
        THEN
            domain_ovrrd = 'contractoptionfund_vantage';
        WHEN domain_name = 'partycontractrelationship' and srcsysname = 'Bestow'
        THEN
            domain_ovrrd = 'partycontractrelationship_bestow';
        ELSE
            domain_ovrrd = domain_name;
    END CASE;

    SELECT target_schema INTO HopSchName
    FROM financedworchestration.hash_definition hash
    WHERE hash.domain_name = domain_ovrrd;

    -- batch tracking call with in-progress status
    CALL financedworchestration.sp_batch_tracking(srcsysname, hop, domain_name, 'in-progress', 'insert', '');

    DROP TABLE IF EXISTS temp_current_batch;

    BEGIN
        CREATE TEMP TABLE temp_current_batch AS
        (SELECT * FROM financedworchestration.current_batch a
         WHERE a.source_system_name = srcsysname AND a.domain_name = domain_name);
        COMMIT;
    END;

    deletecontrols = 'N';
    SELECT COUNT(*) FROM financedwcontrols.kc2_controllogging
    WHERE hop_name = '''' + hop_name + ''''
      AND (batch_id, cycle_date, domain, source_system_name) IN
          (SELECT DISTINCT batch_id, cycle_date, domain_name, source_system_name
           FROM temp_current_batch
           WHERE domain_name = '''' + domain_name + ''''
             AND source_system_name = '''' + srcsysname + '''');

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE deletecontrols INTO Counter;
        COMMIT;
    END;

    -- delete logging data if batch_id already exists
    IF Counter > 0 THEN
        deletecontrols = 'N';
        DELETE FROM financedwcontrols.kc2_controllogging
        WHERE hop_name = '''' + hop_name + ''''
          AND (batch_id, cycle_date, domain, source_system_name) IN
              (SELECT DISTINCT batch_id, cycle_date, domain_name, source_system_name
               FROM temp_current_batch
               WHERE domain_name = '''' + domain_name + ''''
                 AND source_system_name = '''' + srcsysname + '''');
    END IF;

    CALL financedworchestration.sp_dynamic_sql_log(srcsysname, domain_name, hop, REPLACE(deletecontrols, '''', ''''''), 'DeleteOldRecs');

    DROP TABLE IF EXISTS tgttabl;

    CREATE TEMPORARY TABLE tgttabl
    (
        "domain" VARCHAR(255),
        "operation" VARCHAR(255),
        "control_id" VARCHAR(255),
        "fact_col_name" VARCHAR(255),
        "group_col_name" VARCHAR(255),
        "group_col_val" VARCHAR(255),
        "fact_val" VARCHAR(255),
        "cycle_date" DATE,
        "source_system_name" VARCHAR(255) NOT NULL,
        "hop_nm" VARCHAR(255),
        "batch_id" VARCHAR(255)
    );

    DROP TABLE IF EXISTS getmeasures;

    CREATE TEMPORARY TABLE getmeasures
    (
        domainname VARCHAR(100),
        measuretype VARCHAR(50),
        measurefield VARCHAR(100),
        rownum INTEGER
    );

    measures = 'N';
    INSERT INTO getmeasures
    SELECT domain_name, Measure_Type, Measure_Field, row_number() over(order by measure_type) as rownum
    FROM financedwcontrols.lkup_kc2_control_measures
    WHERE source_system_name = '''' + srcsysname + '''' AND domain_name = '''' + DomainNm + '''' AND is_active = 'Y';

    -- execute measures
    BEGIN
        EXECUTE(measures);
        COMMIT;
    END;

    SELECT MAX(rownum) INTO MaxRowNum FROM getmeasures;

    Counter = 1;

    WHILE (Counter <= MaxRowNum)
    LOOP
        SELECT measuretype, measurefield, domainname
        INTO chvnMeasureType, chvnMeasureField, DomainTypeNm
        FROM getmeasures
        WHERE rownum = Counter;

        JoinCondition = ' and b.source_system_name = a.source_system_name and b.source_system_name = '''' + srcsysname + '''' and b.domain_name = '''' + domain_name + '''' ';
        OptionalColumn = 'b.source_system_name as source_system_name,';

        GroupByClause = CASE
            WHEN chvnMeasureType = 'Sum Total on Year' and chvnMeasureField not like '%date%' THEN 'group by a.' + chvnMeasureField + ', a.batchId, b.source_system_name, a.cycledate'
            WHEN chvnMeasureType = 'Sum Total on Year' and chvnMeasureField like '%date%' THEN 'group by a.batchId, b.source_system_name, a.cycledate'
            WHEN chvnMeasureType in ('DistinctCount', 'Sum Total', 'Total Count') THEN 'group by b.source_system_name, a.batchId, a.cycledate'
            ELSE 'group by a.' + chvnMeasureField + ', b.source_system_name, a.batchId, a.cycledate'
        END;

        -- FinanceDW/ERPDM Controls Check ================================
        IF chvnMeasureType = 'Count grouped by distinct value'
        THEN
            chvnTargetQry = 'N
            INSERT INTO tgttabl
            SELECT
            ''' || DomainTypeNm || ''' as Domain, ''' || chvnMeasureType || ''' as Operation,
            ''' || chvnMeasureField || ''' as control_id,
            NULL as fact_col_name,
            ''' || chvnMeasureField || ''' as group_col_name,
            ''' || chvnMeasureField || ''' as group_col_val,
            COUNT(ISNULL(a.' || chvnMeasureField || ',''1'')) as fact_value,
            cycledate as cycle_date,
            ' || OptionalColumn || '
            ''' || HopNm || ''' as hopname,
            a.batchid
            FROM ' || HopSchName || '.' || DomainNm || ' a
            INNER JOIN temp_current_batch b ON a.cycle_date = b.cycle_date AND a.batchid = b.batch_id
            ' || JoinCondition || '
            ' || GroupByClause || '';

        ELSEIF chvnMeasureType = 'DistinctCount'
        THEN
            chvnTargetQry = 'N
            INSERT INTO tgttabl
            SELECT
            ''' || DomainTypeNm || ''' as Domain, ''' || chvnMeasureType || ''' as Operation,
            ''' || chvnMeasureField || ''' as control_id,
            ''' || chvnMeasureField || ''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
            COUNT(DISTINCT a.' || chvnMeasureField || ') as fact_value,
            cycledate as cycle_date,
            ' || OptionalColumn || '
            ''' || HopNm || ''' as HopName, a.batchid
            FROM ' || HopSchName || '.' || DomainNm || ' a
            INNER JOIN temp_current_batch b ON a.cycle_date = b.cycle_date AND a.batchid = b.batch_id
            ' || JoinCondition || '
            ' || GroupByClause || '';

        ELSEIF ltrim(rtrim(chvnMeasureType)) = 'Total Count'
        THEN
            chvnTargetQry = 'N
            INSERT INTO tgttabl
            SELECT
            ''' || DomainTypeNm || ''' as Domain, ''' || chvnMeasureType || ''' as Operation,
            ''' || chvnMeasureField || ''' as control_id,
            ''' || chvnMeasureField || ''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
            COUNT(*) as fact_value,
            cycledate as cycle_date,
            ' || OptionalColumn || '
            ''' || HopNm || ''' as HopName, a.batchid
            FROM ' || HopSchName || '.' || DomainNm || ' a
            INNER JOIN temp_current_batch b ON a.cycle_date = b.cycle_date AND a.batchid = b.batch_id
            ' || JoinCondition || '
            ' || GroupByClause || '';

        ELSEIF chvnMeasureType = 'Sum Total'
        THEN
            chvnTargetQry = 'N
            INSERT INTO tgttabl
            SELECT
            ''' || DomainTypeNm || ''' as Domain, ''' || chvnMeasureType || ''' as Operation,
            ''' || chvnMeasureField || ''' as control_id,
            ''' || chvnMeasureField || ''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
            SUM(coalesce(a.' || chvnMeasureField || ',0)) as fact_value,
            cycledate as cycle_date,
            ' || OptionalColumn || '
            ''' || HopNm || ''' as HopName, a.batchid
            FROM ' || HopSchName || '.' || DomainNm || ' a
            INNER JOIN temp_current_batch b ON a.cycle_date = b.cycle_date AND a.batchid = b.batch_id
            ' || JoinCondition || '
            ' || GroupByClause || '';

        ELSEIF chvnMeasureType = 'Sum Total on Year' and chvnMeasureField like '%date%'
        THEN
            chvnTargetQry = 'N
            INSERT INTO tgttabl
            SELECT
            ''' || DomainTypeNm || ''' as Domain, ''' || chvnMeasureType || ''' as Operation,
            ''' || chvnMeasureField || ''' as control_id,
            ''' || chvnMeasureField || ''' as fact_col_name,
            NULL as group_col_name,
            NULL as group_col_val,
            SUM(convert(bigint,coalesce(EXTRACT(YEAR FROM a.' || chvnMeasureField || '),0))) as fact_value,
            cycledate as cycle_date,
            ' || OptionalColumn || '
            ''' || HopNm || ''' as HopName, a.batchid
            FROM ' || HopSchName || '.' || DomainNm || ' a
            INNER JOIN temp_current_batch b ON a.cycle_date = b.cycle_date AND a.batchid = b.batch_id
            ' || JoinCondition || '
            ' || GroupByClause || '';
        END IF;

        --Updating the DynamicSQL table
        CALL financedworchestration.sp_dynamic_sql_log(srcsysname, domain_name, hop, REPLACE(chvnTargetQry,'''',''''''),'chvnTargetQry');

        BEGIN
            EXECUTE(chvnTargetQry);
            COMMIT;
        END;

        Counter := Counter + 1;
    END LOOP;

    loadTargetData =
    'INSERT INTO financedwcontrols.kc2_controllogging (domain,operation,control_id,fact_col_name,group_col_name,group_col_val,fact_val,cycle_date,source_system_name,hop_name,batch_id)
     SELECT distinct "domain",operation,control_id,fact_col_name,group_col_name,group_col_val,fact_val,cycle_date,source_system_name,hop_name,cast(batch_id as bigint)
     FROM (
        SELECT domain,operation,control_id,fact_col_name,group_col_name,group_col_val,fact_val,cycle_date,
               source_system_name,hop_nm,cast(batch_id as bigint) as batch_id
        FROM tgttabl
     ) a;';

    --Updating the DynamicSQL table
    CALL financedworchestration.sp_dynamic_sql_log(srcsysname,domain_name,hop,REPLACE(loadTargetData,'''',''''''),'load_target_data_query');

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        --LOCK financedwcontrols.kc2_controllogging;
        EXECUTE(loadTargetData);
        COMMIT;
    END;

    -- batch tracking call with complete status
    CALL financedworchestration.sp_batch_tracking(srcsysname,hop,domain_name,'complete','update','');

EXCEPTION
WHEN OTHERS THEN
    CALL financedworchestration.sp_batch_tracking(srcsysname,hop,domain_name,'failed','update',SQLERRM);
    RAISE INFO 'error message SQLERRM %', SQLERRM;
    RAISE INFO 'error message SQLSTATE %', SQLSTATE;
END;
$$;