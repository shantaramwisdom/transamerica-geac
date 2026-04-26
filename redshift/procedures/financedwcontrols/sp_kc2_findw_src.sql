CREATE OR REPLACE PROCEDURE financedwcontrols.sp_kc2_findw_src(source_system_name varchar, domain_name varchar, hop_name varchar)
LANGUAGE plpgsql
AS $$
--Declare Local Variables
DECLARE DomainNm Varchar(100);
DECLARE MaxRowNum int;
DECLARE MaxPubNum int;
DECLARE chvnMeasureType Varchar(50);
DECLARE chvnMeasureField Varchar(150);
DECLARE DomainTypeNm Varchar(100);
DECLARE chvnTargetQry nvarchar(max);
DECLARE loadTargetData nvarchar(max);
DECLARE Counter int;
DECLARE HopNm Varchar(50);
DECLARE JoinCondition Varchar(255);
DECLARE GroupByClause Varchar(255);
DECLARE OptionalColumn Varchar(255);
DECLARE moveoldstageqry Varchar(max);
DECLARE measures Varchar(max);
DECLARE action_type_qry NVARCHAR(MAX);
DECLARE pov_qry NVARCHAR(MAX);
DECLARE qry Varchar(max);
DECLARE qry1 NVARCHAR(MAX);
DECLARE source_table NVARCHAR(MAX) := lower(source_system_name || '_' || domain_name);
DECLARE hash_data_string nvarchar(max);
DECLARE select_cols nvarchar(max);
DECLARE insert_cols nvarchar(max);
DECLARE selecttemptablefields Varchar(max);
DECLARE deletecontrols nvarchar(max);
DECLARE dummy Varchar(10);
DECLARE domain_ovr Varchar(100);
DECLARE load_dmn Varchar(100);
DECLARE id_pattern Varchar(50);
DECLARE tgt_schema NVARCHAR(100);
DECLARE sql_word NVARCHAR(max) := ('select * from tblcdc where actiontype = ''INSERT''');
DECLARE stop_date_format VARCHAR(50);
DECLARE cdc_flag NVARCHAR(2) := '';
DECLARE Distinction nvarchar(max) := '';
DECLARE TotalCountUnion nvarchar(max) := '';
DECLARE GEBDUnion nvarchar(max) := '';
DECLARE SumTotalUnion nvarchar(max) := '';
DECLARE STJOINUnion nvarchar(max) := '';
DECLARE cdc_select nvarchar(max) := '';
DECLARE pov_start_stop_date_update_qry NVARCHAR(MAX);
DECLARE pov_stop_date_update_qry NVARCHAR(MAX);
DECLARE pov_stop_date_active_qry NVARCHAR(MAX);
DECLARE multi_batch_count NVARCHAR(MAX);
DECLARE multibatch_run_flag NVARCHAR(MAX);

BEGIN
    hop := 'kc2_findw_src_' || hop_name;
    HopNm := hop_name;
    DomainNm := domain_name;

    -- batch tracking call with in-progress status
    CALL financedworchestration.sp_batch_tracking(source_system_name, hop, domain_name, 'in-progress', 'insert','') ;

    DROP TABLE IF EXISTS temp_current_batch;

    BEGIN
        CREATE TEMP TABLE temp_current_batch AS
        (SELECT * FROM financedworchestration.current_batch a
            where a.source_system_name = source_system_name and a.domain_name = domain_name);
        commit;
    END;

    CASE WHEN domain_name = 'party' and source_system_name = 'BaNCS'
        THEN
            domain_ovr := 'party_bancs';
        WHEN domain_name = 'party' and source_system_name like 'VantagePX'
        THEN
            domain_ovr := 'party_vantage';
        WHEN domain_name = 'contractoptionfund' and source_system_name = 'BaNCS'
        THEN
            domain_ovr := 'contractoptionfund_bancs';
        WHEN domain_name = 'contractoptionfund' and source_system_name like 'VantagePX'
        THEN
            domain_ovr := 'contractoptionfund_vantage';
        WHEN domain_name = 'partycontractrelationship' and source_system_name = 'Bestow'
        THEN
            domain_ovr := 'partycontractrelationship_bestow';
        ELSE
            domain_ovr := domain_name;
    END CASE;

    select top 1 decode(do_intraday_multibatch_run_flag, 'Y', 'TIMESTAMP', 'DATE'), do_intraday_multibatch_run_flag into stop_date_format, multibatch_run_flag
    from financedworchestration.domain_tracker a
    where a.source_system_name = source_system_name
    and a.domain_name = domain_name;

    SELECT target_schema, load_pattern into tgt_schema, id_pattern
    FROM financedworchestration.hash_definition hash
    WHERE hash.domain_name = domain_ovr;

    deletecontrols = N'
        SELECT COUNT(*) from financedwcontrols.kc2_controllogging
        where
            hop_name like ''curated%''
            and (batch_id, cycle_date, domain, source_system_name) in
                (select distinct batch_id, cycle_date, domain_name, source_system_name
                 from temp_current_batch
                 where domain_name = ''' + domain_name + ''' and source_system_name = ''' + source_system_name + ''')';

    BEGIN
        SELECT 'X' INTO dummy;
        COMMIT;
    END;

    BEGIN
        EXECUTE deletecontrols INTO Counter;
        COMMIT;
    END;

    -- delete logging data if batch_id already exists
    IF Counter > 0
    THEN
        deletecontrols = N'
            delete from financedwcontrols.kc2_controllogging
            where hop_name like ''curated%''
            and (batch_id, cycle_date, domain, source_system_name) in
                (select distinct batch_id, cycle_date, domain_name, source_system_name
                 from temp_current_batch
                 where domain_name = ''' + domain_name + ''' and source_system_name = ''' + source_system_name + ''')';

        CALL financedworchestration.sp_dynamic_sql_log(source_system_name, domain_name, hop, REPLACE(deletecontrols,'''',''''''),'DeleteOldRecs');

        BEGIN
            SELECT 'X' INTO dummy;
            COMMIT;
        END;

        BEGIN
            --LOCK financedwcontrols.kc2_controllogging;
            EXECUTE deletecontrols;
            COMMIT;
        END;
    END IF;

    drop table if exists temp_svw_stg_cols;
    create temp table temp_svw_stg_cols as
    (select tablename, columnname, columnnum
     from pg_catalog.svv_external_columns
     where schemaname = 'financedwcurated' and tablename = source_table
     and columnname not in ('recorded_timestamp','updated_from_desc','documentid','cycle_date','batch_id'));

    select listagg(select_columns, ',' ) within group (order by columnname),
           listagg(insert_columns, ',' ) within group (order by columnname),
           'SHA2(' || LISTAGG(distinct CAST('ISNULL(' || select_columns || ','''')' AS NVARCHAR(MAX)), ',') AS NVARCHAR(MAX)) || ''',256)'
    into select_cols, insert_cols, hash_data_string
    from (select decode(columnname,'cycle_date','cast(a.cycle_date as datetime) as cycle_date',
            'batch_id','cast(a.batch_id as bigint) as batch_id', 'a.' || columnname) select_columns,
            columnname as Insert_columns, columnnum, tablename, columnname
          from temp_svw_stg_cols
          where columnname not in
            (select distinct trim(split_part(natural_key_list || ',' || exclude_field_list,',',id::int)) as value_txt
             from (select row_number() over (order by 1) as id from (select 1 limit 1000) x) n
             cross join financedworchestration.hash_definition hash
             where hash.domain_name = domain_ovr and trim(value_txt) != ''
             and hash.load_pattern = 'type2'
             )) group by tablename;

    if select_cols not like '%activity103ExchangeIndicator%' then
        select_cols := REPLACE(select_cols,'103ExchangeIndicator','103ExchangeIndicator');
        insert_cols := REPLACE(insert_cols,'103ExchangeIndicator','103ExchangeIndicator');
        hash_data_string := REPLACE(hash_data_string,'103ExchangeIndicator','103ExchangeIndicator');
    end if;

        measures = 'SELECT CASE WHEN agg_values = '''' THEN '''' ELSE agg_values END FROM(
                select distinct * , LISTAGG(decode(measure_field,''recdcount'','''',measure_field), '','') WITHIN GROUP (ORDER BY Domain_name) agg_values
                from financedwcontrols.lkup_kc2_control_measures
                where is_active=''Y'' and domain_name = ''' + DomainNm + ''' and source_system_name = ''' + source_system_name + '''
                and key_control_type = ''kc2'')
                group by Domain_name';

    EXECUTE measures INTO selecttemptablefields;

    if hop_name = 'curated' and id_pattern = 'type2'
    then
        select do_cdc_flag into cdc_flag
        from financedworchestration.domain_tracker a
        where a.source_system_name = source_system_name and a.domain_name = domain_name;
        if cdc_flag = 'Y'
        then
            cdc_select := 'cdc_action';
        end if;
    end if;

    IF id_pattern = 'type2'
    THEN
        if stop_date_format is null or upper(stop_date_format) = 'DATE' then
            stop_dt_ovr := 'COALESCE((LEAD(pointofviewstartdate) over (partition by naturallykeyhashvalue order by pointofviewstartdate, pointofviewstopdate) - 1,''9999-12-31'')) AS new_point_of_view_stop_date ';
        else
            stop_dt_ovr := 'COALESCE(dateadd(minute, -1, (LEAD(pointofviewstartdate) over (partition by naturallykeyhashvalue order by batch_id, pointofviewstartdate, pointofviewstopdate))),''9999-12-31'') AS new_point_of_view_stop_date ';
        end if;

        DROP TABLE IF EXISTS drivertable;
        qry1 := 'CREATE TEMPORARY TABLE drivertable as
                 SELECT documentid naturallykeyhashvalue, ' + cdc_select + ' , ' +
                 hash_data_string + ' AS hashvalue, a.cycle_date as cycle_date,
                 cast(''9999-12-31'' as TIMESTAMP) as pointofviewstopdate, a.batch_id as batch_id, a.source_system_name as source_system_name ' + selecttemptablefields + '
                 FROM financedwcurated.' + source_table + ' a
                 INNER JOIN temp_current_batch current_batch USING (batch_id, cycle_date)
                 WHERE domain_name = ''' + domain_name + ''' and current_batch.source_system_name = ''' + source_system_name + '''';

        --Updating the DynamicSQL table
        CALL financedworchestration.sp_dynamic_sql_log(source_system_name, domain_name, hop, REPLACE(qry1,'''',''''''),'drivertable_insert');

        BEGIN
            EXECUTE(qry1);
            COMMIT;
        END;

        DROP TABLE IF EXISTS tblcdc;

        qry := 'CREATE TEMPORARY TABLE tblcdc as
                SELECT tgt.naturallykeyhashvalue, hashvalue' + selecttemptablefields + ',tgt.cycledate as cycle_date,
                pointofviewstartdate,pointofviewstopdate, ''Target'' as sourcetable, cast(null as varchar(50)) actiontype,
                batch_id,source_system_name
                FROM ' + tgt_schema + '.' + DomainNm + ' tgt
                INNER JOIN
                    (Select Min(cycle_date) cycle_date, domain, naturallykeyhashvalue
                     from drivertable
                     group by naturallykeyhashvalue) Staging
                ON Staging.naturallykeyhashvalue = tgt.naturallykeyhashvalue
                AND tgt.pointofviewstopdate = ''9999-12-31''
                UNION ALL
                SELECT naturallykeyhashvalue, hashvalue' + selecttemptablefields + ',cycle_date,
                pointofviewstartdate,pointofviewstopdate, ''Staging'' as sourcetable, cast(null as varchar(50)) actiontype,
                batch_id,source_system_name
                FROM drivertable';

        --Updating the DynamicSQL table
        CALL financedworchestration.sp_dynamic_sql_log(source_system_name,domain_name,hop,REPLACE(qry,'''',''''''),'tblcdc_insert');

        BEGIN
            EXECUTE(qry);
            COMMIT;
        END;

        --Updating the action_type column, Update -> find, Delete -> from staging if hash value matches with next record ,Insert -> For record from staging
        action_type_qry := N' UPDATE tblcdc
                              set actiontype = tmp.actiontype
                              FROM tblcdc c
                              INNER JOIN (
                                  SELECT naturallykeyhashvalue,pointofviewstartdate,pointofviewstopdate,batch_id,
                                         CASE WHEN sourcetable = ''Target'' THEN ''UPDATE''
                                              WHEN hashvalue = LAG(hashvalue) OVER(Partition by naturallykeyhashvalue order by pointofviewstartdate,batch_id)
                                              THEN ''DELETE''
                                              ELSE ''INSERT'' END AS actiontype
                                  FROM tblcdc ) tmp
                              ON c.naturallykeyhashvalue = tmp.naturallykeyhashvalue
                              AND c.pointofviewstartdate = tmp.pointofviewstartdate
                              AND c.pointofviewstopdate = tmp.pointofviewstopdate
                              AND c.batch_id = tmp.batch_id ';

        --Updating the DynamicSQL table
        CALL financedworchestration.sp_dynamic_sql_log(source_system_name,domain_name,hop,REPLACE(action_type_qry,'''',''''''),'action_type_qry');

        BEGIN
            EXECUTE action_type_qry;
            COMMIT;
        END;

        IF DomainNm IN ('contract', 'activity', 'claim','claimbenefit')
        THEN
            WITH CTE AS (
                SELECT naturallykeyhashvalue, contractnumber, cycle_date,actiontype ,hashvalue ,count(*) OVER(PARTITION BY naturallykeyhashvalue, contractnumber, cycle_date ) as count
                FROM tblcdc wc
            ),
            CTE2 AS (
                SELECT naturallykeyhashvalue, contractnumber, cycle_date,actiontype ,hashvalue,
                       case when lag(actiontype) OVER(PARTITION by naturallykeyhashvalue, contractnumber, cycle_date order by naturallykeyhashvalue, contractnumber, cycle_date ) != actiontype
                       then 'ignore'
                       when actiontype IN ('INSERT','UPDATE') and lag(actiontype) OVER(PARTITION by naturallykeyhashvalue, contractnumber, cycle_date order by naturallykeyhashvalue, contractnumber, cycle_date ) in ('INSERT','UPDATE') then 'delete'
                       else 'ignored' end as action_type
                from cte where count > 2 )
            UPDATE tblcdc SET actiontype = 'DELETE'
            FROM tblcdc oc, CTE2
            WHERE oc.naturallykeyhashvalue = CTE2.naturallykeyhashvalue AND oc.cycle_date = CTE2.cycle_date
            AND oc.hashvalue = CTE2.hashvalue AND oc.contractnumber = CTE2.contractnumber AND CTE2.action_type1 = 'delete' ;
        END IF;

        pov_qry := 'UPDATE tblcdc set pointofviewstopdate = tmp.new_point_of_view_stop_date
                    FROM tblcdc as tblcdc , (SELECT naturallykeyhashvalue,pointofviewstartdate,pointofviewstopdate,batch_id,' + stop_dt_ovr + '
                    FROM tblcdc WHERE actiontype <> ''DELETE'') tmp
                    WHERE tblcdc.naturallykeyhashvalue = tmp.naturallykeyhashvalue
                    AND tblcdc.pointofviewstartdate = tmp.pointofviewstartdate
                    AND tblcdc.pointofviewstopdate = tmp.pointofviewstopdate
                    AND tblcdc.batch_id = tmp.batch_id';

        --Updating the DynamicSQL table
        CALL financedworchestration.sp_dynamic_sql_log(source_system_name,domain_name,hop,REPLACE(pov_qry,'''',''''''),'pov_qry');

        BEGIN
            EXECUTE pov_qry;
            COMMIT;
        END;

        UPDATE tblcdc SET actiontype = 'DELETE'
        WHERE sourcetable = 'Target' AND pointofviewstopdate = '9999-12-31';

        if multibatch_run_flag = 'Y'
        then
            DROP TABLE IF EXISTS temp_correct_start_stop;
            pov_start_stop_date_update_qry := 'create temp table temp_correct_start_stop as
                                               SELECT naturallykeyhashvalue, actiontype, sourcetable, cycle_date, batch_id,
                                               lag(pointofviewstartdate) over (partition by naturallykeyhashvalue order by batch_id) as lag_pointofviewstartdate,
                                               lag(sourcetable) over (partition by naturallykeyhashvalue order by batch_id) as lag_sourcetable
                                               from tblcdc
                                               where actiontype = ''DELETE'' group by 1, 2, having count(*) >1)';

            CALL financedworchestration.sp_dynamic_sql_log(source_system_name,domain_name,hop,REPLACE(pov_start_stop_date_update_qry,'''',''''''),'temp_correct_start_stop');

            BEGIN
                EXECUTE(pov_start_stop_date_update_qry);
                COMMIT;
            END;

            EXECUTE 'select count(*) from temp_correct_start_stop' into multi_batch_count;

            if multi_batch_count > 0
            then
                pov_start_stop_date_update_qry := 'update tblcdc wrk set pointofviewstartdate = ts.new_startdate, pointofviewstopdate = ts.new_stopdate from
                                                   (select naturallykeyhashvalue, pointofviewstopdate, hashvalue, batch_id ,
                                                    case when wrk_sourcetable = ''Staging'' then
                                                    pointofviewstartdate::date + interval ''00 hours'' + ((batch_rank_target + batch_rank - 1) * interval ''30 minutes'')
                                                    else pointofviewstartdate end as new_startdate,
                                                    case when (wrk_sourcetable = ''Staging'' and pointofviewstopdate != ''9999-12-31'') or (wrk_sourcetable = ''Target'')
                                                    then new_startdate + interval ''30 minutes'' - interval ''1 minute''
                                                    else pointofviewstopdate end as new_stopdate
                                                    from (select a.sourcetable as wrk_sourcetable, row_number() over (partition by cycle_date, naturallykeyhashvalue order by batch_id) as batch_rank,
                                                    case when lag_sourcetable = ''Target'' then nvl(floor(datediff(MINUTE, DATE_TRUNC(''day'', lag_pointofviewstartdate), lag_pointofviewstartdate) / 30), 0)
                                                    else 0 end as batch_rank_target
                                                    from tblcdc w
                                                    join temp_correct_start_stop using (naturallykeyhashvalue, actiontype, sourcetable, cycle_date, batch_id)) ranked_updates) ts
                                                    where wrk.naturallykeyhashvalue = ts.naturallykeyhashvalue and wrk.pointofviewstartdate = ts.pointofviewstartdate
                                                    and wrk.hashvalue = ts.hashvalue and wrk.batch_id = ts.batch_id ';
            end if;
        end if;
            CALL financedworchestration.sp_dynamic_sql_log(source_system_name, domain_name, hop, REPLACE(pov_start_stop_date_update_qry,'''',''''''),'type2_pov_start_stop_date_update_qry');

            --Execute query
            BEGIN
                EXECUTE(pov_start_stop_date_update_qry);
                COMMIT;
            END;

        end if;
    end if;

ELSEIF id_pattern = 'append' and tgt_schema = 'erpdw'
THEN
    sql_ovrd = 'financedwcurated.' + source_table;
ELSE
    RAISE EXCEPTION 'Invalid Combination';
END IF;

DROP TABLE IF EXISTS tgttabl;
CREATE TEMPORARY TABLE tgttabl (
    "domain" VARCHAR(255),
    "operation" VARCHAR(255),
    "control_id" VARCHAR(255),
    "fact_col_name" VARCHAR(255),
    "group_col_name" VARCHAR(255),
    "group_col_val" VARCHAR(255),
    "fact_val" VARCHAR(255),
    "cycle_date" DATE,
    "src_system_name" VARCHAR(255),
    "hop_nm" VARCHAR(255),
    "batch_id" VARCHAR(255)
);

DROP TABLE IF EXISTS getmeasures;
measures = N'
CREATE TEMPORARY TABLE getmeasures as
select row_number() over(order by measuretype) as rownum from (
select distinct Domain_name as domaintype,Measure_Type as measuretype, Measure_Field as measurefield
from financedwcontrols.lkup_kc2_control_measures
where is_active = ''Y'' and domain_name = ''' + DomainNm + ''' and source_system_name = ''' + source_system_name + ''' and key_control_type = ''kc2''
) x';

--Updating the DynamicSQL table
CALL financedworchestration.sp_dynamic_sql_log(source_system_name,domain_name,hop,REPLACE(measures,'''',''''''),'measures');

--execute measures;
BEGIN
    EXECUTE(measures);
    COMMIT;
END;

SELECT MAX(rownum) into MaxRowNum from getmeasures;
Counter = 1;
commit;

while (Counter <= MaxRowNum)
LOOP
    SELECT measuretype, measurefield, domaintype
    into chvMeasureType, chvMeasureField, DomainTypeNm
    FROM getmeasures
    WHERE rownum=Counter;

    JoinCondition = ' and b.source_system_name = a.source_system_name and b.source_system_name = '''||source_system_name||''' and b.domain_name = '''|| domain_name||''' ';
    OptionalColumn = 'b.source_system_name as source_system_name,';

    GroupByClause =
    case
        when chvMeasureType = 'Sum Total on Year' and chvMeasureField not like '%date%' then 'group by a.'||chvMeasureField||',a.batch_id,b.source_system_name,a.cycle_date'
        when chvMeasureType in ('DistinctCount','Sum Total','Total Count') then 'group by b.source_system_name,a.batch_id,a.cycle_date'
        else 'group by a.'||chvMeasureField||',b.source_system_name,a.batch_id,a.cycle_date' end;

    IF cdc_flag='Y' and chvMeasureType = 'DistinctCount'
    THEN
        DistinctCountUnion = N'
        union all
        select
        '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
        '''||chvMeasureField||''' as control_id,
        '''||chvMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
        Count(Distinct a.'||chvMeasureField||') as fact_value,
        a.cycle_date,
        '||OptionalColumn||'
        ''curated_adjust'' as HopName,a.batch_id
        from (select * from drivertable where cdc_action = ''DELETE'') a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        '||JoinCondition||'
        '||GroupByClause;

    ELSEIF cdc_flag='Y' and ltrim(rtrim(chvMeasureType)) = 'Total Count'
    THEN
        TotalCountUnion = N'
        union all
        select
        '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
        '''||chvMeasureField||''' as control_id,
        '''||chvMeasureField||''' as fact_col_name, NULL as group_col_name, NULL as group_col_val,
        Count(*) as fact_value,
        a.cycle_date,
        '||OptionalColumn||'
        ''curated_adjust'' as HopName,a.batch_id
        from (select * from drivertable where cdc_action = ''DELETE'') a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        '||JoinCondition||'
        '||GroupByClause;

    ELSEIF cdc_flag='Y' and chvMeasureType = 'Count grouped by distinct value'
    THEN
        CGBDVUnion = '
        union all
        select
        '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
        '''||chvMeasureField||''' as control_id,
        NULL as fact_col_name, '''||chvMeasureField||''' as group_col_name, '''||chvMeasureField||''' as group_col_val,
        Count(isnull(a.'||chvMeasureField||',1)) as fact_value,
        a.cycle_date,
        '||OptionalColumn||'
        ''curated_adjust'' as HopName,a.batch_id
        from (select * from drivertable where cdc_action = ''DELETE'') a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        '||JoinCondition||'
        '||GroupByClause;

    ELSEIF cdc_flag='Y' and chvMeasureType = 'Sum Total'
    THEN
        SumTotalUnion = '
        union all
        select
        '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
        '''||chvMeasureField||''' as control_id,
        '''||chvMeasureField||''' as fact_col_name, NULL as group_col_name , NULL as group_col_val,
        SUM(coalesce('||chvMeasureField||',0)) as fact_value,
        a.cycle_date,
        '||OptionalColumn||'
        ''curated_adjust'' as HopName,a.batch_id
        from (select * from drivertable where cdc_action = ''DELETE'') a
        inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
        '||JoinCondition||'
        '||GroupByClause;
        ELSEIF cdc_flag='Y' and chvMeasureType = 'Sum Total on Year' and chvMeasureField like '%date%'
        THEN
            STYONUnion = '
            union all
            select
            '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
            '''||chvMeasureField||''' as control_id,
            '''||chvMeasureField||''' as fact_col_name,
            null as group_col_name,
            null as group_col_val,
            sum(Convert(BigInt,coalesce(EXTRACT(YEAR FROM a.'||chvMeasureField||'),0))) as fact_value,
            a.cycle_date,
            '||OptionalColumn||'
            ''curated_adjust'' as HopName,a.batch_id
            from (select * from drivertable where cdc_action = ''DELETE'') a
            inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
            '||JoinCondition||'
            '||GroupByClause;

        END IF;

        IF chvMeasureType = 'DistinctCount'
        THEN
            chvTargetQry = N'
            INSERT INTO tgttabl
            select
            '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
            '''||chvMeasureField||''' as control_id,
            '''||chvMeasureField||''' as fact_col_name, NULL as group_col_name , NULL as group_col_val,
            Count(Distinct a.'||chvMeasureField||') as fact_value,
            a.cycle_date,
            '||OptionalColumn||'
            '''||HopNm||''' as HopName,a.batch_id
            from '|| sql_ovrd ||'
            inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
            '||JoinCondition||'
            '||GroupByClause||'
            '||DistinctCountUnion||'';

        ELSEIF ltrim(rtrim(chvMeasureType)) = 'Total Count'
        THEN
            chvTargetQry = N'
            INSERT INTO tgttabl
            select
            '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
            '''||chvMeasureField||''' as control_id,
            '''||chvMeasureField||''' as fact_col_name, NULL as group_col_name , NULL as group_col_val,
            Count(*) as fact_value,
            a.cycle_date,
            '||OptionalColumn||'
            '''||HopNm||''' as HopName,a.batch_id
            from '|| sql_ovrd ||'
            inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
            '||JoinCondition||'
            '||GroupByClause||'
            '||TotalCountUnion||'';

        ELSEIF chvMeasureType = 'Count grouped by distinct value'
        THEN
            chvTargetQry = N'
            INSERT INTO tgttabl
            select
            '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
            '''||chvMeasureField||''' as control_id,
            NULL as fact_col_name, '''||chvMeasureField||''' as group_col_name,
            '''||chvMeasureField||''' as group_col_val,
            Count(isnull(a.'||chvMeasureField||',1)) as fact_value,
            a.cycle_date,
            '||OptionalColumn||'
            '''||HopNm||''' as HopName,a.batch_id
            from '|| sql_ovrd ||'
            inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
            '||JoinCondition||'
            '||GroupByClause||'
            '||CGBDVUnion||'';

        ELSEIF chvMeasureType = 'Sum Total'
        THEN
            chvTargetQry = N'
            INSERT INTO tgttabl
            select
            '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
            '''||chvMeasureField||''' as control_id,
            '''||chvMeasureField||''' as fact_col_name, NULL as group_col_name , NULL as group_col_val,
            SUM(coalesce('||chvMeasureField||',0)) as fact_value,
            a.cycle_date,
            '||OptionalColumn||'
            '''||HopNm||''' as HopName,a.batch_id
            from '|| sql_ovrd ||'
            inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
            '||JoinCondition||'
            '||GroupByClause||'
            '||SumTotalUnion||'';

        ELSEIF chvMeasureType = 'Sum Total on Year' and chvMeasureField like '%date%'
        THEN
            chvTargetQry = N'
            INSERT INTO tgttabl
            select
            '''||DomainTypeNm||''' as Domain, '''||chvMeasureType||''' as Operation,
            '''||chvMeasureField||''' as control_id,
            '''||chvMeasureField||''' as fact_col_name,
            null as group_col_name,
            null as group_col_val,
            sum(Convert(BigInt,coalesce(EXTRACT(YEAR FROM a.'||chvMeasureField||'),0))) as fact_value,
            a.cycle_date,
            '||OptionalColumn||'
            '''||HopNm||''' as HopName,a.batch_id
            from '|| sql_ovrd ||'
            inner join temp_current_batch b on a.cycle_date = b.cycle_date and a.batch_id = b.batch_id
            '||JoinCondition||'
            '||GroupByClause||'
            '||STYONUnion||'';

        END IF;

        --Updating the DynamicSQL table
        CALL financedworchestration.sp_dynamic_sql_log(source_system_name,domain_name,hop,REPLACE(chvTargetQry,'''',''''''),'chvTargetQry');

        BEGIN
            EXECUTE(chvTargetQry);
            COMMIT;
        END;

                Counter := Counter+1;
END LOOP;

--================Load Controls Table================

loadTargetData =
'INSERT INTO financedwcontrols.kc2_controllogging (domain, operation, control_id, fact_col_name, group_col_name, group_col_val, fact_val, cycle_date, source_system_name, hop_name, batch_id)
 SELECT distinct "domain", operation, control_id, fact_col_name, group_col_name, group_col_val, fact_val, cycle_date, source_system_name, hop_name, cast(batch_id as bigint)
 FROM (
   SELECT domain, operation, control_id, fact_col_name, group_col_name, group_col_val, fact_val, cycle_date,
          src_system_name as source_system_name,
          hop_nm, cast(batch_id as bigint) as batch_id
   FROM tgttabl
 );';

--Updating the DynamicSQL table
CALL financedworchestration.sp_dynamic_sql_log(source_system_name, domain_name, hop, REPLACE(loadTargetData,'''',''''''),'load_target_data_query');

BEGIN
    SELECT 'X' INTO dummy;
    COMMIT;
END;

BEGIN
    --LOCK financedwcontrols.kc2_controllogging;
    EXECUTE(loadTargetData);
    COMMIT;
END;

-- batch tracking call with in-progress status
CALL financedworchestration.sp_batch_tracking(source_system_name, hop, domain_name, 'complete', 'update', '');

EXCEPTION
WHEN OTHERS THEN
    -- batch tracking call with failed status
    CALL financedworchestration.sp_batch_tracking(source_system_name, hop, domain_name, 'failed', 'update', SQLERRM);
    RAISE INFO 'error message SQLERRM: %', SQLERRM;
    RAISE INFO 'error message SQLSTATE: %', SQLSTATE;
END;
$$;