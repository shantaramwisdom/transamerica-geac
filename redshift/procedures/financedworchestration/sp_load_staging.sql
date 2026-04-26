CREATE OR REPLACE PROCEDURE financedorchestration.sp_load_staging(source_system varchar, domain_name varchar)
LANGUAGE plpgsql
AS $$
DECLARE qry NVARCHAR(MAX);
DECLARE select_cols VARCHAR(MAX);
DECLARE insert_cols VARCHAR(MAX);
DECLARE integer_var int;
DECLARE staging_qry VARCHAR(MAX);
DECLARE source_schema VARCHAR(100);
DECLARE staging_view VARCHAR(100);
DECLARE source_table VARCHAR(100);
DECLARE target_table VARCHAR(100);
DECLARE hash_data_string VARCHAR(MAX);
DECLARE hash_natural_key_string VARCHAR(MAX);
DECLARE natural_key_string VARCHAR(MAX);
DECLARE ld_pattern VARCHAR(100);
DECLARE qry_ovrd VARCHAR(MAX) := 'naturalkeyhashvalue , hashvalue, documentid,';
DECLARE qry_ovrd2 VARCHAR(MAX) := 'pointofviewstartdate, pointofviewstopdate, batchid, cycledate, source_system_name,';
DECLARE qry_ovrd3 VARCHAR(MAX) := ' where a.source_system_name = ''' || source_system || '''';
DECLARE qry_ovrd4 VARCHAR(MAX);
DECLARE qry_ovrd5 VARCHAR(MAX);
DECLARE curated_run VARCHAR(MAX) := 'N';
DECLARE domain_ovrrd VARCHAR(MAX) := domain_name;
DECLARE kc2_cnt integer;
DECLARE tgt_schema varchar(max);
DECLARE cdc_flag VARCHAR(1) := 'N';
DECLARE cdc_clause VARCHAR(30) := '';
DECLARE current_database varchar(50);
DECLARE staging_schema VARCHAR(max);
BEGIN
    --Inserting the new record to Batch Tracking
    CALL financedorchestration.sp_batch_tracking(source_system,'staging', domain_name,'in-progress','insert','');

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

    SELECT target_schema || 'staging' INTO staging_schema
    FROM financedorchestration.hash_definition a
    WHERE a.domain_name = domain_ovrrd;

    source_schema := staging_schema;

    target_table := 'stg_' || lower(source_system) || '_' || domain_name;

    IF domain_name NOT IN ('general_ledger_header', 'general_ledger_line_item') THEN
        target_table := replace(replace(target_table,'vantagep55pl','spl'),'vantagep','p');
    END IF;

    --Truncate staging table before loading.
    EXECUTE('TRUNCATE TABLE ' || staging_schema || '.' || target_table);

    SELECT staging_view_name INTO staging_view
    FROM financedorchestration.lkup_staging_view a
    WHERE source_system_name = source_system and a.domain_name = domain_name;

    source_table := staging_view;

    IF staging_view IS NULL THEN
        curated_run := 'Y';
        source_schema := 'financecurated';
        source_table := lower(source_system || '_' || domain_name);
    END IF;

    drop table if exists temp_svv_stg_cols;

    SELECT load_pattern, target_schema INTO ld_pattern, tgt_schema
    FROM financedorchestration.hash_definition hash
    WHERE hash.domain_name = domain_ovrrd;

    --Get column names from the table which are present in natural_key_list
    IF ld_pattern = 'type2' THEN
        --Get column names from the table which are present in natural_key_list
        SELECT natural_key_list
        INTO natural_key_string
        FROM financedorchestration.hash_definition hash
        WHERE hash.domain_name = domain_ovrrd and hash.load_pattern = 'type2';
    END IF;

    natural_key_string := decode(natural_key_string, null, '', natural_key_string || ',');

    IF domain_name in ('financecontract','contractparty') or curated_run = 'Y'
    THEN
        hash_natural_key_string := 'documentid,';
    END IF;

    EXECUTE 'select current_database()' into current_database;

    IF curated_run = 'N' THEN
        create table temp_svv_stg_cols as
        (select table_name as tablename, column_name as columnname, ordinal_position as columnnum
         from svv_columns us where table_catalog = current_database
         and table_schema = staging_schema
         and table_name = staging_view
         and column_name not in ('insert_timestamp','update_timestamp'));
    ELSE
        create temp table temp_svv_stg_cols as
        (select tablename, columnname, columnnum
         from pg_catalog.svv_external_columns
         where schemaname = 'financecurated' and tablename = source_table
         and columnname not in ('recorded_timestamp','documentid','cycle_date','batch_id','cdc_action'));
    END IF;

    IF ld_pattern = 'append' THEN
        select listagg(select_columns, ', ') within group (order by columnnum),
               listagg(insert_columns, ', ') within group (order by columnnum)
        into select_cols, insert_cols
        from
        (select decode(columnname,'cycle_date','cast(a.cycle_date as datetime) as cycle_date','batch_id','cast(a.batch_id as bigint) as batch_id','a.'||columnname) select_columns,
                columnname as insert_columns, columnnum, tablename
         from temp_svv_stg_cols
         where columnname not in
               (select distinct trim(SPLIT_PART(exclude_field_list,',',id::int)) as value_txt from (select row_number() over (order by 1) as id from (select 1
               from stl_connection_log a
               limit 100)) a
               cross join financedorchestration.hash_definition hash where hash.domain_name = domain_ovrrd and trim(value_txt) != '' and hash.load_pattern = 'append')
         group by tablename);
        qry_ovrd2 := '';
    ELSIF curated_run = 'Y' THEN
        select listagg(select_columns, ', ') within group (order by columnnum),
               listagg(insert_columns, ', ') within group (order by columnnum),
               'SHA2(' || listagg(distinct CAST('ISNULL(CAST('|| select_columns || ' AS NVARCHAR(MAX)),'''')' AS NVARCHAR(MAX)), ',') || '::256)' 
        into select_cols, insert_cols, hash_data_string
        from
        (select decode(columnname,'cycle_date','cast(a.cycle_date as datetime) as cycle_date','batch_id','cast(a.batch_id as bigint) as batch_id','a.'||columnname) select_columns,
                columnname as insert_columns, columnnum, tablename
         from temp_svv_stg_cols
         where columnname not in
               (select distinct trim(SPLIT_PART(natural_key_list,',',id::int)) as value_txt from (select row_number() over (order by 1) as id from (select 1
               from stl_connection_log a
               limit 100)) a
               cross join financedorchestration.hash_definition hash where hash.domain_name = domain_ovrrd and trim(value_txt) != '' and hash.load_pattern = 'type2')
         group by tablename);
    ELSE
        select listagg(select_columns, ', ') within group (order by columnnum),
               listagg(insert_columns, ', ') within group (order by columnnum),
               'SHA2(' || listagg(distinct CAST('ISNULL(CAST('|| select_columns || ' AS NVARCHAR(MAX)),'''')' AS NVARCHAR(MAX)), ',') || '::256)' 
        into select_cols, insert_cols, hash_data_string
        from
        (select decode(columnname,'cycle_date','cast(a.cycle_date as datetime) as cycle_date','batch_id','cast(a.batch_id as bigint) as batch_id','a.'||columnname) select_columns,
                columnname as insert_columns, columnnum, tablename
         from temp_svv_stg_cols
         where columnname not in
               (select distinct trim(SPLIT_PART(natural_key_list,',',id::int)) as value_txt from (select row_number() over (order by 1) as id from (select 1
               from stl_connection_log a
               limit 100)) a
               cross join financedorchestration.hash_definition hash where hash.domain_name = domain_ovrrd and trim(value_txt) != '' and hash.load_pattern = 'type2')
         group by tablename);
    END IF;
else
	select listagg(select_columns, ', ') within group (order by columnnum)
		, listagg(insert_columns, ', ') within group (order by columnnum)
		, 'SHA2(' || LISTAGG(distinct CAST('ISNULL(CAST(' || select_columns || ' AS NVARCHAR(MAX)),'''')' AS NVARCHAR(MAX)) , ',') || '::' ) within group (order by columnnum) || ',256)'
	into select_cols, insert_cols, hash_data_string
	from
	(select decode(columnname,'cycle_date','cast(a.cycle_date as datetime)', 'batch_id','cast(a.batch_id as bigint)', 'a.' || columnname) select_columns
	, columnname as insert_columns, columnnum, tablename
	from temp_svv_stg_cols
	where columnname not in
	(select distinct trim(SPLIT_PART(natural_key_list,',',id::int)) as value_txt from (select row_number() over (order by 1) as id from (select 1
	limit 100)) a
	cross join financedworchestration.hash_definition hash where hash.domain_name = domain_ovrrd and trim(value_txt) != '' and hash.load_pattern = 'type2'
	)
	group by tablename);
end if;

if select_cols not like '%activity1035exchangeindicator%' then
	select_cols = REPLACE(select_cols,'1035exchangeindicator','1035exchangeindicator');
	insert_cols = REPLACE(insert_cols,'1035exchangeindicator','1035exchangeindicator');
	hash_data_string = REPLACE(hash_data_string,'1035exchangeindicator','1035exchangeindicator');
end if;

hash_data_string = decode(hash_data_string,'', '', hash_data_string || ',');
qry_ovrrd4 = hash_natural_key_string + hash_data_string + hash_natural_key_string + ',';

if source_system = 'refdata' then
	qry_ovrrd = '';
	if ld_pattern = 'type2' then
		qry_ovrrd = 'naturalkeyhashvalue, hashvalue,';
		qry_ovrrd4 = hash_natural_key_string + hash_data_string + ',';
	end if;
end if;

if source_system in ('aah','refdata') then
	natural_key_string = '';
	qry_ovrrd3 = '';
	qry_ovrrd4 = '';
elsif ld_pattern = 'append' then
	natural_key_string = '';
	qry_ovrrd4 = '';
end if;

qry_ovrrd5 = qry_ovrrd2;
qry_ovrrd6 = natural_key_string;

if curated_run = 'Y' and ld_pattern = 'type2' then
	select do_cdc_flag INTO cdc_flag
	from financedworchestration.domain_tracker a
	where a.source_system_name = source_system and a.domain_name = domain_name;
	if cdc_flag='Y' then
		cdc_clause='a.cdc_action=''INSERT'' and';
	end if;
end if;

if curated_run = 'Y' then
	if ld_pattern = 'append' then
		qry_ovrrd2 = 'a.batchid, cycledate,';
		qry_ovrrd2 = 'b.batchid, b.cycle_date, a.cycle_date,';
		qry_ovrrd3 = ', b, financedworchestration.current_batch b where a.cycle_date = b.cycle_date and a.batch_id = b.batch_id and b.domain_name = ''' + domain_name + ''' and b.source_system_name = ''' + source_system + '''';
		natural_key_string = natural_key_string;
	elsif ld_pattern = 'type2' then
		qry_ovrrd2 = 'b.cycle_date, ''9999-12-31'', b.batch_id, b.cycle_date, b.source_system_name,';
		qry_ovrrd3 = ', b, financedworchestration.current_batch b where ' + cdc_clause + ' a.cycle_date = b.cycle_date and a.batch_id = b.batch_id and b.domain_name = ''' + domain_name + ''' and b.source_system_name = ''' + source_system + '''';
		natural_key_string = natural_key_string;
	else
		RAISE EXCEPTION 'Invalid Combination';
	end if;
end if;

staging_qry = 'INSERT INTO ' + staging_schema + '.' + target_table + '(' + qry_ovrrd + qry_ovrrd6 + qry_ovrrd5 + insert_cols + ')
(select ' + qry_ovrrd4 + natural_key_string + qry_ovrrd2 + select_cols + ' from ' + source_schema + '.' + source_table + qry_ovrrd3 + ')';

--Update the DynamicSQL table
CALL financedworchestration.sp_dynamic_sql_log(source_system, domain_name, 'staging', REPLACE(staging_qry, '''', ''''''), 'staging_qry');

EXECUTE staging_qry;
commit;

-- Run OC Controls
CALL financewdcontrols.sp_oc_findh_src(domain_name,'staging',source_system);
CALL financewdcontrols.sp_oc_find_tgt(domain_name,'staging',source_system);
CALL financewdcontrols.sp_oc_findh_controls(domain_name,'staging',source_system);

EXECUTE 'select count(*) from financedwcontrols.lkup_kc2_control_measures
	where source_system_name = ''' + source_system + ''' and is_active = ''Y'' and key_control_type = ''kc2''
	and domain_name = ''' + domain_name + '''' into kc2_cnt;
IF kc2_cnt > 0 THEN
	CALL financedwcontrols.sp_kc2_findh_src(source_system, domain_name, 'curated');
END IF;

--Batch tracking call
CALL financedworchestration.sp_batch_tracking(source_system, 'staging', domain_name, 'complete', 'update', '');

EXCEPTION
WHEN OTHERS THEN
	CALL financedworchestration.sp_batch_tracking(source_system, 'staging', domain_name, 'failed', 'update', SQLERRM);
	RAISE INFO 'Error message SQLERRM %', SQLERRM;
	RAISE INFO 'Error message SQLSTATE %', SQLSTATE;
END;
$$;