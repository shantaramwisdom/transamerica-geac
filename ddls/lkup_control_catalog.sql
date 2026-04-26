CREATE TABLE IF NOT EXISTS financedwcontrols.lkup_control_catalog
(
    source_system_name VARCHAR(50)
    ,measure_category VARCHAR(20)
    ,measure_table VARCHAR(255)
    ,measure_name VARCHAR(50)
    ,load_frequency VARCHAR(10)
    ,control_low_threshold_value INTEGER
    ,control_high_threshold_value INTEGER
    ,action_no_controlentry CHAR(1)
    ,action_threshold_breach CHAR(1)
    ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
    ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
    ,UNIQUE (source_system_name, measure_category, measure_table, measure_name)
);
