_params = {
 'databases': {
  'datastage' : """f'ta_individual_findw_{environment}_financedatastage{project_ovrd}'""",
  'gdq' : """f'ta_individual_findw_{environment}_financemasterdatastore{project_ovrd}'""",
  'controls' : """f'ta_individual_findw_{environment}_financecontrols{project_ovrd}'""",
  'curated' : """f'ta_individual_findw_{environment}_financecurated{project_ovrd}'""",
  'annuities' : """f'ta_individual_findw_{environment}_annuitiescurated'""",
  'datalake_curated' : """f'ta_individual_datalake_{environment}_{sourcesystem.lower()}_curated'""",
  'arr_curated' : """f'ta_individual_arr_{environment}_curated'""",
  'datalake_ref_db' : """f'ta_individual_datalake_{environment}_ref_db'""",
  'datalake_ref_edmcs_db' : """f'ta_individual_datalake_{environment}_oracleedmcs_curated'""",
 },
 'glerp': {
  'outbound_sources' : ['oracleap'],
  'soft_error_max_age_days' : 14,
  'header_only_domain' : ['accounts_payable_vendor'],
  'source_initial_cut_off' : {'p3dss' : '2025-08-04','p5dss' : '2025-08-04','p7dss' : '2025-06-09',
   'tpaclaims' : '2025-06-09','ahdfuturefirst' : '2025-06-09','cyberlife001tebifrs17' : '2025-06-09','cyberlife970lifrs17' : '2025-06-09',
   'horizonifrs17' : '2025-10-06','tpapifrs17' : '2025-10-06','affinity' : '2025-10-06','VantageP65' : '2025-10-06','VantageP6' : '2025-10-06',
   'VantageP75' : '2025-10-06','ltcg' : '2025-10-06','ltcghybrid' : '2025-10-06','ahd' : '2025-08-04','mclsifrs17' : '2025-08-04',
   'Bestow' : '2025-10-06','fsps' : '2025-08-04','genesysifrs17' : '2025-08-04','mantisisfrs17' : '2025-08-04','lifecomlifrs17' : '2025-08-04',
   'lifepro111lifrs17' : '2025-08-04','tpadlifrs17' : '2025-08-04','processoroneexton' : '2025-10-13','processoroneplano' : '2025-10-13',
   'lifepro109lifrs17' : '2025-10-13','clearwater' : '2025-10-13','murex' : '2025-10-13','yardi' : '2025-10-13','invstranet' : '2025-10-13',
   'revport' : '2025-10-13','mkdb' : '2025-10-13','arr' : '2025-10-06','illumifinifrs17' : '2025-10-13'},
 },
 'common': {
  'idl_originating_sourcesystem': {'p3dss' : 'P3DSSFB', 'p5dss' : 'P5DSSFB', 'p7dss' : 'P7DSSFB'},
  'source_initial_cut_off' : {'ss1' : 'YYYY-MM-DD', 'ss2' : 'YYYY-MM-DD'},
  'curated_initial_cut_off' : {},
  'refresh_materialized_views': []
 },
}

#Prod Backup
'source_initial_cut_off' : {'p3dss' : '2025-08-04','p5dss' : '2025-08-04','p7dss' : '2025-06-09',
 'tpaclaims' : '2025-06-09','ahdfuturefirst' : '2025-06-09','cyberlife001tebifrs17' : '2025-06-09','cyberlife970lifrs17' : '2025-06-09',
 'horizonifrs17' : '2025-12-31','tpapifrs17' : '2025-12-31','affinity' : '2025-12-31','VantageP65' : '2025-12-31','VantageP6' : '2025-12-31',
 'VantageP75' : '2025-12-31','ltcg' : '2025-12-31','ltcghybrid' : '2025-12-31','ahd' : '2025-08-04','mclsifrs17' : '2025-08-04',
 'Bestow' : '2025-08-04','fsps' : '2025-08-04','genesysifrs17' : '2025-08-04','mantisisfrs17' : '2025-08-04','lifecomlifrs17' : '2025-08-04',
 'lifepro111lifrs17' : '2025-08-04','tpadlifrs17' : '2025-08-04','processoroneexton' : '2025-10-13','processoroneplano' : '2025-10-13',
 'lifepro109lifrs17' : '2025-10-13','clearwater' : '2025-10-13','murex' : '2025-10-13','yardi' : '2025-10-13','invstranet' : '2025-10-13',
 'revport' : '2025-10-13','mkdb' : '2025-10-13','arr' : '2025-12-28','illumifinifrs17' : '2025-10-13'},
