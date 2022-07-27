WITH 
params AS (
  SELECT
    'ODS' AS source_schema,
    'PROD_CJVRDC' AS source_database,
    'STAGE_VRDC' AS target_schema,
    'LOCAL_JUANVAGO' AS target_database
),
table_conf AS (
  SELECT 
    column1 as target_table,
    column2 as source_table,
    column3 as file_base_name
  FROM
  (
    VALUES
      ('CJI_COST_SCORECARD','VRDC_CJI_COST_SCORECARD','vrdc_cji_cost_scorecard'),
      ('CJI_OUTCOMES_SCORECARD','VRDC_CJI_OUTCOMES_SCORECARD','vrdc_cji_outcomes_scorecard'),
      ('CJI_PAC_COST_SCORECARD','VRDC_CJI_PAC_COST_SCORECARD','no data'),
      ('CJI_PAC_OUTCOMES_SCORECARD','VRDC_CJI_PAC_OUTCOMES_SCORECARD','no data'),
      ('CLAIM_ACTIVITY_ICD_DM','VRDC_CLAIM_ACTIVITY_ICD_DM','AXB_ICD_DX_X_NPI'),
      ('CLAIM_ACTIVITY_PROC','VRDC_CLAIM_ACTIVITY_PROC','AXB_HCPCS_X_NPI'),
      ('EGM_ACO','VRDC_EGM_ACO','no data'),
      ('EGM_GEO','VRDC_EGM_GEO','egm_geo'),
      ('EGM_NPI','VRDC_EGM_NPI','vrdc_egm_npi'),
      ('NA_BENCHMARKS','VRDC_NA_BENCHMARKS','BENCHMARKS_EXPORT'),
      ('NA_BILL_NPI_X_CCN','N/A','NA_BILL_NPI_X_CCN'),
      ('NA_BILL_NPI_X_NPI','N/A','NA_BILL_NPI_X_NPI'),
      ('NA_BPCI_PCP','VRDC_NA_BPCI_PCP','no data'),
      ('NA_BPCI_TRIGGER_PROVIDER','VRDC_NA_BPCI_TRIGGER_PROVIDER','no data'),
      ('NA_GEOGRAPHY','VRDC_NA_GEOGRAPHY','no data'),
      ('NA_HASHED_TIN_X_BILL_NPI','VRDC_NA_HASHED_TIN_X_BILL_NPI','NA_HASHED_TIN_X_BILL_NPI'),
      ('NPI_X_PROVIDER_ZIP','VRDC_NPI_X_PROVIDER_ZIP','NA_REF_NPI_X_PROVIDER_ZIP'),
      ('NPI_X_SPECIALTY','VRDC_NPI_X_SPECIALTY','NPI_X_SPECIALTY_EXPORT'),
      ('PPI_COST_SCORE','VRDC_PPI_COST_SCORE','PPI_COST_SCORE'),
      ('PPI_COST_SCORECARD','VRDC_PPI_COST_SCORECARD','PPI_COST_SCORECARD'),
      ('PPI_CROSSWALK','VRDC_PPI_CROSSWALK','no data'),
      ('PPI_PAC_COST_SCORE','VRDC_PPI_PAC_COST_SCORE','no data'),
      ('PPI_PAC_COST_SCORECARD','VRDC_PPI_PAC_COST_SCORECARD','no data'),
      ('PPI_PAC_CROSSWALK','VRDC_PPI_PAC_CROSSWALK','no data'),
      ('PPI_PAC_QUALITY_SCORE','VRDC_PPI_PAC_QUALITY_SCORE','no data'),
      ('PPI_PAC_QUALITY_SCORECARD','VRDC_PPI_PAC_QUALITY_SCORECARD','no data'),
      ('PPI_QUALITY_SCORE','VRDC_PPI_QUALITY_SCORE','NA_NPI_PPI_OUTCOMES_OPH_PROD'),
      ('PPI_QUALITY_SCORECARD','VRDC_PPI_QUALITY_SCORECARD','PPI_OUTCOMES_SCD_OPH_PROD'),
      ('PRACTICE_GROUP_LOOKUP','VRDC_PRACTICE_GROUP_LOOKUP','na_ref_practice_group_lookup'),
      ('PROVIDER_COST_UTILIZATION','N/A','EXPL_BILL_NPI_MSSP_PMPY'),
      ('PROVIDER_COST_UTILIZATION_ATTR','VRDC_PROFILE_LIST_ATTR_NPI','METRICS_PCP_EXPORT'),
      ('PROVIDER_COST_UTILIZATION_ATTR_PG','VRDC_PROFILE_LIST_ATTR_PG','ATTR_PG_PROFILE_LIST_EXPORT'),
      ('PROVIDER_COST_UTILIZATION_FACILITY','VRDC_PROFILE_LIST_FACILITY','profile_list_facility'),
      ('PROVIDER_COST_UTILIZATION_POST_ACUTE','VRDC_PROFILE_LIST_POST_ACUTE','no data'),
      ('PROVIDER_COST_UTILIZATION_PRAC','VRDC_PROFILE_LIST_PROVIDER','no data'),
      ('PROVIDER_COST_UTILIZATION_RNDRG_PG','VRDC_PROFILE_LIST_RNDRG_PG','profile_list_rndrg_pg_partd'),
      ('PROVIDER_COST_UTILIZATION_SPECIALIST','VRDC_PROFILE_LIST_SPECIALIST','profile_list_specialist'),
      ('REFERRALS','VRDC_REFERRALS','NA_REFERRAL_PRO_TO_PRO'),
      ('UTILIZATION_PCP','VRDC_UTILIZATION_PCP','NA_UTILIZATION_PCP')
  )
  WHERE column2 <> 'N/A'
),
/*Some tables have no RECORD_ID field, then we use NULL instead*/
record_id_null AS (
  SELECT *
  FROM (
      VALUES 
      (1, 'CJI_PAC_OUTCOMES_SCORECARD'), 
      (2, 'CJI_PAC_COST_SCORECARD')//, 
//      (3, 'PROFILE_LIST_MARKET_V01'), 
//      (4, 'ICD_DX_X_NPI_BACKUP_0106'), 
//      (5, 'HCPCS_X_NPI_BACKUP_0106'), 
//      (6, 'ACO_X_PLAN_SWAP'), 
//      (7, 'PARTD_ATTR_NPI_X_PLAN_SWAP')
      )
),
columns_translator AS (
SELECT 
   target_cols_info.table_schema AS target_schema,
   target_cols_info.table_name AS target_table,
   target_cols_info.column_name AS target_col,
   table_conf.source_table,
   CASE 
      WHEN target_cols_info.column_name = 'LOAD_FILE_NM' AND table_conf.file_base_name = 'no data' THEN  CONCAT('''historical/historical/',target_cols_info.table_name,'''')
      WHEN target_cols_info.column_name = 'LOAD_FILE_NM' THEN CONCAT('''historical/historical/',table_conf.file_base_name,'''')
      WHEN target_cols_info.column_name = 'LOAD_FILE_ROW_NUM' AND record_id_null.column2 = NULL THEN 'RECORD_ID' 
      WHEN target_cols_info.column_name = 'LOAD_FILE_ROW_NUM' THEN 'NULL' /*Some tables have no RECORD_ID field, then we use NULL instead*/
      WHEN table_conf.source_table = 'VRDC_PROFILE_LIST_ATTR_NPI' AND target_cols_info.column_name = 'PROVIDER_ID' THEN 'SRC_FK_PROVIDER_ID'
      ELSE CONCAT('SRC_',target_cols_info.column_name) 
   END AS source_col
FROM DEV_STAGE_RAW.INFORMATION_SCHEMA.COLUMNS target_cols_info
LEFT JOIN record_id_null ON record_id_null.column2 = target_cols_info.table_name
INNER JOIN table_conf ON table_conf.target_table = target_cols_info.table_name
WHERE  target_cols_info.table_schema = (SELECT target_schema FROM params)
ORDER BY target_cols_info.table_schema,target_cols_info.table_name,target_cols_info.ordinal_position
)
SELECT
    CONCAT('INSERT INTO ',(SELECT target_database FROM params),'.',target_table_info.table_schema,'.',target_table_info.table_name,' (',listagg(columns_translator.target_col, ','),') SELECT ',listagg(columns_translator.source_col, ','), ' FROM ',(SELECT source_database FROM params),'.',(SELECT source_schema FROM params),'.',source_table_info.table_name, ' WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = ''a'';')
FROM DEV_STAGE_RAW.INFORMATION_SCHEMA.TABLES target_table_info 
INNER JOIN columns_translator 
    on columns_translator.target_table = target_table_info.table_name
    and columns_translator.target_schema = target_table_info.table_schema
INNER JOIN PROD_CJVRDC.INFORMATION_SCHEMA.TABLES source_table_info 
    on source_table_info.table_name = columns_translator.source_table //CONCAT('VRDC_',target_table_info.table_name)
    and source_table_info.table_schema = (SELECT source_schema FROM params)
WHERE target_table_info.table_schema = (SELECT target_schema FROM params)
//AND target_table_info.table_name = 'PROVIDER_COST_UTILIZATION_ATTR'
GROUP BY target_table_info.table_name, source_table_info.table_name, target_table_info.table_schema
ORDER BY target_table_info.table_name
