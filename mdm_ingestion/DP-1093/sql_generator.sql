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
      ('CLAIM_ACTIVITY_ICD_DM','VRDC_ICD_DX_X_NPI','AXB_ICD_DX_X_NPI'),
      ('CLAIM_ACTIVITY_PROC','VRDC_HCPCS_X_NPI','AXB_HCPCS_X_NPI'),
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
  WHERE source_table <> 'N/A'
),
/*special treatment for some target table and col:*/
rules AS (
  SELECT 
    column1 as source_table,
    column2 as source_col,
    column3 as target_table,
    column4 as target_col
  FROM (
      VALUES 
      ('VRDC_CJI_PAC_OUTCOMES_SCORECARD','0','CJI_PAC_OUTCOMES_SCORECARD','LOAD_FILE_ROW_NUM'),
      ('VRDC_CJI_PAC_COST_SCORECARD','0','CJI_PAC_COST_SCORECARD','LOAD_FILE_ROW_NUM'),
      ('VRDC_PROFILE_LIST_ATTR_NPI','SRC_FK_PROVIDER_ID','PROVIDER_COST_UTILIZATION_ATTR','PROVIDER_ID'),
      ('VRDC_PROFILE_LIST_PROVIDER','SRC_FK_PROVIDER_ID','PROVIDER_COST_UTILIZATION_PRAC','PROVIDER_ID'),
      ('VRDC_PROFILE_LIST_SPECIALIST','SRC_FK_PROVIDER_ID','PROVIDER_COST_UTILIZATION_SPECIALIST','PROVIDER_ID'),
      ('VRDC_PROFILE_LIST_FACILITY','SRC_FK_FACILITY_ID','PROVIDER_COST_UTILIZATION_FACILITY','PROVIDER_ID'),
      ('VRDC_PROFILE_LIST_POST_ACUTE','SRC_FK_FACILITY_ID','PROVIDER_COST_UTILIZATION_POST_ACUTE','PROVIDER_ID'),
      ('VRDC_ICD_DX_X_NPI','SRC_ICD_CD','CLAIM_ACTIVITY_ICD_DM','MEDICAL_CD'),
      ('VRDC_ICD_DX_X_NPI','SRC_ICD_DESC','CLAIM_ACTIVITY_ICD_DM','MEDICAL_CD_DSCR'),
      ('VRDC_HCPCS_X_NPI','SRC_CLM_TYPE','CLAIM_ACTIVITY_PROC','CLAIM_TYPE'),
      ('VRDC_HCPCS_X_NPI','SRC_HCPCS_CD','CLAIM_ACTIVITY_PROC','MEDICAL_CD')
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
      WHEN target_cols_info.column_name = 'LOAD_FILE_ROW_NUM' THEN COALESCE(rules.source_col,'RECORD_ID')
      ELSE COALESCE(rules.source_col,CONCAT('SRC_',target_cols_info.column_name))
   END AS source_col,
   target_cols_info.ordinal_position
FROM table_conf 
INNER JOIN DEV_STAGE_RAW.INFORMATION_SCHEMA.COLUMNS target_cols_info ON table_conf.target_table = target_cols_info.table_name
LEFT JOIN rules 
    ON rules.target_table = target_cols_info.table_name
    AND rules.target_col = target_cols_info.column_name
WHERE  target_cols_info.table_schema = (SELECT target_schema FROM params)
ORDER BY target_cols_info.table_schema,target_cols_info.table_name,target_cols_info.ordinal_position
)
SELECT
    CONCAT('INSERT INTO ',(SELECT target_database FROM params),'.',columns_translator.target_schema,'.',columns_translator.target_table,' (',listagg(columns_translator.target_col, ','),') SELECT ',listagg(columns_translator.source_col, ','), ' FROM ',(SELECT source_database FROM params),'.',(SELECT source_schema FROM params),'.',columns_translator.source_table, ' WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = ''a'';')
FROM columns_translator 
GROUP BY columns_translator.target_schema, columns_translator.target_table,columns_translator.source_table
ORDER BY columns_translator.target_table
