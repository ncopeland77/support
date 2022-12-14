INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.CJI_COST_SCORECARD (NPI, EPISODE_TYPE, SPECIALTY, CBSA, NUM_EPISODES, AVG_OBSERVED, AVG_EXPECTED, AVG_OE, YEAR, TOT_CBSA_EPISODES, PAC_NUM, PCT_PARTB_ALLOWED, PCT_INPATIENT_ALLOWED, PCT_OUTPATIENT_ALLOWED, PCT_SNF_ALLOWED, PCT_HHA_ALLOWED, PCT_HOSPICE_ALLOWED, PCT_PARTBDME_ALLOWED, AVG_HCC, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_NPI, SRC_EPISODE_TYPE, SRC_SPECIALTY, SRC_CBSA, SRC_NUM_EPISODES, SRC_AVG_OBSERVED, SRC_AVG_EXPECTED, SRC_AVG_OE, SRC_YEAR, SRC_TOT_CBSA_EPISODES, SRC_PAC_NUM, SRC_PCT_PARTB_ALLOWED, SRC_PCT_INPATIENT_ALLOWED, SRC_PCT_OUTPATIENT_ALLOWED, SRC_PCT_SNF_ALLOWED, SRC_PCT_HHA_ALLOWED, SRC_PCT_HOSPICE_ALLOWED, SRC_PCT_PARTBDME_ALLOWED, SRC_AVG_HCC, 'historical/historical/vrdc_cji_cost_scorecard', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_CJI_COST_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.CJI_OUTCOMES_SCORECARD (NUMERATOR, DENOMINATOR, YEAR, PAC_NUM, EPISODE_TYPE, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, NPI, MEASURE_ID, MEASURE_DESC, SPECIALTY, CBSA, PROBABILITY, NATIONAL_PROB_STD, PROB_PERCENTILE)
SELECT SRC_NUMERATOR, SRC_DENOMINATOR, SRC_YEAR, SRC_PAC_NUM, SRC_EPISODE_TYPE, 'historical/historical/vrdc_cji_outcomes_scorecard', RECORD_ID, SRC_NPI, SRC_MEASURE_ID, SRC_MEASURE_DESC, SRC_SPECIALTY, SRC_CBSA, SRC_PROBABILITY, SRC_NATIONAL_PROB_STD, SRC_PROB_PERCENTILE
FROM PROD_CJVRDC.ODS.VRDC_CJI_OUTCOMES_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.CJI_PAC_COST_SCORECARD (PAC_NUM, EPISODE_TYPE, SPECIALTY, CBSA, NUM_EPISODES, AVG_OBSERVED, AVG_EXPECTED, AVG_OE, AVG_HCC, TOT_CBSA_EPISODES, YEAR, PCT_PARTB_ALLOWED, PCT_INPATIENT_ALLOWED, PCT_OUTPATIENT_ALLOWED, PCT_SNF_ALLOWED, PCT_HHA_ALLOWED, PCT_HOSPICE_ALLOWED, PCT_PARTBDME_ALLOWED, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_PAC_NUM, SRC_EPISODE_TYPE, SRC_SPECIALTY, SRC_CBSA, SRC_NUM_EPISODES, SRC_AVG_OBSERVED, SRC_AVG_EXPECTED, SRC_AVG_OE, SRC_AVG_HCC, SRC_TOT_CBSA_EPISODES, SRC_YEAR, SRC_PCT_PARTB_ALLOWED, SRC_PCT_INPATIENT_ALLOWED, SRC_PCT_OUTPATIENT_ALLOWED, SRC_PCT_SNF_ALLOWED, SRC_PCT_HHA_ALLOWED, SRC_PCT_HOSPICE_ALLOWED, SRC_PCT_PARTBDME_ALLOWED, 'historical/historical/CJI_PAC_COST_SCORECARD', 0
FROM PROD_CJVRDC.ODS.VRDC_CJI_PAC_COST_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.CJI_PAC_OUTCOMES_SCORECARD (SPECIALTY, CBSA, PROBABILITY, NATIONAL_PROB_STD, PROB_PERCENTILE, NUMERATOR, DENOMINATOR, YEAR, EPISODE_TYPE, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, PAC_NUM, MEASURE_ID, MEASURE_DESC)
SELECT SRC_SPECIALTY, SRC_CBSA, SRC_PROBABILITY, SRC_NATIONAL_PROB_STD, SRC_PROB_PERCENTILE, SRC_NUMERATOR, SRC_DENOMINATOR, SRC_YEAR, SRC_EPISODE_TYPE, 'historical/historical/CJI_PAC_OUTCOMES_SCORECARD', 0, SRC_PAC_NUM, SRC_MEASURE_ID, SRC_MEASURE_DESC
FROM PROD_CJVRDC.ODS.VRDC_CJI_PAC_OUTCOMES_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.CLAIM_ACTIVITY_ICD_DM (FK_PROVIDER_ID, MEDICAL_CD, MEDICAL_CD_DSCR, YEAR, BENE_CNT, CLAIM_CNT, TOTAL_ALLOWED, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_ICD_CD, SRC_ICD_DESC, SRC_YEAR, SRC_BENE_CNT, SRC_CLAIM_CNT, SRC_TOTAL_ALLOWED, 'historical/historical/AXB_ICD_DX_X_NPI', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_ICD_DX_X_NPI
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.CLAIM_ACTIVITY_PROC (CLAIM_TYPE, FK_PROVIDER_ID, MEDICAL_CD, YEAR, BENE_CNT, CLAIM_CNT, TOTAL_ALLOWED, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_CLM_TYPE, SRC_FK_PROVIDER_ID, SRC_HCPCS_CD, SRC_YEAR, SRC_BENE_CNT, SRC_CLAIM_CNT, SRC_TOTAL_ALLOWED, 'historical/historical/AXB_HCPCS_X_NPI', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_HCPCS_X_NPI
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.EGM_ACO (EPISODE_TYPE_CD, EPISODE_DESCRIPTION, RUN_YEAR, ATTRIBUTION_TYPE, CONNECTED_EPISODES, CONNECTED_SEQUELA, NUM_EPISODES, AVG_HCC_SCORE, AVG_EPISODE_COST, AVG_PARTB_COST, AVG_INPATIENT_COST, AVG_OUTPATIENT_COST, AVG_SNF_COST, AVG_HHA_COST, AVG_HOSPICE_COST, ACO_NAME, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, FK_ACO_ID)
SELECT SRC_EPISODE_TYPE_CD, SRC_EPISODE_DESCRIPTION, SRC_RUN_YEAR, SRC_ATTRIBUTION_TYPE, SRC_CONNECTED_EPISODES, SRC_CONNECTED_SEQUELA, SRC_NUM_EPISODES, SRC_AVG_HCC_SCORE, SRC_AVG_EPISODE_COST, SRC_AVG_PARTB_COST, SRC_AVG_INPATIENT_COST, SRC_AVG_OUTPATIENT_COST, SRC_AVG_SNF_COST, SRC_AVG_HHA_COST, SRC_AVG_HOSPICE_COST, SRC_ACO_NAME, 'historical/historical/EGM_ACO', RECORD_ID, SRC_FK_ACO_ID
FROM PROD_CJVRDC.ODS.VRDC_EGM_ACO
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.EGM_GEO (FK_GEOGRAPHY_ID, EPISODE_TYPE_CD, EPISODE_DESCRIPTION, RUN_YEAR, ATTRIBUTION_TYPE, CONNECTED_EPISODES, CONNECTED_SEQUELA, NUM_EPISODES, GEOGRAPHY_TYPE, AVG_HCC_SCORE, AVG_EPISODE_COST, AVG_PARTB_COST, AVG_INPATIENT_COST, AVG_OUTPATIENT_COST, AVG_SNF_COST, AVG_HHA_COST, AVG_HOSPICE_COST, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_GEOGRAPHY_ID, SRC_EPISODE_TYPE_CD, SRC_EPISODE_DESCRIPTION, SRC_RUN_YEAR, SRC_ATTRIBUTION_TYPE, SRC_CONNECTED_EPISODES, SRC_CONNECTED_SEQUELA, SRC_NUM_EPISODES, SRC_GEOGRAPHY_TYPE, SRC_AVG_HCC_SCORE, SRC_AVG_EPISODE_COST, SRC_AVG_PARTB_COST, SRC_AVG_INPATIENT_COST, SRC_AVG_OUTPATIENT_COST, SRC_AVG_SNF_COST, SRC_AVG_HHA_COST, SRC_AVG_HOSPICE_COST, 'historical/historical/egm_geo', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_EGM_GEO
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.EGM_NPI (AVG_HHA_COST, AVG_HOSPICE_COST, PCT_SEQUELA, PCT_PB_COST, PCT_IP_COST, PCT_OP_COST, PCT_SNF_COST, PCT_HHA_COST, PCT_HOSPICE_COST, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, FK_PROVIDER_ID, PAC_NUM, EPISODE_TYPE_CD, EPISODE_DESCRIPTION, RUN_YEAR, ATTRIBUTION_TYPE, CONNECTED_EPISODES, CONNECTED_SEQUELA, TOP_EPISODE_HCPCS, NUM_EPISODES, AVG_HCC_SCORE, AVG_NPI_SPEND, AVG_EPISODE_COST, AVG_NPI_PARTB_COST, AVG_NPI_INPATIENT_COST, AVG_NPI_OUTPATIENT_COST, AVG_NPI_SNF_COST, AVG_NPI_HHA_COST, AVG_NPI_HOSPICE_COST, AVG_PARTB_COST, AVG_INPATIENT_COST, AVG_OUTPATIENT_COST, AVG_SNF_COST)
SELECT SRC_AVG_HHA_COST, SRC_AVG_HOSPICE_COST, SRC_PCT_SEQUELA, SRC_PCT_PB_COST, SRC_PCT_IP_COST, SRC_PCT_OP_COST, SRC_PCT_SNF_COST, SRC_PCT_HHA_COST, SRC_PCT_HOSPICE_COST, 'historical/historical/vrdc_egm_npi', RECORD_ID, SRC_FK_PROVIDER_ID, SRC_PAC_NUM, SRC_EPISODE_TYPE_CD, SRC_EPISODE_DESCRIPTION, SRC_RUN_YEAR, SRC_ATTRIBUTION_TYPE, SRC_CONNECTED_EPISODES, SRC_CONNECTED_SEQUELA, SRC_TOP_EPISODE_HCPCS, SRC_NUM_EPISODES, SRC_AVG_HCC_SCORE, SRC_AVG_NPI_SPEND, SRC_AVG_EPISODE_COST, SRC_AVG_NPI_PARTB_COST, SRC_AVG_NPI_INPATIENT_COST, SRC_AVG_NPI_OUTPATIENT_COST, SRC_AVG_NPI_SNF_COST, SRC_AVG_NPI_HHA_COST, SRC_AVG_NPI_HOSPICE_COST, SRC_AVG_PARTB_COST, SRC_AVG_INPATIENT_COST, SRC_AVG_OUTPATIENT_COST, SRC_AVG_SNF_COST
FROM PROD_CJVRDC.ODS.VRDC_EGM_NPI
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.NA_BENCHMARKS (BENCHMARK_TYPE, BENCHMARK, AGG_TYPE, COHORT_TYPE, ENROLLMENT_TYPE, YEAR, METRIC_LABEL, MEAN, STD_DEV, PCTL_20, PCTL_40, PCTL_60, PCTL_80, COUNT, TOTAL, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_BENCHMARK_TYPE, SRC_BENCHMARK, SRC_AGG_TYPE, SRC_COHORT_TYPE, SRC_ENROLLMENT_TYPE, SRC_YEAR, SRC_METRIC_LABEL, SRC_MEAN, SRC_STD_DEV, SRC_PCTL_20, SRC_PCTL_40, SRC_PCTL_60, SRC_PCTL_80, SRC_COUNT, SRC_TOTAL, 'historical/historical/BENCHMARKS_EXPORT', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_NA_BENCHMARKS
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

--Error: NULL result in a non-nullable column
INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.NA_BPCI_PCP (FK_PROVIDER_ID, PAC_NUM, EPISODE_TYPE_CD, EPISODE_SUBSET, MEASURE_ID, YEAR, NUM_EPISODES, AVG_HCC_SCORE, TOTAL_EPISODE_ALLOWED, STD_DEV_EPISODE_ALLOWED, PARTB_ALLOWED, INPATIENT_ALLOWED, OUTPATIENT_ALLOWED, SNF_ALLOWED, HHA_ALLOWED, HOSPICE_ALLOWED, PARTBDME_ALLOWED, MORTALITY_RATE, READMISSION_RATE, READMISSION_ALLOWED, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PCP_PROVIDER_ID, SRC_PAC_NUM, SRC_EPISODE_TYPE_CD, SRC_EPISODE_SUBSET, SRC_MEASURE_ID, SRC_YEAR, SRC_NUM_EPISODES, SRC_AVG_HCC_SCORE, SRC_TOTAL_EPISODE_ALLOWED, SRC_STD_DEV_EPISODE_ALLOWED, SRC_PARTB_ALLOWED, SRC_INPATIENT_ALLOWED, SRC_OUTPATIENT_ALLOWED, SRC_SNF_ALLOWED, SRC_HHA_ALLOWED, SRC_HOSPICE_ALLOWED, SRC_PARTBDME_ALLOWED, SRC_MORTALITY_RATE, SRC_READMISSION_RATE, SRC_READMISSION_ALLOWED, 'historical/historical/NA_BPCI_PCP', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_BPCI_PCP
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

--Error: NULL result in a non-nullable column.
INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.NA_BPCI_TRIGGER_PROVIDER (FK_PROVIDER_ID, FK_FACILITY_ID, PAC_NUM, EPISODE_TYPE_CD, EPISODE_SUBSET, MEASURE_ID, YEAR, NUM_EPISODES, AVG_HCC_SCORE, TOTAL_EPISODE_ALLOWED, STD_DEV_EPISODE_ALLOWED, PARTB_ALLOWED, INPATIENT_ALLOWED, OUTPATIENT_ALLOWED, SNF_ALLOWED, HHA_ALLOWED, HOSPICE_ALLOWED, PARTBDME_ALLOWED, MORTALITY_RATE, READMISSION_RATE, READMISSION_ALLOWED, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_ATTRIBUTED_PROVIDER_ID, SRC_FK_ATTRIBUTED_FACILITY_ID, SRC_PAC_NUM, SRC_EPISODE_TYPE_CD, SRC_EPISODE_SUBSET, SRC_MEASURE_ID, SRC_YEAR, SRC_NUM_EPISODES, SRC_AVG_HCC_SCORE, SRC_TOTAL_EPISODE_ALLOWED, SRC_STD_DEV_EPISODE_ALLOWED, SRC_PARTB_ALLOWED, SRC_INPATIENT_ALLOWED, SRC_OUTPATIENT_ALLOWED, SRC_SNF_ALLOWED, SRC_HHA_ALLOWED, SRC_HOSPICE_ALLOWED, SRC_PARTBDME_ALLOWED, SRC_MORTALITY_RATE, SRC_READMISSION_RATE, SRC_READMISSION_ALLOWED, 'historical/historical/NA_BPCI_TRIGGER_PROVIDER', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_BPCI_TRIGGER_PROVIDER
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.NA_GEOGRAPHY (ZIP_CD, CBSA_DESC, COUNTY_CD, COUNTY_DESC, STATE, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_ZIP_CD, SRC_CBSA_DESC, SRC_COUNTY_CD, SRC_COUNTY_DESC, SRC_STATE, 'historical/historical/NA_GEOGRAPHY', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_NA_GEOGRAPHY
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

--Error: SQL compilation error: Object 'PROD_CJVRDC.ODS.VRDC_NA_HASHED_TIN_X_BILL_NPI' does not exist or not authorized.
INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.NA_HASHED_TIN_X_BILL_NPI (BILL_NPI, YEAR, SPEND, HASHED_TIN, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_BILL_NPI, SRC_YEAR, SRC_SPEND, SRC_HASHED_TIN, 'historical/historical/NA_HASHED_TIN_X_BILL_NPI', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_NA_HASHED_TIN_X_BILL_NPI
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.NPI_X_PROVIDER_ZIP (NPI, YEAR, PROV_ZIP, ACO_ID, MULTI_ACO, CJ_INDEX_COST, CJ_INDEX_OUTCOMES, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_NPI, SRC_YEAR, SRC_PROV_ZIP, SRC_ACO_ID, SRC_MULTI_ACO, SRC_CJ_INDEX_COST, SRC_CJ_INDEX_OUTCOMES, 'historical/historical/NA_REF_NPI_X_PROVIDER_ZIP', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_NPI_X_PROVIDER_ZIP
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.NPI_X_SPECIALTY (NPI, PRIMARY_SPECIALTY, PCT_PRIMARY_SPECIALTY, SECONDARY_SPECIALTY, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_NPI, SRC_PRIMARY_SPECIALTY, SRC_PCT_PRIMARY_SPECIALTY, SRC_SECONDARY_SPECIALTY, 'historical/historical/NPI_X_SPECIALTY_EXPORT', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_NPI_X_SPECIALTY
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_COST_SCORE (FK_PROVIDER_ID, YEAR, TAXONOMY, CBSA, EPISODE, EPISODE_TYPE, PPI_COST_SCORE, COST_TEST_STATISTIC, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_YEAR, SRC_TAXONOMY, SRC_CBSA, SRC_EPISODE, SRC_EPISODE_TYPE, SRC_PPI_COST_SCORE, SRC_COST_TEST_STATISTIC, SRC_VERSION, 'historical/historical/PPI_COST_SCORE', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PPI_COST_SCORE
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_COST_SCORECARD (AVG_EXPECTED, AVG_OE, TEST_STATISTIC, AVG_HCC, AVG_CBSA_EPISODES, YEAR, TOT_PARTB_ALLOWED, TOT_INPATIENT_ALLOWED, TOT_OUTPATIENT_ALLOWED, TOT_SNF_ALLOWED, TOT_HHA_ALLOWED, TOT_HOSPICE_ALLOWED, TOT_PARTBDME_ALLOWED, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, FK_PROVIDER_ID, EPISODE, EPISODE_TYPE, TAXONOMY, CBSA, NUM_EPISODES, AVG_OBSERVED)
SELECT SRC_AVG_EXPECTED, SRC_AVG_OE, SRC_TEST_STATISTIC, SRC_AVG_HCC, SRC_AVG_CBSA_EPISODES, SRC_YEAR, SRC_TOT_PARTB_ALLOWED, SRC_TOT_INPATIENT_ALLOWED, SRC_TOT_OUTPATIENT_ALLOWED, SRC_TOT_SNF_ALLOWED, SRC_TOT_HHA_ALLOWED, SRC_TOT_HOSPICE_ALLOWED, SRC_TOT_PARTBDME_ALLOWED, SRC_VERSION, 'historical/historical/PPI_COST_SCORECARD', RECORD_ID, SRC_FK_PROVIDER_ID, SRC_EPISODE, SRC_EPISODE_TYPE, SRC_TAXONOMY, SRC_CBSA, SRC_NUM_EPISODES, SRC_AVG_OBSERVED
FROM PROD_CJVRDC.ODS.VRDC_PPI_COST_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_CROSSWALK (FK_PROVIDER_ID, YEAR, SPECIALTY, TAXONOMY, LEGACY_COST_SCORE, ENHANCED_COST_SCORE, LEGACY_QUALITY_SCORE, ENHANCED_QUALITY_SCORE, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_YEAR, SRC_SPECIALTY, SRC_TAXONOMY, SRC_LEGACY_COST_SCORE, SRC_ENHANCED_COST_SCORE, SRC_LEGACY_QUALITY_SCORE, SRC_ENHANCED_QUALITY_SCORE, SRC_VERSION, 'historical/historical/PPI_CROSSWALK', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PPI_CROSSWALK
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_PAC_COST_SCORE (CBSA, EPISODE, EPISODE_TYPE, PPI_COST_SCORE, COST_TEST_STATISTIC, HASH, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, PAC_NUM, YEAR, TAXONOMY)
SELECT SRC_CBSA, SRC_EPISODE, SRC_EPISODE_TYPE, SRC_PPI_COST_SCORE, SRC_COST_TEST_STATISTIC, SRC_HASH, SRC_VERSION, 'historical/historical/PPI_PAC_COST_SCORE', RECORD_ID, SRC_PAC_NUM, SRC_YEAR, SRC_TAXONOMY
FROM PROD_CJVRDC.ODS.VRDC_PPI_PAC_COST_SCORE
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_PAC_COST_SCORECARD (LOAD_FILE_NM, LOAD_FILE_ROW_NUM, PAC_NUM, EPISODE, EPISODE_TYPE, TAXONOMY, CBSA, NUM_EPISODES, AVG_OBSERVED, AVG_EXPECTED, AVG_OE, TEST_STATISTIC, AVG_HCC, AVG_CBSA_EPISODES, YEAR, TOT_PARTB_ALLOWED, TOT_INPATIENT_ALLOWED, TOT_OUTPATIENT_ALLOWED, TOT_SNF_ALLOWED, TOT_HHA_ALLOWED, TOT_HOSPICE_ALLOWED, TOT_PARTBDME_ALLOWED, VERSION)
SELECT 'historical/historical/PPI_PAC_COST_SCORECARD', RECORD_ID, SRC_PAC_NUM, SRC_EPISODE, SRC_EPISODE_TYPE, SRC_TAXONOMY, SRC_CBSA, SRC_NUM_EPISODES, SRC_AVG_OBSERVED, SRC_AVG_EXPECTED, SRC_AVG_OE, SRC_TEST_STATISTIC, SRC_AVG_HCC, SRC_AVG_CBSA_EPISODES, SRC_YEAR, SRC_TOT_PARTB_ALLOWED, SRC_TOT_INPATIENT_ALLOWED, SRC_TOT_OUTPATIENT_ALLOWED, SRC_TOT_SNF_ALLOWED, SRC_TOT_HHA_ALLOWED, SRC_TOT_HOSPICE_ALLOWED, SRC_TOT_PARTBDME_ALLOWED, SRC_VERSION
FROM PROD_CJVRDC.ODS.VRDC_PPI_PAC_COST_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_PAC_CROSSWALK (PAC_NUM, YEAR, LEGACY_COST_SCORE, ENHANCED_COST_SCORE, LEGACY_QUALITY_SCORE, ENHANCED_QUALITY_SCORE, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_PAC_NUM, SRC_YEAR, SRC_LEGACY_COST_SCORE, SRC_ENHANCED_COST_SCORE, SRC_LEGACY_QUALITY_SCORE, SRC_ENHANCED_QUALITY_SCORE, SRC_VERSION, 'historical/historical/PPI_PAC_CROSSWALK', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PPI_PAC_CROSSWALK
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_PAC_QUALITY_SCORE (PAC_NUM, YEAR, TAXONOMY, CBSA, PPI_QUALITY_SCORE, QUALITY_TEST_STATISTIC, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_PAC_NUM, SRC_YEAR, SRC_TAXONOMY, SRC_CBSA, SRC_PPI_QUALITY_SCORE, SRC_QUALITY_TEST_STATISTIC, SRC_VERSION, 'historical/historical/PPI_PAC_QUALITY_SCORE', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PPI_PAC_QUALITY_SCORE
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_PAC_QUALITY_SCORECARD (PAC_NUM, MEASURE_ID, MEASURE_DESC, TAXONOMY, CBSA, PROBABILITY, PROB_PERCENTILE, NUMERATOR, DENOMINATOR, RATE, YEAR, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_PAC_NUM, SRC_MEASURE_ID, SRC_MEASURE_DESC, SRC_TAXONOMY, SRC_CBSA, SRC_PROBABILITY, SRC_PROB_PERCENTILE, SRC_NUMERATOR, SRC_DENOMINATOR, SRC_RATE, SRC_YEAR, SRC_VERSION, 'historical/historical/PPI_PAC_QUALITY_SCORECARD', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PPI_PAC_QUALITY_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_QUALITY_SCORE (FK_PROVIDER_ID, YEAR, TAXONOMY, CBSA, PPI_QUALITY_SCORE, QUALITY_TEST_STATISTIC, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_YEAR, SRC_TAXONOMY, SRC_CBSA, SRC_PPI_QUALITY_SCORE, SRC_QUALITY_TEST_STATISTIC, SRC_VERSION, 'historical/historical/NA_NPI_PPI_OUTCOMES_OPH_PROD', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PPI_QUALITY_SCORE
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PPI_QUALITY_SCORECARD (FK_PROVIDER_ID, MEASURE_ID, MEASURE_DESC, TAXONOMY, CBSA, PROBABILITY, PROB_PERCENTILE, NUMERATOR, DENOMINATOR, RATE, YEAR, VERSION, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_MEASURE_ID, SRC_MEASURE_DESC, SRC_TAXONOMY, SRC_CBSA, SRC_PROBABILITY, SRC_PROB_PERCENTILE, SRC_NUMERATOR, SRC_DENOMINATOR, SRC_RATE, SRC_YEAR, SRC_VERSION, 'historical/historical/PPI_OUTCOMES_SCD_OPH_PROD', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PPI_QUALITY_SCORECARD
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PRACTICE_GROUP_LOOKUP (PAC_ZIP, PRIMARY_PRACTICE_SPECIALTY, OTHER_SPECIALTIES, ACO_ID, MULTI_ACO, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, PAC_NUM, YEAR, PRACTICE_GROUP)
SELECT SRC_PAC_ZIP, SRC_PRIMARY_PRACTICE_SPECIALTY, SRC_OTHER_SPECIALTIES, SRC_ACO_ID, SRC_MULTI_ACO, 'historical/historical/na_ref_practice_group_lookup', RECORD_ID, SRC_PAC_NUM, SRC_YEAR, SRC_PRACTICE_GROUP
FROM PROD_CJVRDC.ODS.VRDC_PRACTICE_GROUP_LOOKUP
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PROVIDER_COST_UTILIZATION_ATTR (PROVIDER_ID, ATTRIBUTION_TYPE, YEAR, METRIC_LABEL, METRIC, PAC_NUM, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_ATTRIBUTION_TYPE, SRC_YEAR, SRC_METRIC_LABEL, SRC_METRIC, SRC_PAC_NUM, 'historical/historical/METRICS_PCP_EXPORT', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PROFILE_LIST_ATTR_NPI
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PROVIDER_COST_UTILIZATION_ATTR_PG (PAC_NUM, YEAR, METRIC_ORDER, METRIC_LABEL, METRIC, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_PAC_NUM, SRC_YEAR, SRC_METRIC_ORDER, SRC_METRIC_LABEL, SRC_METRIC, 'historical/historical/ATTR_PG_PROFILE_LIST_EXPORT', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PROFILE_LIST_ATTR_PG
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PROVIDER_COST_UTILIZATION_FACILITY (PROVIDER_ID, YEAR, METRIC_ORDER, METRIC_LABEL, METRIC, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_FACILITY_ID, SRC_YEAR, SRC_METRIC_ORDER, SRC_METRIC_LABEL, SRC_METRIC, 'historical/historical/profile_list_facility', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PROFILE_LIST_FACILITY
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PROVIDER_COST_UTILIZATION_POST_ACUTE (PROVIDER_ID, YEAR, METRIC_LABEL, METRIC, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_FACILITY_ID, SRC_YEAR, SRC_METRIC_LABEL, SRC_METRIC, 'historical/historical/PROVIDER_COST_UTILIZATION_POST_ACUTE', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PROFILE_LIST_POST_ACUTE
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

--Error: NULL result in a non-nullable column
INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PROVIDER_COST_UTILIZATION_PRAC (PROVIDER_ID, YEAR, METRIC_LABEL, METRIC, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_YEAR, SRC_METRIC_LABEL, SRC_METRIC, 'historical/historical/PROVIDER_COST_UTILIZATION_PRAC', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PROFILE_LIST_PROVIDER
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

--Error: NULL result in a non-nullable column
INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PROVIDER_COST_UTILIZATION_RNDRG_PG (PAC_NUM, YEAR, SPECIALTY, METRIC_ORDER, METRIC_LABEL, METRIC, METRIC_TOTAL, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_PAC_NUM, SRC_YEAR, SRC_SPECIALTY, SRC_METRIC_ORDER, SRC_METRIC_LABEL, SRC_METRIC, SRC_METRIC_TOTAL, 'historical/historical/profile_list_rndrg_pg_partd', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PROFILE_LIST_RNDRG_PG
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.PROVIDER_COST_UTILIZATION_SPECIALIST (PROVIDER_ID, PAC_NUM, YEAR, METRIC_ORDER, METRIC_LABEL, METRIC, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PROVIDER_ID, SRC_PAC_NUM, SRC_YEAR, SRC_METRIC_ORDER, SRC_METRIC_LABEL, SRC_METRIC, 'historical/historical/profile_list_specialist', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_PROFILE_LIST_SPECIALIST
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.REFERRALS (FROM_PROVIDER_SPECIALTY, FK_PROVIDER_ID_TO, TO_PROVIDER_TYPE, TO_PROVIDER_SPECIALTY, FK_FROM_PAC_NUM, FK_TO_PAC_NUM, YEAR, REFERRAL_SUBSET, TOTAL_ALLOWED, TOTAL_CLAIMS, TOTAL_PATIENTS, PCT_ALLOWED_TO, PCT_ALLOWED_FROM, PCT_PATIENTS_TO, PCT_PATIENTS_FROM, LOAD_FILE_NM, LOAD_FILE_ROW_NUM, FK_PROVIDER_ID_FROM, FROM_PROVIDER_TYPE)
SELECT SRC_FROM_PROVIDER_SPECIALTY, SRC_FK_PROVIDER_ID_TO, SRC_TO_PROVIDER_TYPE, SRC_TO_PROVIDER_SPECIALTY, SRC_FK_FROM_PAC_NUM, SRC_FK_TO_PAC_NUM, SRC_YEAR, SRC_REFERRAL_SUBSET, SRC_TOTAL_ALLOWED, SRC_TOTAL_CLAIMS, SRC_TOTAL_PATIENTS, SRC_PCT_ALLOWED_TO, SRC_PCT_ALLOWED_FROM, SRC_PCT_PATIENTS_TO, SRC_PCT_PATIENTS_FROM, 'historical/historical/NA_REFERRAL_PRO_TO_PRO', RECORD_ID, SRC_FK_PROVIDER_ID_FROM, SRC_FROM_PROVIDER_TYPE
FROM PROD_CJVRDC.ODS.VRDC_REFERRALS
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';

INSERT INTO LOCAL_JUANVAGO.STAGE_VRDC.UTILIZATION_PCP (FK_PCP_PROVIDER_ID, QEXPU_HEADER_CD, SPECIALTY_CD, RENDERING_PROVIDER_ID, RENDERING_FACILITY_ID, PCP_PAC_NUM, RENDERING_PAC_NUM, YEAR, SPEND, BENE_COUNT, CLAIM_COUNT, LOAD_FILE_NM, LOAD_FILE_ROW_NUM)
SELECT SRC_FK_PCP_PROVIDER_ID, SRC_QEXPU_HEADER_CD, SRC_SPECIALTY_CD, SRC_RENDERING_PROVIDER_ID, SRC_RENDERING_FACILITY_ID, SRC_PCP_PAC_NUM, SRC_RENDERING_PAC_NUM, SRC_YEAR, SRC_SPEND, SRC_BENE_COUNT, SRC_CLAIM_COUNT, 'historical/historical/NA_UTILIZATION_PCP', RECORD_ID
FROM PROD_CJVRDC.ODS.VRDC_UTILIZATION_PCP
WHERE EFFECTIVE_FLAG AND RECORD_STATUS_CD = 'a';
