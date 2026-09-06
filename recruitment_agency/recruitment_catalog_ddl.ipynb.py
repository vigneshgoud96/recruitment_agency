# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Recruitment Catalog DDL - Title
# MAGIC %md
# MAGIC # Recruitment Catalog DDL Scripts
# MAGIC
# MAGIC Complete CREATE TABLE/VIEW statements for all objects in the recruitment catalog, organized by schema layer.

# COMMAND ----------

# DBTITLE 1,Bronze Schema Header
# MAGIC %md
# MAGIC ## Bronze Schema
# MAGIC
# MAGIC Raw data ingestion layer - 12 tables

# COMMAND ----------

# DBTITLE 1,catalog & schema
# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG IF NOT EXISTS recruitment;
# MAGIC
# MAGIC -- Create schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS recruitment.bronze
# MAGIC   COMMENT 'Bronze layer: raw ingested data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS recruitment.silver
# MAGIC   COMMENT 'Silver layer: cleaned and validated data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS recruitment.gold
# MAGIC   COMMENT 'Gold layer: business-level aggregated data';

# COMMAND ----------

# DBTITLE 1,volumn
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS recruitment.bronze.uploads;

# COMMAND ----------

# DBTITLE 1,Bronze DDL Output
# Fetch and display all Bronze schema DDL statements
catalog = "recruitment"
schema = "bronze"

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
bronze_tables = [row.tableName for row in tables]

print("BRONZE SCHEMA - CREATE TABLE STATEMENTS")
print("="*80)
print(f"Total tables: {len(bronze_tables)}\n")

for table in sorted(bronze_tables):
    print(f"\n{'='*80}")
    print(f"-- {table.upper()}")
    print(f"{'='*80}\n")
    ddl = spark.sql(f"SHOW CREATE TABLE {catalog}.{schema}.{table}").collect()[0][0]
    print(ddl)
    print(";\n")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BRONZE SCHEMA - CREATE TABLE STATEMENTS
# MAGIC -- ================================================================================
# MAGIC -- Total tables: 12
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_AGREEMENTS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_agreements (
# MAGIC   agreement_id STRING COLLATE UTF8_BINARY,
# MAGIC   company_id STRING COLLATE UTF8_BINARY,
# MAGIC   agreement_type STRING COLLATE UTF8_BINARY,
# MAGIC   agreement_start_date STRING COLLATE UTF8_BINARY,
# MAGIC   agreement_end_date STRING COLLATE UTF8_BINARY,
# MAGIC   fee_percentage STRING COLLATE UTF8_BINARY,
# MAGIC   payment_terms_days STRING COLLATE UTF8_BINARY,
# MAGIC   replacement_period_days STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_ownership_days STRING COLLATE UTF8_BINARY,
# MAGIC   agreement_status STRING COLLATE UTF8_BINARY,
# MAGIC   document_location STRING COLLATE UTF8_BINARY,
# MAGIC   notes STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested agreement data from agreements.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_CANDIDATES
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_candidates (
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_name STRING COLLATE UTF8_BINARY,
# MAGIC   email STRING COLLATE UTF8_BINARY,
# MAGIC   phone STRING COLLATE UTF8_BINARY,
# MAGIC   alternate_phone STRING COLLATE UTF8_BINARY,
# MAGIC   current_location STRING COLLATE UTF8_BINARY,
# MAGIC   preferred_location STRING COLLATE UTF8_BINARY,
# MAGIC   total_experience_years STRING COLLATE UTF8_BINARY,
# MAGIC   relevant_experience_years STRING COLLATE UTF8_BINARY,
# MAGIC   current_company STRING COLLATE UTF8_BINARY,
# MAGIC   current_designation STRING COLLATE UTF8_BINARY,
# MAGIC   skills STRING COLLATE UTF8_BINARY,
# MAGIC   primary_skill STRING COLLATE UTF8_BINARY,
# MAGIC   secondary_skills STRING COLLATE UTF8_BINARY,
# MAGIC   current_ctc STRING COLLATE UTF8_BINARY,
# MAGIC   expected_ctc STRING COLLATE UTF8_BINARY,
# MAGIC   notice_period_days STRING COLLATE UTF8_BINARY,
# MAGIC   availability_date STRING COLLATE UTF8_BINARY,
# MAGIC   resume_file_name STRING COLLATE UTF8_BINARY,
# MAGIC   resume_location STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_source STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_status STRING COLLATE UTF8_BINARY,
# MAGIC   consent_status STRING COLLATE UTF8_BINARY,
# MAGIC   notes STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested candidate data from candidates.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_COMPANIES
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_companies (
# MAGIC   company_id STRING COLLATE UTF8_BINARY,
# MAGIC   company_name STRING COLLATE UTF8_BINARY,
# MAGIC   industry STRING COLLATE UTF8_BINARY,
# MAGIC   company_type STRING COLLATE UTF8_BINARY,
# MAGIC   website STRING COLLATE UTF8_BINARY,
# MAGIC   company_location STRING COLLATE UTF8_BINARY,
# MAGIC   city STRING COLLATE UTF8_BINARY,
# MAGIC   state STRING COLLATE UTF8_BINARY,
# MAGIC   country STRING COLLATE UTF8_BINARY,
# MAGIC   company_size STRING COLLATE UTF8_BINARY,
# MAGIC   status STRING COLLATE UTF8_BINARY,
# MAGIC   notes STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY COMMENT 'Source file name',
# MAGIC   source_row_number INT COMMENT 'Row number in source file',
# MAGIC   source_file_modified_ts TIMESTAMP COMMENT 'Source file modification timestamp',
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY COMMENT 'Hash of the source file content',
# MAGIC   cfg_insert_ts TIMESTAMP COMMENT 'When this record was inserted into bronze',
# MAGIC   cfg_update_ts TIMESTAMP COMMENT 'When this record was last updated in bronze',
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY COMMENT 'Ingestion run that loaded this record',
# MAGIC   record_hash STRING COLLATE UTF8_BINARY COMMENT 'Hash of the record content for change detection')
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested company data from companies.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_CONTACTS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_contacts (
# MAGIC   contact_id STRING COLLATE UTF8_BINARY,
# MAGIC   company_id STRING COLLATE UTF8_BINARY,
# MAGIC   contact_name STRING COLLATE UTF8_BINARY,
# MAGIC   designation STRING COLLATE UTF8_BINARY,
# MAGIC   email STRING COLLATE UTF8_BINARY,
# MAGIC   phone STRING COLLATE UTF8_BINARY,
# MAGIC   linkedin_url STRING COLLATE UTF8_BINARY,
# MAGIC   contact_type STRING COLLATE UTF8_BINARY,
# MAGIC   status STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested contact data from contacts.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_INTERVIEWS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_interviews (
# MAGIC   interview_id STRING COLLATE UTF8_BINARY,
# MAGIC   submission_id STRING COLLATE UTF8_BINARY,
# MAGIC   job_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY,
# MAGIC   interview_round STRING COLLATE UTF8_BINARY,
# MAGIC   interview_type STRING COLLATE UTF8_BINARY,
# MAGIC   scheduled_ts STRING COLLATE UTF8_BINARY,
# MAGIC   completed_ts STRING COLLATE UTF8_BINARY,
# MAGIC   interviewer STRING COLLATE UTF8_BINARY,
# MAGIC   interview_status STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_feedback STRING COLLATE UTF8_BINARY,
# MAGIC   client_feedback STRING COLLATE UTF8_BINARY,
# MAGIC   result STRING COLLATE UTF8_BINARY,
# MAGIC   next_action STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested interview data from interviews.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_INVOICES
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_invoices (
# MAGIC   invoice_id STRING COLLATE UTF8_BINARY,
# MAGIC   placement_id STRING COLLATE UTF8_BINARY,
# MAGIC   company_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY,
# MAGIC   invoice_number STRING COLLATE UTF8_BINARY,
# MAGIC   invoice_date STRING COLLATE UTF8_BINARY,
# MAGIC   due_date STRING COLLATE UTF8_BINARY,
# MAGIC   invoice_amount STRING COLLATE UTF8_BINARY,
# MAGIC   tax_amount STRING COLLATE UTF8_BINARY,
# MAGIC   total_invoice_amount STRING COLLATE UTF8_BINARY,
# MAGIC   invoice_status STRING COLLATE UTF8_BINARY,
# MAGIC   payment_terms_days STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested invoice data from invoices.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_JOBS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_jobs (
# MAGIC   job_id STRING COLLATE UTF8_BINARY,
# MAGIC   company_id STRING COLLATE UTF8_BINARY,
# MAGIC   job_title STRING COLLATE UTF8_BINARY,
# MAGIC   job_description STRING COLLATE UTF8_BINARY,
# MAGIC   job_location STRING COLLATE UTF8_BINARY,
# MAGIC   work_mode STRING COLLATE UTF8_BINARY,
# MAGIC   employment_type STRING COLLATE UTF8_BINARY,
# MAGIC   experience_min_years STRING COLLATE UTF8_BINARY,
# MAGIC   experience_max_years STRING COLLATE UTF8_BINARY,
# MAGIC   mandatory_skills STRING COLLATE UTF8_BINARY,
# MAGIC   preferred_skills STRING COLLATE UTF8_BINARY,
# MAGIC   min_ctc STRING COLLATE UTF8_BINARY,
# MAGIC   max_ctc STRING COLLATE UTF8_BINARY,
# MAGIC   notice_period_requirement STRING COLLATE UTF8_BINARY,
# MAGIC   number_of_positions STRING COLLATE UTF8_BINARY,
# MAGIC   job_priority STRING COLLATE UTF8_BINARY,
# MAGIC   job_status STRING COLLATE UTF8_BINARY,
# MAGIC   job_open_date STRING COLLATE UTF8_BINARY,
# MAGIC   job_close_date STRING COLLATE UTF8_BINARY,
# MAGIC   job_owner STRING COLLATE UTF8_BINARY,
# MAGIC   notes STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested job data from jobs.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_OFFERS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_offers (
# MAGIC   offer_id STRING COLLATE UTF8_BINARY,
# MAGIC   submission_id STRING COLLATE UTF8_BINARY,
# MAGIC   job_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY,
# MAGIC   offer_date STRING COLLATE UTF8_BINARY,
# MAGIC   offered_ctc STRING COLLATE UTF8_BINARY,
# MAGIC   joining_date STRING COLLATE UTF8_BINARY,
# MAGIC   offer_status STRING COLLATE UTF8_BINARY,
# MAGIC   offer_expiry_date STRING COLLATE UTF8_BINARY,
# MAGIC   rejection_reason STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested offer data from offers.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_PAYMENTS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_payments (
# MAGIC   payment_id STRING COLLATE UTF8_BINARY,
# MAGIC   invoice_id STRING COLLATE UTF8_BINARY,
# MAGIC   company_id STRING COLLATE UTF8_BINARY,
# MAGIC   payment_date STRING COLLATE UTF8_BINARY,
# MAGIC   payment_amount STRING COLLATE UTF8_BINARY,
# MAGIC   payment_reference STRING COLLATE UTF8_BINARY,
# MAGIC   payment_status STRING COLLATE UTF8_BINARY,
# MAGIC   payment_mode STRING COLLATE UTF8_BINARY,
# MAGIC   notes STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested payment data from payments.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_PLACEMENTS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_placements (
# MAGIC   placement_id STRING COLLATE UTF8_BINARY,
# MAGIC   job_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY,
# MAGIC   company_id STRING COLLATE UTF8_BINARY,
# MAGIC   offer_id STRING COLLATE UTF8_BINARY,
# MAGIC   joining_date STRING COLLATE UTF8_BINARY,
# MAGIC   annual_ctc STRING COLLATE UTF8_BINARY,
# MAGIC   recruitment_fee_percentage STRING COLLATE UTF8_BINARY,
# MAGIC   recruitment_fee_amount STRING COLLATE UTF8_BINARY,
# MAGIC   replacement_period_days STRING COLLATE UTF8_BINARY,
# MAGIC   placement_status STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested placement data from placements.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_QUARANTINE
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_quarantine (
# MAGIC   quarantine_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique quarantine record ID',
# MAGIC   source_table STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Bronze table the record was destined for',
# MAGIC   source_file STRING COLLATE UTF8_BINARY COMMENT 'Source file name',
# MAGIC   source_row_number INT COMMENT 'Row number in source file',
# MAGIC   record_data STRING COLLATE UTF8_BINARY COMMENT 'JSON representation of the invalid record',
# MAGIC   dq_rule_failed STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Which DQ rule failed',
# MAGIC   error_message STRING COLLATE UTF8_BINARY COMMENT 'Detailed error message',
# MAGIC   quarantine_ts TIMESTAMP NOT NULL COMMENT 'When the record was quarantined',
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY COMMENT 'Ingestion run ID')
# MAGIC USING delta
# MAGIC COMMENT 'Quarantine table for records that failed data quality validation'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- BRONZE_SUBMISSIONS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.bronze.bronze_submissions (
# MAGIC   submission_id STRING COLLATE UTF8_BINARY,
# MAGIC   job_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY,
# MAGIC   submission_date STRING COLLATE UTF8_BINARY,
# MAGIC   submitted_by STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_match_score STRING COLLATE UTF8_BINARY,
# MAGIC   submission_status STRING COLLATE UTF8_BINARY,
# MAGIC   client_feedback STRING COLLATE UTF8_BINARY,
# MAGIC   client_feedback_date STRING COLLATE UTF8_BINARY,
# MAGIC   rejection_reason STRING COLLATE UTF8_BINARY,
# MAGIC   source_file STRING COLLATE UTF8_BINARY,
# MAGIC   source_row_number INT,
# MAGIC   source_file_modified_ts TIMESTAMP,
# MAGIC   source_file_hash STRING COLLATE UTF8_BINARY,
# MAGIC   cfg_insert_ts TIMESTAMP,
# MAGIC   cfg_update_ts TIMESTAMP,
# MAGIC   ingestion_run_id STRING COLLATE UTF8_BINARY,
# MAGIC   record_hash STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC COMMENT 'Bronze: raw ingested submission data from submissions.xlsx'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Silver Schema Header
# MAGIC %md
# MAGIC ## Silver Schema
# MAGIC
# MAGIC Cleansed and conformed data layer - 15 tables

# COMMAND ----------

# DBTITLE 1,Silver DDL Output
# Fetch and display all Silver schema DDL statements
catalog = "recruitment"
schema = "silver"

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
silver_tables = [row.tableName for row in tables]

print("SILVER SCHEMA - CREATE TABLE STATEMENTS")
print("="*80)
print(f"Total tables: {len(silver_tables)}\n")

for table in sorted(silver_tables):
    print(f"\n{'='*80}")
    print(f"-- {table.upper()}")
    print(f"{'='*80}\n")
    ddl = spark.sql(f"SHOW CREATE TABLE {catalog}.{schema}.{table}").collect()[0][0]
    print(ddl)
    print(";\n")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SILVER SCHEMA - CREATE TABLE STATEMENTS
# MAGIC -- ================================================================================
# MAGIC -- Total tables: 15
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- AUDIT_DATA_CHANGES
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.audit_data_changes (
# MAGIC   audit_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique audit entry ID (UUID)',
# MAGIC   table_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Fully qualified table name affected',
# MAGIC   record_key STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Business key of the affected record',
# MAGIC   operation STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'INSERT, UPDATE, DELETE, MERGE',
# MAGIC   old_record_hash STRING COLLATE UTF8_BINARY COMMENT 'Record hash before change',
# MAGIC   new_record_hash STRING COLLATE UTF8_BINARY COMMENT 'Record hash after change',
# MAGIC   changed_ts TIMESTAMP NOT NULL COMMENT 'When the change occurred',
# MAGIC   run_id STRING COLLATE UTF8_BINARY COMMENT 'Run ID that triggered the change')
# MAGIC USING delta
# MAGIC COMMENT 'Audit trail of all data changes — insert, update, delete with before/after hashes'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- CONFIG_BUSINESS_RULES
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.config_business_rules (
# MAGIC   rule_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Name of the configurable rule',
# MAGIC   rule_value STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Value of the rule (stored as string, cast as needed)',
# MAGIC   description STRING COLLATE UTF8_BINARY COMMENT 'Human-readable description of the rule',
# MAGIC   active_flag BOOLEAN NOT NULL COMMENT 'Whether the rule is currently active',
# MAGIC   effective_from DATE COMMENT 'Date from which the rule is effective',
# MAGIC   effective_to DATE COMMENT 'Date until which the rule is effective (NULL = no expiry)',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last updated timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Configurable business rules — fee percentages, match weights, payment terms, etc.'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- CONTROL_FILE_INGESTION
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.control_file_ingestion (
# MAGIC   ingestion_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique identifier for each ingestion event (UUID)',
# MAGIC   source_file STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Name of the source file processed',
# MAGIC   source_path STRING COLLATE UTF8_BINARY COMMENT 'Full path where the file was uploaded',
# MAGIC   target_table STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Target bronze table name',
# MAGIC   file_modified_ts TIMESTAMP COMMENT 'Last modification timestamp of the source file',
# MAGIC   file_hash STRING COLLATE UTF8_BINARY COMMENT 'Hash of the file content for change detection',
# MAGIC   load_start_ts TIMESTAMP NOT NULL COMMENT 'When the ingestion started',
# MAGIC   load_end_ts TIMESTAMP COMMENT 'When the ingestion completed',
# MAGIC   records_read INT COMMENT 'Number of records read from the source',
# MAGIC   records_inserted INT COMMENT 'Number of records inserted into bronze',
# MAGIC   records_updated INT COMMENT 'Number of records updated in bronze',
# MAGIC   records_rejected INT COMMENT 'Number of records rejected/quarantined',
# MAGIC   status STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'SUCCESS, FAILED, PARTIAL',
# MAGIC   error_message STRING COLLATE UTF8_BINARY COMMENT 'Error details if ingestion failed')
# MAGIC USING delta
# MAGIC COMMENT 'Tracks each file ingestion event — which file, when, how many records, status'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- CONTROL_JOB_RUN
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.control_job_run (
# MAGIC   run_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique run identifier (UUID)',
# MAGIC   job_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Name of the job/pipeline step',
# MAGIC   start_ts TIMESTAMP NOT NULL COMMENT 'Job start timestamp',
# MAGIC   end_ts TIMESTAMP COMMENT 'Job end timestamp',
# MAGIC   status STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'RUNNING, SUCCESS, FAILED, PARTIAL',
# MAGIC   records_processed INT COMMENT 'Number of records processed',
# MAGIC   error_message STRING COLLATE UTF8_BINARY COMMENT 'Error message if failed')
# MAGIC USING delta
# MAGIC COMMENT 'Tracks each pipeline job run — start, end, status, records processed'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- DIM_AGREEMENT
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.dim_agreement (
# MAGIC   agreement_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique agreement identifier',
# MAGIC   company_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_company',
# MAGIC   agreement_type STRING COLLATE UTF8_BINARY COMMENT 'STANDARD, EXCLUSIVE, RETAINER',
# MAGIC   agreement_start_date DATE COMMENT 'Agreement effective from',
# MAGIC   agreement_end_date DATE COMMENT 'Agreement valid until',
# MAGIC   fee_percentage DOUBLE COMMENT 'Recruitment fee percentage',
# MAGIC   payment_terms_days INT COMMENT 'Payment terms in days',
# MAGIC   replacement_period_days INT COMMENT 'Replacement guarantee period',
# MAGIC   candidate_ownership_days INT COMMENT 'Ownership period for submitted candidates',
# MAGIC   agreement_status STRING COLLATE UTF8_BINARY COMMENT 'ACTIVE, EXPIRED, TERMINATED',
# MAGIC   document_location STRING COLLATE UTF8_BINARY COMMENT 'Path to signed agreement document',
# MAGIC   notes STRING COLLATE UTF8_BINARY COMMENT 'Free text notes',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: agreement dimension — client agreements with fee terms'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- DIM_CANDIDATE
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.dim_candidate (
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique candidate identifier',
# MAGIC   candidate_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Candidate full name',
# MAGIC   email STRING COLLATE UTF8_BINARY COMMENT 'Email address (PII)',
# MAGIC   phone STRING COLLATE UTF8_BINARY COMMENT 'Phone number (PII)',
# MAGIC   alternate_phone STRING COLLATE UTF8_BINARY COMMENT 'Alternate phone (PII)',
# MAGIC   current_location STRING COLLATE UTF8_BINARY COMMENT 'Current city/location',
# MAGIC   preferred_location STRING COLLATE UTF8_BINARY COMMENT 'Preferred work location',
# MAGIC   total_experience_years DOUBLE COMMENT 'Total experience in years',
# MAGIC   relevant_experience_years DOUBLE COMMENT 'Relevant experience in years',
# MAGIC   current_company STRING COLLATE UTF8_BINARY COMMENT 'Current employer',
# MAGIC   current_designation STRING COLLATE UTF8_BINARY COMMENT 'Current job title',
# MAGIC   skills STRING COLLATE UTF8_BINARY COMMENT 'Comma-separated skills list',
# MAGIC   primary_skill STRING COLLATE UTF8_BINARY COMMENT 'Primary skill',
# MAGIC   secondary_skills STRING COLLATE UTF8_BINARY COMMENT 'Secondary skills',
# MAGIC   current_ctc DOUBLE COMMENT 'Current CTC (annual)',
# MAGIC   expected_ctc DOUBLE COMMENT 'Expected CTC (annual)',
# MAGIC   notice_period_days INT COMMENT 'Notice period in days',
# MAGIC   availability_date DATE COMMENT 'Date available to join',
# MAGIC   resume_file_name STRING COLLATE UTF8_BINARY COMMENT 'Resume file name',
# MAGIC   resume_location STRING COLLATE UTF8_BINARY COMMENT 'Resume storage location',
# MAGIC   candidate_source STRING COLLATE UTF8_BINARY COMMENT 'Sourcing channel',
# MAGIC   candidate_status STRING COLLATE UTF8_BINARY COMMENT 'ACTIVE, PLACED, INACTIVE, BLACKLISTED',
# MAGIC   consent_status STRING COLLATE UTF8_BINARY COMMENT 'Consent for data processing',
# MAGIC   notes STRING COLLATE UTF8_BINARY COMMENT 'Free text notes',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: candidate dimension — cleansed, validated, PII-flagged'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- DIM_COMPANY
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.dim_company (
# MAGIC   company_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Stable unique company identifier',
# MAGIC   company_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Company name',
# MAGIC   industry STRING COLLATE UTF8_BINARY COMMENT 'Industry sector',
# MAGIC   company_type STRING COLLATE UTF8_BINARY COMMENT 'Client, Prospect, etc.',
# MAGIC   website STRING COLLATE UTF8_BINARY COMMENT 'Company website URL',
# MAGIC   company_location STRING COLLATE UTF8_BINARY COMMENT 'General location description',
# MAGIC   city STRING COLLATE UTF8_BINARY COMMENT 'City',
# MAGIC   state STRING COLLATE UTF8_BINARY COMMENT 'State/Province',
# MAGIC   country STRING COLLATE UTF8_BINARY COMMENT 'Country',
# MAGIC   company_size STRING COLLATE UTF8_BINARY COMMENT 'Company size band (e.g. 50-200)',
# MAGIC   status STRING COLLATE UTF8_BINARY COMMENT 'ACTIVE, INACTIVE, PROSPECT',
# MAGIC   notes STRING COLLATE UTF8_BINARY COMMENT 'Free text notes',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: company dimension — cleansed and validated'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- DIM_CONTACT
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.dim_contact (
# MAGIC   contact_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique contact identifier',
# MAGIC   company_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_company',
# MAGIC   contact_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Contact person name',
# MAGIC   designation STRING COLLATE UTF8_BINARY COMMENT 'Job title',
# MAGIC   email STRING COLLATE UTF8_BINARY COMMENT 'Email address',
# MAGIC   phone STRING COLLATE UTF8_BINARY COMMENT 'Phone number',
# MAGIC   linkedin_url STRING COLLATE UTF8_BINARY COMMENT 'LinkedIn profile URL',
# MAGIC   contact_type STRING COLLATE UTF8_BINARY COMMENT 'PRIMARY, SECONDARY, etc.',
# MAGIC   status STRING COLLATE UTF8_BINARY COMMENT 'ACTIVE, INACTIVE',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: contact dimension — cleansed and validated'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- DIM_JOB
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.dim_job (
# MAGIC   job_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique job identifier',
# MAGIC   company_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_company',
# MAGIC   job_title STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Job title',
# MAGIC   job_description STRING COLLATE UTF8_BINARY COMMENT 'Full job description',
# MAGIC   job_location STRING COLLATE UTF8_BINARY COMMENT 'Job location',
# MAGIC   work_mode STRING COLLATE UTF8_BINARY COMMENT 'ONSITE, REMOTE, HYBRID',
# MAGIC   employment_type STRING COLLATE UTF8_BINARY COMMENT 'FULL_TIME, CONTRACT, etc.',
# MAGIC   experience_min_years DOUBLE COMMENT 'Minimum experience required',
# MAGIC   experience_max_years DOUBLE COMMENT 'Maximum experience required',
# MAGIC   mandatory_skills STRING COLLATE UTF8_BINARY COMMENT 'Required skills',
# MAGIC   preferred_skills STRING COLLATE UTF8_BINARY COMMENT 'Nice-to-have skills',
# MAGIC   min_ctc DOUBLE COMMENT 'Minimum CTC budget',
# MAGIC   max_ctc DOUBLE COMMENT 'Maximum CTC budget',
# MAGIC   notice_period_requirement INT COMMENT 'Max acceptable notice period days',
# MAGIC   number_of_positions INT COMMENT 'Number of open positions',
# MAGIC   job_priority STRING COLLATE UTF8_BINARY COMMENT 'HIGH, MEDIUM, LOW',
# MAGIC   job_status STRING COLLATE UTF8_BINARY COMMENT 'OPEN, ON_HOLD, CLOSED, CANCELLED',
# MAGIC   job_open_date DATE COMMENT 'Date the job was opened',
# MAGIC   job_close_date DATE COMMENT 'Date the job was closed',
# MAGIC   job_owner STRING COLLATE UTF8_BINARY COMMENT 'Recruiter responsible',
# MAGIC   notes STRING COLLATE UTF8_BINARY COMMENT 'Free text notes',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: job dimension — cleansed and validated'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- FACT_INTERVIEW
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.fact_interview (
# MAGIC   interview_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique interview identifier',
# MAGIC   submission_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to fact_submission',
# MAGIC   job_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_job',
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_candidate',
# MAGIC   interview_round INT COMMENT 'Interview round number (1, 2, 3...)',
# MAGIC   interview_type STRING COLLATE UTF8_BINARY COMMENT 'TECHNICAL, HR, MANAGERIAL, etc.',
# MAGIC   scheduled_ts TIMESTAMP COMMENT 'Scheduled date/time',
# MAGIC   completed_ts TIMESTAMP COMMENT 'Actual completion date/time',
# MAGIC   interviewer STRING COLLATE UTF8_BINARY COMMENT 'Interviewer name',
# MAGIC   interview_status STRING COLLATE UTF8_BINARY COMMENT 'SCHEDULED, COMPLETED, CANCELLED, NO_SHOW',
# MAGIC   candidate_feedback STRING COLLATE UTF8_BINARY COMMENT 'Candidate post-interview feedback',
# MAGIC   client_feedback STRING COLLATE UTF8_BINARY COMMENT 'Client/interviewer feedback',
# MAGIC   result STRING COLLATE UTF8_BINARY COMMENT 'PASS, FAIL, ON_HOLD',
# MAGIC   next_action STRING COLLATE UTF8_BINARY COMMENT 'Next step in pipeline',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: interview fact — preserves full interview history per round'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- FACT_INVOICE
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.fact_invoice (
# MAGIC   invoice_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique invoice identifier',
# MAGIC   placement_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to fact_placement',
# MAGIC   company_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_company',
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_candidate',
# MAGIC   invoice_number STRING COLLATE UTF8_BINARY COMMENT 'Human-readable invoice number',
# MAGIC   invoice_date DATE COMMENT 'Date invoice was issued',
# MAGIC   due_date DATE COMMENT 'Payment due date',
# MAGIC   invoice_amount DOUBLE COMMENT 'Pre-tax invoice amount',
# MAGIC   tax_amount DOUBLE COMMENT 'Tax/GST amount',
# MAGIC   total_invoice_amount DOUBLE COMMENT 'Total including tax',
# MAGIC   invoice_status STRING COLLATE UTF8_BINARY COMMENT 'DRAFT, SENT, PAID, PARTIALLY_PAID, OVERDUE, CANCELLED',
# MAGIC   payment_terms_days INT COMMENT 'Payment terms in days',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: invoice fact — invoices issued for placements'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- FACT_OFFER
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.fact_offer (
# MAGIC   offer_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique offer identifier',
# MAGIC   submission_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to fact_submission',
# MAGIC   job_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_job',
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_candidate',
# MAGIC   offer_date DATE COMMENT 'Date offer was made',
# MAGIC   offered_ctc DOUBLE COMMENT 'CTC offered (annual)',
# MAGIC   joining_date DATE COMMENT 'Expected joining date',
# MAGIC   offer_status STRING COLLATE UTF8_BINARY COMMENT 'PENDING, ACCEPTED, REJECTED, EXPIRED',
# MAGIC   offer_expiry_date DATE COMMENT 'Offer validity end date',
# MAGIC   rejection_reason STRING COLLATE UTF8_BINARY COMMENT 'Reason for rejection if any',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: offer fact — candidate offers with status tracking'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- FACT_PAYMENT
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.fact_payment (
# MAGIC   payment_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique payment identifier',
# MAGIC   invoice_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to fact_invoice',
# MAGIC   company_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_company',
# MAGIC   payment_date DATE COMMENT 'Date payment was received',
# MAGIC   payment_amount DOUBLE COMMENT 'Amount received',
# MAGIC   payment_reference STRING COLLATE UTF8_BINARY COMMENT 'Bank/payment reference',
# MAGIC   payment_status STRING COLLATE UTF8_BINARY COMMENT 'RECEIVED, PENDING, BOUNCED',
# MAGIC   payment_mode STRING COLLATE UTF8_BINARY COMMENT 'BANK_TRANSFER, CHEQUE, etc.',
# MAGIC   notes STRING COLLATE UTF8_BINARY COMMENT 'Free text notes',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: payment fact — payments received against invoices'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- FACT_PLACEMENT
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.fact_placement (
# MAGIC   placement_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique placement identifier',
# MAGIC   job_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_job',
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_candidate',
# MAGIC   company_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_company',
# MAGIC   offer_id STRING COLLATE UTF8_BINARY COMMENT 'FK to fact_offer',
# MAGIC   joining_date DATE COMMENT 'Actual joining date',
# MAGIC   annual_ctc DOUBLE COMMENT 'Confirmed annual CTC',
# MAGIC   recruitment_fee_percentage DOUBLE COMMENT 'Fee % agreed with client',
# MAGIC   recruitment_fee_amount DOUBLE COMMENT 'Calculated fee amount (CTC * % / 100)',
# MAGIC   replacement_period_days INT COMMENT 'Replacement guarantee period',
# MAGIC   placement_status STRING COLLATE UTF8_BINARY COMMENT 'JOINED, LEFT_WITHIN_GUARANTEE, REPLACED, ACTIVE',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: placement fact — confirmed placements with fee calculation'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- FACT_SUBMISSION
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.silver.fact_submission (
# MAGIC   submission_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Unique submission identifier',
# MAGIC   job_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_job',
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'FK to dim_candidate',
# MAGIC   submission_date DATE COMMENT 'Date candidate was submitted',
# MAGIC   submitted_by STRING COLLATE UTF8_BINARY COMMENT 'Recruiter who submitted',
# MAGIC   candidate_match_score DOUBLE COMMENT 'Internal matching score (0-100)',
# MAGIC   submission_status STRING COLLATE UTF8_BINARY COMMENT 'SUBMITTED, SHORTLISTED, REJECTED, etc.',
# MAGIC   client_feedback STRING COLLATE UTF8_BINARY COMMENT 'Client feedback text',
# MAGIC   client_feedback_date DATE COMMENT 'Date client gave feedback',
# MAGIC   rejection_reason STRING COLLATE UTF8_BINARY COMMENT 'Reason for rejection if any',
# MAGIC   created_ts TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC   updated_ts TIMESTAMP NOT NULL COMMENT 'Last update timestamp')
# MAGIC USING delta
# MAGIC COMMENT 'Silver: submission fact — candidate submitted to a job'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Gold Schema Header
# MAGIC %md
# MAGIC ## Gold Schema
# MAGIC
# MAGIC Business-level aggregates and views - 9 objects

# COMMAND ----------

# DBTITLE 1,Gold DDL Output
# Fetch and display all Gold schema DDL statements
catalog = "recruitment"
schema = "gold"

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
gold_tables = [row.tableName for row in tables]

print("GOLD SCHEMA - CREATE TABLE/VIEW STATEMENTS")
print("="*80)
print(f"Total objects: {len(gold_tables)}\n")

for table in sorted(gold_tables):
    print(f"\n{'='*80}")
    print(f"-- {table.upper()}")
    print(f"{'='*80}\n")
    ddl = spark.sql(f"SHOW CREATE TABLE {catalog}.{schema}.{table}").collect()[0][0]
    print(ddl)
    print(";\n")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOLD SCHEMA - CREATE TABLE/VIEW STATEMENTS
# MAGIC -- ================================================================================
# MAGIC -- Total objects: 9
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- GOLD_CANDIDATE_MATCHING
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.gold.gold_candidate_matching (
# MAGIC   job_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_id STRING COLLATE UTF8_BINARY,
# MAGIC   candidate_name STRING COLLATE UTF8_BINARY,
# MAGIC   job_title STRING COLLATE UTF8_BINARY,
# MAGIC   match_score DOUBLE,
# MAGIC   skill_match_score DOUBLE,
# MAGIC   experience_score DOUBLE,
# MAGIC   location_score DOUBLE,
# MAGIC   ctc_score DOUBLE,
# MAGIC   notice_period_score DOUBLE,
# MAGIC   recommendation STRING COLLATE UTF8_BINARY)
# MAGIC USING delta
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- GOLD_DATA_QUALITY_SUMMARY
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE TABLE recruitment.gold.gold_data_quality_summary (
# MAGIC   table_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Silver table being checked',
# MAGIC   check_name STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'Name of the DQ check',
# MAGIC   total_records BIGINT NOT NULL COMMENT 'Total records in scope',
# MAGIC   failed_records BIGINT NOT NULL COMMENT 'Records failing this check',
# MAGIC   pass_percentage DOUBLE COMMENT 'Percentage of records passing (0-100)',
# MAGIC   dq_status STRING COLLATE UTF8_BINARY NOT NULL COMMENT 'PASS, FAIL, WARNING',
# MAGIC   run_ts TIMESTAMP NOT NULL COMMENT 'When this DQ check was run')
# MAGIC USING delta
# MAGIC COMMENT 'Summary of all data quality checks across Silver tables'
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.domainMetadata' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd',
# MAGIC   'delta.parquet.format.version' = '2.12.0',
# MAGIC   'delta.parquet.format.version.afe.internal' = '2.12.0')
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- VW_CANDIDATE_MATCHING
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE VIEW recruitment.gold.vw_candidate_matching (
# MAGIC   job_id,
# MAGIC   candidate_id,
# MAGIC   candidate_name,
# MAGIC   job_title,
# MAGIC   match_score,
# MAGIC   skill_match_score,
# MAGIC   experience_score,
# MAGIC   location_score,
# MAGIC   ctc_score,
# MAGIC   notice_period_score,
# MAGIC   recommendation)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS SELECT
# MAGIC   m.job_id,
# MAGIC   m.candidate_id,
# MAGIC   m.candidate_name,
# MAGIC   m.job_title,
# MAGIC   m.match_score,
# MAGIC   m.skill_match_score,
# MAGIC   m.experience_score,
# MAGIC   m.location_score,
# MAGIC   m.ctc_score,
# MAGIC   m.notice_period_score,
# MAGIC   m.recommendation
# MAGIC FROM recruitment.gold.gold_candidate_matching m
# MAGIC WHERE m.match_score >= 50.0
# MAGIC ORDER BY m.job_id, m.match_score DESC
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- VW_CANDIDATE_PIPELINE
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE VIEW recruitment.gold.vw_candidate_pipeline (
# MAGIC   submission_id COMMENT 'Unique submission identifier',
# MAGIC   company_name COMMENT 'Company name',
# MAGIC   job_title COMMENT 'Job title',
# MAGIC   candidate_name COMMENT 'Candidate full name',
# MAGIC   submission_date COMMENT 'Date candidate was submitted',
# MAGIC   match_score COMMENT 'Internal matching score (0-100)',
# MAGIC   submission_status COMMENT 'SUBMITTED, SHORTLISTED, REJECTED, etc.',
# MAGIC   latest_interview_status,
# MAGIC   offer_status,
# MAGIC   placement_status)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS SELECT
# MAGIC   s.submission_id,
# MAGIC   c.company_name,
# MAGIC   j.job_title,
# MAGIC   cd.candidate_name,
# MAGIC   s.submission_date,
# MAGIC   s.candidate_match_score AS match_score,
# MAGIC   s.submission_status,
# MAGIC   (SELECT MAX(iv.interview_status) FROM recruitment.silver.fact_interview iv
# MAGIC    WHERE iv.submission_id = s.submission_id) AS latest_interview_status,
# MAGIC   (SELECT MAX(o.offer_status) FROM recruitment.silver.fact_offer o
# MAGIC    WHERE o.submission_id = s.submission_id) AS offer_status,
# MAGIC   (SELECT MAX(p.placement_status) FROM recruitment.silver.fact_placement p
# MAGIC    WHERE p.candidate_id = s.candidate_id AND p.job_id = s.job_id) AS placement_status
# MAGIC FROM recruitment.silver.fact_submission s
# MAGIC JOIN recruitment.silver.dim_job j ON s.job_id = j.job_id
# MAGIC JOIN recruitment.silver.dim_company c ON j.company_id = c.company_id
# MAGIC JOIN recruitment.silver.dim_candidate cd ON s.candidate_id = cd.candidate_id
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- VW_CLIENT_PIPELINE
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE VIEW recruitment.gold.vw_client_pipeline (
# MAGIC   company_id COMMENT 'Stable unique company identifier',
# MAGIC   company_name COMMENT 'Company name',
# MAGIC   industry COMMENT 'Industry sector',
# MAGIC   company_type COMMENT 'Client, Prospect, etc.',
# MAGIC   contact_name COMMENT 'Contact person name',
# MAGIC   contact_designation COMMENT 'Job title',
# MAGIC   contact_email COMMENT 'Email address',
# MAGIC   open_jobs,
# MAGIC   total_submissions,
# MAGIC   placements,
# MAGIC   total_revenue,
# MAGIC   client_status COMMENT 'ACTIVE, INACTIVE, PROSPECT')
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS SELECT
# MAGIC   c.company_id,
# MAGIC   c.company_name,
# MAGIC   c.industry,
# MAGIC   c.company_type,
# MAGIC   ct.contact_name,
# MAGIC   ct.designation AS contact_designation,
# MAGIC   ct.email AS contact_email,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.dim_job j WHERE j.company_id = c.company_id AND j.job_status = 'OPEN') AS open_jobs,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.fact_submission s
# MAGIC    JOIN recruitment.silver.dim_job j ON s.job_id = j.job_id
# MAGIC    WHERE j.company_id = c.company_id) AS total_submissions,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.fact_placement p WHERE p.company_id = c.company_id) AS placements,
# MAGIC   (SELECT COALESCE(SUM(p.recruitment_fee_amount), 0) FROM recruitment.silver.fact_placement p
# MAGIC    WHERE p.company_id = c.company_id) AS total_revenue,
# MAGIC   c.status AS client_status
# MAGIC FROM recruitment.silver.dim_company c
# MAGIC LEFT JOIN recruitment.silver.dim_contact ct ON c.company_id = ct.company_id AND ct.contact_type = 'PRIMARY'
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- VW_MANAGEMENT_DASHBOARD
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE VIEW recruitment.gold.vw_management_dashboard (
# MAGIC   active_clients,
# MAGIC   open_jobs,
# MAGIC   active_candidates,
# MAGIC   candidates_submitted,
# MAGIC   interviews_scheduled,
# MAGIC   offers,
# MAGIC   placements,
# MAGIC   total_revenue,
# MAGIC   outstanding_receivables,
# MAGIC   overdue_invoices,
# MAGIC   submission_to_interview_rate,
# MAGIC   interview_to_offer_rate,
# MAGIC   offer_to_joining_rate,
# MAGIC   overall_placement_conversion_rate)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS SELECT
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.dim_company WHERE status = 'ACTIVE') AS active_clients,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.dim_job WHERE job_status = 'OPEN') AS open_jobs,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.dim_candidate WHERE candidate_status = 'ACTIVE') AS active_candidates,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.fact_submission) AS candidates_submitted,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.fact_interview WHERE interview_status = 'SCHEDULED') AS interviews_scheduled,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.fact_offer WHERE offer_status IN ('PENDING','ACCEPTED')) AS offers,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.fact_placement) AS placements,
# MAGIC   (SELECT COALESCE(SUM(recruitment_fee_amount), 0) FROM recruitment.silver.fact_placement) AS total_revenue,
# MAGIC   (SELECT COALESCE(SUM(total_invoice_amount), 0) - COALESCE((SELECT SUM(payment_amount) FROM recruitment.silver.fact_payment), 0)
# MAGIC    FROM recruitment.silver.fact_invoice) AS outstanding_receivables,
# MAGIC   (SELECT COUNT(*) FROM recruitment.silver.fact_invoice i
# MAGIC    WHERE i.due_date < CURRENT_DATE()
# MAGIC    AND i.total_invoice_amount - COALESCE((SELECT SUM(p.payment_amount) FROM recruitment.silver.fact_payment p WHERE p.invoice_id = i.invoice_id), 0) > 0
# MAGIC   ) AS overdue_invoices,
# MAGIC   -- Conversion rates
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM recruitment.silver.fact_submission) > 0
# MAGIC     THEN ROUND((SELECT COUNT(DISTINCT submission_id) FROM recruitment.silver.fact_interview) * 100.0 / (SELECT COUNT(*) FROM recruitment.silver.fact_submission), 2)
# MAGIC     ELSE 0 END AS submission_to_interview_rate,
# MAGIC   CASE WHEN (SELECT COUNT(DISTINCT submission_id) FROM recruitment.silver.fact_interview) > 0
# MAGIC     THEN ROUND((SELECT COUNT(*) FROM recruitment.silver.fact_offer) * 100.0 / (SELECT COUNT(DISTINCT submission_id) FROM recruitment.silver.fact_interview), 2)
# MAGIC     ELSE 0 END AS interview_to_offer_rate,
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM recruitment.silver.fact_offer) > 0
# MAGIC     THEN ROUND((SELECT COUNT(*) FROM recruitment.silver.fact_placement) * 100.0 / (SELECT COUNT(*) FROM recruitment.silver.fact_offer), 2)
# MAGIC     ELSE 0 END AS offer_to_joining_rate,
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM recruitment.silver.fact_submission) > 0
# MAGIC     THEN ROUND((SELECT COUNT(*) FROM recruitment.silver.fact_placement) * 100.0 / (SELECT COUNT(*) FROM recruitment.silver.fact_submission), 2)
# MAGIC     ELSE 0 END AS overall_placement_conversion_rate
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- VW_OPEN_JOBS
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE VIEW recruitment.gold.vw_open_jobs (
# MAGIC   job_id COMMENT 'Unique job identifier',
# MAGIC   company_name COMMENT 'Company name',
# MAGIC   job_title COMMENT 'Job title',
# MAGIC   job_location COMMENT 'Job location',
# MAGIC   work_mode COMMENT 'ONSITE, REMOTE, HYBRID',
# MAGIC   experience_min_years COMMENT 'Minimum experience required',
# MAGIC   experience_max_years COMMENT 'Maximum experience required',
# MAGIC   mandatory_skills COMMENT 'Required skills',
# MAGIC   preferred_skills COMMENT 'Nice-to-have skills',
# MAGIC   number_of_positions COMMENT 'Number of open positions',
# MAGIC   job_priority COMMENT 'HIGH, MEDIUM, LOW',
# MAGIC   job_status COMMENT 'OPEN, ON_HOLD, CLOSED, CANCELLED',
# MAGIC   job_open_date COMMENT 'Date the job was opened',
# MAGIC   days_open)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS SELECT
# MAGIC   j.job_id,
# MAGIC   c.company_name,
# MAGIC   j.job_title,
# MAGIC   j.job_location,
# MAGIC   j.work_mode,
# MAGIC   j.experience_min_years,
# MAGIC   j.experience_max_years,
# MAGIC   j.mandatory_skills,
# MAGIC   j.preferred_skills,
# MAGIC   j.number_of_positions,
# MAGIC   j.job_priority,
# MAGIC   j.job_status,
# MAGIC   j.job_open_date,
# MAGIC   DATEDIFF(CURRENT_DATE(), j.job_open_date) AS days_open
# MAGIC FROM recruitment.silver.dim_job j
# MAGIC JOIN recruitment.silver.dim_company c ON j.company_id = c.company_id
# MAGIC WHERE j.job_status = 'OPEN'
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- VW_RECRUITMENT_FUNNEL
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE VIEW recruitment.gold.vw_recruitment_funnel (
# MAGIC   sourced,
# MAGIC   submitted,
# MAGIC   interviewed,
# MAGIC   offered,
# MAGIC   placed)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS SELECT
# MAGIC   COUNT(DISTINCT cd.candidate_id) AS sourced,
# MAGIC   COUNT(DISTINCT CASE WHEN s.submission_id IS NOT NULL THEN cd.candidate_id END) AS submitted,
# MAGIC   COUNT(DISTINCT CASE WHEN iv.interview_id IS NOT NULL THEN cd.candidate_id END) AS interviewed,
# MAGIC   COUNT(DISTINCT CASE WHEN o.offer_id IS NOT NULL THEN cd.candidate_id END) AS offered,
# MAGIC   COUNT(DISTINCT CASE WHEN p.placement_id IS NOT NULL THEN cd.candidate_id END) AS placed
# MAGIC FROM recruitment.silver.dim_candidate cd
# MAGIC LEFT JOIN recruitment.silver.fact_submission s ON s.candidate_id = cd.candidate_id
# MAGIC LEFT JOIN recruitment.silver.fact_interview iv ON iv.submission_id = s.submission_id
# MAGIC LEFT JOIN recruitment.silver.fact_offer o ON o.submission_id = s.submission_id
# MAGIC LEFT JOIN recruitment.silver.fact_placement p ON p.candidate_id = cd.candidate_id AND p.job_id = s.job_id
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- -- VW_REVENUE
# MAGIC -- ================================================================================
# MAGIC
# MAGIC CREATE VIEW recruitment.gold.vw_revenue (
# MAGIC   placement_id COMMENT 'Unique placement identifier',
# MAGIC   company_name COMMENT 'Company name',
# MAGIC   candidate_name COMMENT 'Candidate full name',
# MAGIC   annual_ctc COMMENT 'Confirmed annual CTC',
# MAGIC   recruitment_fee_percentage COMMENT 'Fee % agreed with client',
# MAGIC   recruitment_fee_amount COMMENT 'Calculated fee amount (CTC * % / 100)',
# MAGIC   invoice_id COMMENT 'Unique invoice identifier',
# MAGIC   invoice_number COMMENT 'Human-readable invoice number',
# MAGIC   invoice_amount COMMENT 'Total including tax',
# MAGIC   paid_amount,
# MAGIC   outstanding_amount,
# MAGIC   payment_status,
# MAGIC   due_date COMMENT 'Payment due date',
# MAGIC   days_overdue)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS SELECT
# MAGIC   p.placement_id,
# MAGIC   c.company_name,
# MAGIC   cd.candidate_name,
# MAGIC   p.annual_ctc,
# MAGIC   p.recruitment_fee_percentage,
# MAGIC   p.recruitment_fee_amount,
# MAGIC   i.invoice_id,
# MAGIC   i.invoice_number,
# MAGIC   i.total_invoice_amount AS invoice_amount,
# MAGIC   COALESCE(
# MAGIC     (SELECT SUM(pay.payment_amount) FROM recruitment.silver.fact_payment pay WHERE pay.invoice_id = i.invoice_id),
# MAGIC     0
# MAGIC   ) AS paid_amount,
# MAGIC   i.total_invoice_amount - COALESCE(
# MAGIC     (SELECT SUM(pay.payment_amount) FROM recruitment.silver.fact_payment pay WHERE pay.invoice_id = i.invoice_id),
# MAGIC     0
# MAGIC   ) AS outstanding_amount,
# MAGIC   CASE
# MAGIC     WHEN i.due_date < CURRENT_DATE() AND
# MAGIC          i.total_invoice_amount - COALESCE((SELECT SUM(pay.payment_amount) FROM recruitment.silver.fact_payment pay WHERE pay.invoice_id = i.invoice_id), 0) > 0
# MAGIC     THEN 'OVERDUE'
# MAGIC     ELSE i.invoice_status
# MAGIC   END AS payment_status,
# MAGIC   i.due_date,
# MAGIC   CASE WHEN i.due_date < CURRENT_DATE() AND
# MAGIC        i.total_invoice_amount - COALESCE((SELECT SUM(pay.payment_amount) FROM recruitment.silver.fact_payment pay WHERE pay.invoice_id = i.invoice_id), 0) > 0
# MAGIC   THEN DATEDIFF(CURRENT_DATE(), i.due_date) ELSE 0 END AS days_overdue
# MAGIC FROM recruitment.silver.fact_placement p
# MAGIC JOIN recruitment.silver.dim_company c ON p.company_id = c.company_id
# MAGIC JOIN recruitment.silver.dim_candidate cd ON p.candidate_id = cd.candidate_id
# MAGIC LEFT JOIN recruitment.silver.fact_invoice i ON i.placement_id = p.placement_id
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

