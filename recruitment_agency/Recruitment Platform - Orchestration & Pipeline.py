# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Orchestration Overview
# MAGIC %md
# MAGIC # Recruitment Consultancy Management Platform — Orchestration
# MAGIC ## Complete End-to-End Data Pipeline
# MAGIC ====================================================================
# MAGIC ##### This notebook orchestrates the full pipeline:
# MAGIC #####   1. Detect changed source files (via file hash comparison)
# MAGIC #####   2. Load changed files into Bronze
# MAGIC #####   3. Run Bronze validation
# MAGIC #####   4. Transform to Silver (MERGE)
# MAGIC #####   5. Run DQ checks
# MAGIC #####   6. Update Gold views/tables
# MAGIC #####   7. Run candidate matching (only if jobs or candidates changed)
# MAGIC #####   8. Update control tables
# MAGIC #####
# MAGIC ### ✨ Recent Optimizations (2026-09-06)
# MAGIC * **Spark Connect Performance**: Optimized bronze ingestion cell
# MAGIC   - Cached schema access (single RPC vs. hundreds)
# MAGIC   - Replaced chained `.withColumn()` with single `.select()`
# MAGIC   - Trigger actions inside try/except for immediate error detection
# MAGIC * **Date Parsing Fix**: Fixed CAST_INVALID_INPUT errors in silver MERGE
# MAGIC   - Excel dates in M/d/yy format now parsed with `to_date('M/d/yy')`
# MAGIC   - Timestamps in M/d/yy H:mm format parsed with `to_timestamp('M/d/yy H:mm')`
# MAGIC * **Pipeline Status**: ✅ Fully operational - all 11 silver tables merging successfully
# MAGIC
# MAGIC ### Pipeline Architecture
# MAGIC **SELECTIVE PROCESSING**: Only changed sources are reprocessed.
# MAGIC ##### If candidates.xlsx changes: bronze_candidates -> dim_candidate -> DQ -> matching -> Gold
# MAGIC ##### If invoices.xlsx changes: bronze_invoices -> fact_invoice -> finance Gold views
# MAGIC ##### ====================================================================

# COMMAND ----------

# DBTITLE 1,🗑️ Clear All Tables (Run Before Fresh Pipeline Execution)
# CLEAR ALL BRONZE, SILVER, AND GOLD TABLES
# Run this cell to reset the entire pipeline before a fresh execution

print("Clearing all Bronze, Silver, and Gold tables...\n")

# Bronze tables
bronze_tables = [
    'bronze_companies', 'bronze_contacts', 'bronze_candidates', 'bronze_jobs',
    'bronze_submissions', 'bronze_interviews', 'bronze_offers', 'bronze_placements',
    'bronze_agreements', 'bronze_invoices', 'bronze_payments'
]

print("Clearing Bronze layer...")
for table in bronze_tables:
    spark.sql(f"TRUNCATE TABLE recruitment.bronze.{table}")
    print(f"  ✓ Cleared {table}")

# Silver tables
silver_tables = [
    'dim_company', 'dim_contact', 'dim_candidate', 'dim_job',
    'fact_submission', 'fact_interview', 'fact_offer', 'fact_placement',
    'dim_agreement', 'fact_invoice', 'fact_payment',
    'control_file_ingestion', 'control_job_run', 'config_business_rules'
]

print("\nClearing Silver layer...")
for table in silver_tables:
    spark.sql(f"TRUNCATE TABLE recruitment.silver.{table}")
    print(f"  ✓ Cleared {table}")

# Gold tables
gold_tables = [
    'gold_data_quality_summary',
    'gold_candidate_matching'
]

print("\nClearing Gold layer...")
for table in gold_tables:
    spark.sql(f"TRUNCATE TABLE recruitment.gold.{table}")
    print(f"  ✓ Cleared {table}")

print("\n" + "="*60)
print("All tables cleared successfully!")
print("You can now rerun the complete pipeline from Step 1.")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Configuration & Imports
import uuid, hashlib, builtins
from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Global run ID for this orchestration
run_id = str(uuid.uuid4())
run_start = datetime.now()
print(f"Orchestration run started: {run_id} at {run_start}")

# Source file to bronze table mapping
SOURCE_MAP = {
    'companies.xlsx':    {'bronze': 'bronze_companies',    'silver': 'dim_company',     'downstream': ['dim_company']},
    'contacts.xlsx':     {'bronze': 'bronze_contacts',     'silver': 'dim_contact',     'downstream': ['dim_contact']},
    'jobs.xlsx':         {'bronze': 'bronze_jobs',         'silver': 'dim_job',        'downstream': ['dim_job', 'candidate_matching']},
    'candidates.xlsx':   {'bronze': 'bronze_candidates',   'silver': 'dim_candidate',  'downstream': ['dim_candidate', 'candidate_matching']},
    'submissions.xlsx':  {'bronze': 'bronze_submissions', 'silver': 'fact_submission', 'downstream': ['fact_submission']},
    'interviews.xlsx':   {'bronze': 'bronze_interviews',  'silver': 'fact_interview', 'downstream': ['fact_interview']},
    'offers.xlsx':       {'bronze': 'bronze_offers',      'silver': 'fact_offer',     'downstream': ['fact_offer']},
    'placements.xlsx':  {'bronze': 'bronze_placements',  'silver': 'fact_placement', 'downstream': ['fact_placement', 'fact_invoice']},
    'agreements.xlsx':   {'bronze': 'bronze_agreements',  'silver': 'dim_agreement',  'downstream': ['dim_agreement']},
    'invoices.xlsx':    {'bronze': 'bronze_invoices',     'silver': 'fact_invoice',   'downstream': ['fact_invoice']},
    'payments.xlsx':    {'bronze': 'bronze_payments',     'silver': 'fact_payment',   'downstream': ['fact_payment']}
}

# COMMAND ----------

# DBTITLE 1,Step 1: Detect Changed Source Files
# Step 1: Detect Changed Source Files
# Compares file hashes in control_file_ingestion against incoming file hashes.
# Only changed files trigger reprocessing.

def detect_changed_files(uploaded_files):
    """uploaded_files: list of dicts with 'filename', 'file_hash', 'file_path', 'modified_ts'
    Returns list of filenames that have changed since last ingestion."""
    changed = []
    for f in uploaded_files:
        # Check if this file was already ingested with same hash
        existing = spark.sql(f"""
            SELECT file_hash, status, records_inserted
            FROM recruitment.silver.control_file_ingestion
            WHERE source_file = '{f['filename']}'
            ORDER BY load_start_ts DESC
            LIMIT 1
        """).collect()
        
        if len(existing) == 0:
            # Never ingested before
            changed.append(f['filename'])
        elif existing[0].file_hash != f['file_hash']:
            # Hash changed = file modified
            changed.append(f['filename'])
        elif existing[0].status == 'SUCCESS' and existing[0].records_inserted == 0:
            # Previous ingestion succeeded but loaded 0 records - re-ingest
            changed.append(f['filename'])
    
    return changed

# Read files from recruitment.bronze.uploads volume
import hashlib

VOLUME_PATH = "/Volumes/recruitment/bronze/uploads"

def calculate_file_hash(file_path):
    """Calculate MD5 hash of a file using Spark (Serverless compatible)."""
    try:
        # Read file content using Spark's binaryFile format
        binary_df = spark.read.format("binaryFile").load(file_path)
        content = binary_df.select("content").first()[0]
        return hashlib.md5(content).hexdigest()
    except Exception as e:
        print(f"Warning: Could not hash {file_path}: {e}")
        return None

# List all Excel files in the volume
try:
    files_in_volume = dbutils.fs.ls(VOLUME_PATH)
    uploaded_files = []
    
    for file_info in files_in_volume:
        if file_info.name.endswith('.xlsx'):
            file_path = file_info.path
            file_hash = calculate_file_hash(file_path)
            
            if file_hash:
                uploaded_files.append({
                    'filename': file_info.name,
                    'file_hash': file_hash,
                    'file_path': file_path,
                    'modified_ts': datetime.fromtimestamp(file_info.modificationTime / 1000)
                })
    
    print(f"Found {len(uploaded_files)} Excel files in {VOLUME_PATH}")
    
    changed_files = detect_changed_files(uploaded_files)
    print(f"Changed files detected: {changed_files}")
    
except Exception as e:
    print(f"Error reading from volume: {e}")
    print(f"Make sure the volume path {VOLUME_PATH} exists and contains files.")
    uploaded_files = []
    changed_files = []

# COMMAND ----------

# DBTITLE 1,Step 2-3: Bronze Ingestion & Validation (Optimized)
# Step 2: Load changed files into Bronze
# Step 3: Run Bronze validation
# Reads Excel files from volume, maps columns to bronze schemas, adds audit columns

import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import *

def ingest_to_bronze(file_info, source_map_entry, run_id):
    """Ingest one Excel file into its bronze table.
    
    Args:
        file_info: dict with 'filename', 'file_hash', 'file_path', 'modified_ts'
        source_map_entry: dict with 'bronze' table name
        run_id: current orchestration run ID
    """
    filename = file_info['filename']
    file_path = file_info['file_path']
    bronze_table = f"recruitment.bronze.{source_map_entry['bronze']}"
    
    print(f"\n  Ingesting {filename} -> {bronze_table}")
    
    try:
        # Record ingestion start in control table (matching actual schema)
        load_id = f"{run_id}_{filename}"
        spark.sql(f"""
            INSERT INTO recruitment.silver.control_file_ingestion
            (ingestion_id, source_file, source_path, target_table, file_modified_ts, file_hash, load_start_ts, load_end_ts, records_read, records_inserted, records_updated, records_rejected, status, error_message)
            VALUES (
                '{load_id}',
                '{filename}',
                '{file_path}',
                '{bronze_table}',
                CAST('{file_info['modified_ts']}' AS TIMESTAMP),
                '{file_info['file_hash']}',
                CAST('{datetime.now()}' AS TIMESTAMP),
                NULL,
                0,
                0,
                0,
                0,
                'IN_PROGRESS',
                NULL
            )
        """)
        
        # Read Excel file - extract headers from first row
        df_with_headers = spark.read.format("excel") \
            .option("header", "false") \
            .option("inferSchema", "false") \
            .load(file_path)
        
        # Extract first row as column names
        first_row = df_with_headers.first()
        header_names = [first_row[i] for i in range(len(df_with_headers.columns))]
        
        # Rename columns and skip first row
        df_raw = df_with_headers.toDF(*header_names).filter(F.col(header_names[0]) != header_names[0])
        
        # Get the bronze table schema to map columns
        bronze_desc = spark.sql(f"DESCRIBE {bronze_table}").collect()
        bronze_cols = {row.col_name.lower(): row.col_name for row in bronze_desc if not row.col_name.startswith('cfg_') and row.col_name not in ('source_file', 'source_row_number', 'source_file_modified_ts', 'source_file_hash', 'ingestion_run_id', 'record_hash')}
        
        # Clean and map Excel column names to bronze schema
        # SPARK CONNECT OPTIMIZATION: Cache df_raw.columns once, build rename map in one pass
        excel_cols_list = df_raw.columns  # Single RPC call
        rename_map = {}
        for excel_col in excel_cols_list:
            # Normalize column name: lowercase, strip, replace spaces/special chars
            normalized = excel_col.lower().strip().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
            
            # Find matching bronze column
            if normalized in bronze_cols:
                rename_map[excel_col] = bronze_cols[normalized]
        
        # Apply all renames in one pass using select()
        select_exprs = [
            F.col(c).alias(rename_map[c]) if c in rename_map else F.col(c)
            for c in excel_cols_list
        ]
        df_clean = df_raw.select(*select_exprs)
        
        # Add audit columns
        # SPARK CONNECT OPTIMIZATION: Cache columns once, use single select() instead of chained withColumn()
        clean_cols_list = df_clean.columns  # Single RPC call
        
        audit_exprs = [
            *[F.col(c) for c in clean_cols_list],  # Keep existing columns
            F.lit(filename).alias("source_file"),
            (F.monotonically_increasing_id().cast("int") + 1).alias("source_row_number"),
            F.lit(file_info['modified_ts']).alias("source_file_modified_ts"),
            F.lit(file_info['file_hash']).alias("source_file_hash"),
            F.current_timestamp().alias("cfg_insert_ts"),
            F.current_timestamp().alias("cfg_update_ts"),
            F.lit(run_id).alias("ingestion_run_id"),
            F.md5(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in clean_cols_list])).alias("record_hash")
        ]
        
        df_bronze = df_clean.select(*audit_exprs)
        
        # Get final bronze table schema and select/cast all columns
        final_bronze_desc = spark.sql(f"DESCRIBE {bronze_table}").collect()
        final_bronze_cols = [row.col_name for row in final_bronze_desc]
        
        # SPARK CONNECT OPTIMIZATION: Cache df_bronze.columns once
        bronze_cols_set = set(df_bronze.columns)
        
        # Select columns in the right order, add NULL for missing columns
        select_cols = []
        for col_name in final_bronze_cols:
            if col_name in bronze_cols_set:
                # Cast all business columns to STRING (bronze raw layer)
                if col_name not in ('source_row_number', 'source_file_modified_ts', 'cfg_insert_ts', 'cfg_update_ts'):
                    select_cols.append(F.col(col_name).cast("string").alias(col_name))
                else:
                    select_cols.append(F.col(col_name))
            else:
                # Add NULL for missing columns
                if col_name == 'source_row_number':
                    select_cols.append(F.lit(None).cast("int").alias(col_name))
                elif col_name in ('source_file_modified_ts', 'cfg_insert_ts', 'cfg_update_ts'):
                    select_cols.append(F.lit(None).cast("timestamp").alias(col_name))
                else:
                    select_cols.append(F.lit(None).cast("string").alias(col_name))
        
        df_final = df_bronze.select(*select_cols)
        
        # SPARK CONNECT: Trigger action INSIDE try/except to catch errors immediately
        row_count = df_final.count()
        df_final.write.mode("append").format("delta").saveAsTable(bronze_table)
        
        # Update control table with success
        spark.sql(f"""
            UPDATE recruitment.silver.control_file_ingestion
            SET load_end_ts = CAST('{datetime.now()}' AS TIMESTAMP),
                status = 'SUCCESS',
                records_read = {row_count},
                records_inserted = {row_count}
            WHERE ingestion_id = '{load_id}'
        """)
        
        print(f"    ✓ Loaded {row_count} records")
        return row_count
        
    except Exception as e:
        # Record failure in control table
        error_msg = str(e).replace("'", "''")[:500]  # Escape quotes, limit length
        spark.sql(f"""
            UPDATE recruitment.silver.control_file_ingestion
            SET load_end_ts = CAST('{datetime.now()}' AS TIMESTAMP),
                status = 'FAILED',
                error_message = '{error_msg}'
            WHERE ingestion_id = '{load_id}'
        """)
        print(f"    ✗ FAILED: {str(e)[:200]}")
        return 0

# Ingest all changed files
total_records = 0
for fname in changed_files:
    if fname in SOURCE_MAP:
        file_info = next((f for f in uploaded_files if f['filename'] == fname), None)
        if file_info:
            records = ingest_to_bronze(file_info, SOURCE_MAP[fname], run_id)
            total_records += records

print(f"\nBronze ingestion complete: {total_records} total records loaded")

# COMMAND ----------

# DBTITLE 1,Step 4-5: Silver MERGE & DQ Checks (Fixed Date Parsing)
# Step 4: Transform to Silver (MERGE)
# Step 5: Run DQ Checks
# Each silver MERGE statement is defined here and executed only for changed sources.
# DATE PARSING FIX: Excel dates are in M/d/yy format - using to_date() with format string

SILVER_MERGE_SQL = {
    'dim_company': """MERGE INTO recruitment.silver.dim_company AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_companies WHERE company_id IS NOT NULL AND company_name IS NOT NULL) WHERE rn = 1) AS s
        ON t.company_id = s.company_id
        WHEN MATCHED THEN UPDATE SET company_name=s.company_name, industry=s.industry, company_type=s.company_type, website=s.website, company_location=s.company_location, city=s.city, state=s.state, country=s.country, company_size=s.company_size, status=s.status, notes=s.notes, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (company_id, company_name, industry, company_type, website, company_location, city, state, country, company_size, status, notes, created_ts, updated_ts) VALUES (s.company_id, s.company_name, s.industry, s.company_type, s.website, s.company_location, s.city, s.state, s.country, s.company_size, s.status, s.notes, current_timestamp(), current_timestamp())""",
    
    'dim_contact': """MERGE INTO recruitment.silver.dim_contact AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_contacts WHERE contact_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.contact_id = s.contact_id
        WHEN MATCHED THEN UPDATE SET company_id=s.company_id, contact_name=s.contact_name, designation=s.designation, email=s.email, phone=s.phone, linkedin_url=s.linkedin_url, contact_type=s.contact_type, status=s.status, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (contact_id, company_id, contact_name, designation, email, phone, linkedin_url, contact_type, status, created_ts, updated_ts) VALUES (s.contact_id, s.company_id, s.contact_name, s.designation, s.email, s.phone, s.linkedin_url, s.contact_type, s.status, current_timestamp(), current_timestamp())""",
    
    'dim_candidate': """MERGE INTO recruitment.silver.dim_candidate AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_candidates WHERE candidate_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.candidate_id = s.candidate_id
        WHEN MATCHED THEN UPDATE SET candidate_name=s.candidate_name, email=s.email, phone=s.phone, alternate_phone=s.alternate_phone, current_location=s.current_location, preferred_location=s.preferred_location, total_experience_years=CAST(s.total_experience_years AS DOUBLE), relevant_experience_years=CAST(s.relevant_experience_years AS DOUBLE), current_company=s.current_company, current_designation=s.current_designation, skills=s.skills, primary_skill=s.primary_skill, secondary_skills=s.secondary_skills, current_ctc=CAST(s.current_ctc AS DOUBLE), expected_ctc=CAST(s.expected_ctc AS DOUBLE), notice_period_days=CAST(s.notice_period_days AS INT), availability_date=to_date(s.availability_date, 'M/d/yy'), resume_file_name=s.resume_file_name, resume_location=s.resume_location, candidate_source=s.candidate_source, candidate_status=s.candidate_status, consent_status=s.consent_status, notes=s.notes, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (candidate_id, candidate_name, email, phone, alternate_phone, current_location, preferred_location, total_experience_years, relevant_experience_years, current_company, current_designation, skills, primary_skill, secondary_skills, current_ctc, expected_ctc, notice_period_days, availability_date, resume_file_name, resume_location, candidate_source, candidate_status, consent_status, notes, created_ts, updated_ts) VALUES (s.candidate_id, s.candidate_name, s.email, s.phone, s.alternate_phone, s.current_location, s.preferred_location, CAST(s.total_experience_years AS DOUBLE), CAST(s.relevant_experience_years AS DOUBLE), s.current_company, s.current_designation, s.skills, s.primary_skill, s.secondary_skills, CAST(s.current_ctc AS DOUBLE), CAST(s.expected_ctc AS DOUBLE), CAST(s.notice_period_days AS INT), to_date(s.availability_date, 'M/d/yy'), s.resume_file_name, s.resume_location, s.candidate_source, s.candidate_status, s.consent_status, s.notes, current_timestamp(), current_timestamp())""",
    
    'dim_job': """MERGE INTO recruitment.silver.dim_job AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_jobs WHERE job_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.job_id = s.job_id
        WHEN MATCHED THEN UPDATE SET company_id=s.company_id, job_title=s.job_title, job_description=s.job_description, job_location=s.job_location, work_mode=s.work_mode, employment_type=s.employment_type, experience_min_years=CAST(s.experience_min_years AS DOUBLE), experience_max_years=CAST(s.experience_max_years AS DOUBLE), mandatory_skills=s.mandatory_skills, preferred_skills=s.preferred_skills, min_ctc=CAST(s.min_ctc AS DOUBLE), max_ctc=CAST(s.max_ctc AS DOUBLE), notice_period_requirement=CAST(s.notice_period_requirement AS INT), number_of_positions=CAST(s.number_of_positions AS INT), job_priority=s.job_priority, job_status=s.job_status, job_open_date=to_date(s.job_open_date, 'M/d/yy'), job_close_date=to_date(s.job_close_date, 'M/d/yy'), job_owner=s.job_owner, notes=s.notes, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (job_id, company_id, job_title, job_description, job_location, work_mode, employment_type, experience_min_years, experience_max_years, mandatory_skills, preferred_skills, min_ctc, max_ctc, notice_period_requirement, number_of_positions, job_priority, job_status, job_open_date, job_close_date, job_owner, notes, created_ts, updated_ts) VALUES (s.job_id, s.company_id, s.job_title, s.job_description, s.job_location, s.work_mode, s.employment_type, CAST(s.experience_min_years AS DOUBLE), CAST(s.experience_max_years AS DOUBLE), s.mandatory_skills, s.preferred_skills, CAST(s.min_ctc AS DOUBLE), CAST(s.max_ctc AS DOUBLE), CAST(s.notice_period_requirement AS INT), CAST(s.number_of_positions AS INT), s.job_priority, s.job_status, to_date(s.job_open_date, 'M/d/yy'), to_date(s.job_close_date, 'M/d/yy'), s.job_owner, s.notes, current_timestamp(), current_timestamp())""",
    
    'fact_submission': """MERGE INTO recruitment.silver.fact_submission AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY submission_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_submissions WHERE submission_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.submission_id = s.submission_id
        WHEN MATCHED THEN UPDATE SET job_id=s.job_id, candidate_id=s.candidate_id, submission_date=to_date(s.submission_date, 'M/d/yy'), submitted_by=s.submitted_by, candidate_match_score=CAST(s.candidate_match_score AS DOUBLE), submission_status=s.submission_status, client_feedback=s.client_feedback, client_feedback_date=to_date(s.client_feedback_date, 'M/d/yy'), rejection_reason=s.rejection_reason, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (submission_id, job_id, candidate_id, submission_date, submitted_by, candidate_match_score, submission_status, client_feedback, client_feedback_date, rejection_reason, created_ts, updated_ts) VALUES (s.submission_id, s.job_id, s.candidate_id, to_date(s.submission_date, 'M/d/yy'), s.submitted_by, CAST(s.candidate_match_score AS DOUBLE), s.submission_status, s.client_feedback, to_date(s.client_feedback_date, 'M/d/yy'), s.rejection_reason, current_timestamp(), current_timestamp())""",
    
    'fact_interview': """MERGE INTO recruitment.silver.fact_interview AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY interview_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_interviews WHERE interview_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.interview_id = s.interview_id
        WHEN MATCHED THEN UPDATE SET submission_id=s.submission_id, job_id=s.job_id, candidate_id=s.candidate_id, interview_round=CAST(s.interview_round AS INT), interview_type=s.interview_type, scheduled_ts=to_timestamp(s.scheduled_ts, 'M/d/yy H:mm'), completed_ts=to_timestamp(s.completed_ts, 'M/d/yy H:mm'), interviewer=s.interviewer, interview_status=s.interview_status, candidate_feedback=s.candidate_feedback, client_feedback=s.client_feedback, result=s.result, next_action=s.next_action, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (interview_id, submission_id, job_id, candidate_id, interview_round, interview_type, scheduled_ts, completed_ts, interviewer, interview_status, candidate_feedback, client_feedback, result, next_action, created_ts, updated_ts) VALUES (s.interview_id, s.submission_id, s.job_id, s.candidate_id, CAST(s.interview_round AS INT), s.interview_type, to_timestamp(s.scheduled_ts, 'M/d/yy H:mm'), to_timestamp(s.completed_ts, 'M/d/yy H:mm'), s.interviewer, s.interview_status, s.candidate_feedback, s.client_feedback, s.result, s.next_action, current_timestamp(), current_timestamp())""",
    
    'fact_offer': """MERGE INTO recruitment.silver.fact_offer AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_offers WHERE offer_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.offer_id = s.offer_id
        WHEN MATCHED THEN UPDATE SET submission_id=s.submission_id, job_id=s.job_id, candidate_id=s.candidate_id, offer_date=to_date(s.offer_date, 'M/d/yy'), offered_ctc=CAST(s.offered_ctc AS DOUBLE), joining_date=to_date(s.joining_date, 'M/d/yy'), offer_status=s.offer_status, offer_expiry_date=to_date(s.offer_expiry_date, 'M/d/yy'), rejection_reason=s.rejection_reason, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (offer_id, submission_id, job_id, candidate_id, offer_date, offered_ctc, joining_date, offer_status, offer_expiry_date, rejection_reason, created_ts, updated_ts) VALUES (s.offer_id, s.submission_id, s.job_id, s.candidate_id, to_date(s.offer_date, 'M/d/yy'), CAST(s.offered_ctc AS DOUBLE), to_date(s.joining_date, 'M/d/yy'), s.offer_status, to_date(s.offer_expiry_date, 'M/d/yy'), s.rejection_reason, current_timestamp(), current_timestamp())""",
    
    'fact_placement': """MERGE INTO recruitment.silver.fact_placement AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY placement_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_placements WHERE placement_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.placement_id = s.placement_id
        WHEN MATCHED THEN UPDATE SET job_id=s.job_id, candidate_id=s.candidate_id, company_id=s.company_id, offer_id=s.offer_id, joining_date=to_date(s.joining_date, 'M/d/yy'), annual_ctc=CAST(s.annual_ctc AS DOUBLE), recruitment_fee_percentage=CAST(s.recruitment_fee_percentage AS DOUBLE), recruitment_fee_amount=CAST(s.recruitment_fee_amount AS DOUBLE), replacement_period_days=CAST(s.replacement_period_days AS INT), placement_status=s.placement_status, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (placement_id, job_id, candidate_id, company_id, offer_id, joining_date, annual_ctc, recruitment_fee_percentage, recruitment_fee_amount, replacement_period_days, placement_status, created_ts, updated_ts) VALUES (s.placement_id, s.job_id, s.candidate_id, s.company_id, s.offer_id, to_date(s.joining_date, 'M/d/yy'), CAST(s.annual_ctc AS DOUBLE), CAST(s.recruitment_fee_percentage AS DOUBLE), CAST(s.recruitment_fee_amount AS DOUBLE), CAST(s.replacement_period_days AS INT), s.placement_status, current_timestamp(), current_timestamp())""",
    
    'dim_agreement': """MERGE INTO recruitment.silver.dim_agreement AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY agreement_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_agreements WHERE agreement_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.agreement_id = s.agreement_id
        WHEN MATCHED THEN UPDATE SET company_id=s.company_id, agreement_type=s.agreement_type, agreement_start_date=to_date(s.agreement_start_date, 'M/d/yy'), agreement_end_date=to_date(s.agreement_end_date, 'M/d/yy'), fee_percentage=CAST(s.fee_percentage AS DOUBLE), payment_terms_days=CAST(s.payment_terms_days AS INT), replacement_period_days=CAST(s.replacement_period_days AS INT), candidate_ownership_days=CAST(s.candidate_ownership_days AS INT), agreement_status=s.agreement_status, document_location=s.document_location, notes=s.notes, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (agreement_id, company_id, agreement_type, agreement_start_date, agreement_end_date, fee_percentage, payment_terms_days, replacement_period_days, candidate_ownership_days, agreement_status, document_location, notes, created_ts, updated_ts) VALUES (s.agreement_id, s.company_id, s.agreement_type, to_date(s.agreement_start_date, 'M/d/yy'), to_date(s.agreement_end_date, 'M/d/yy'), CAST(s.fee_percentage AS DOUBLE), CAST(s.payment_terms_days AS INT), CAST(s.replacement_period_days AS INT), CAST(s.candidate_ownership_days AS INT), s.agreement_status, s.document_location, s.notes, current_timestamp(), current_timestamp())""",
    
    'fact_invoice': """MERGE INTO recruitment.silver.fact_invoice AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_invoices WHERE invoice_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.invoice_id = s.invoice_id
        WHEN MATCHED THEN UPDATE SET placement_id=s.placement_id, company_id=s.company_id, candidate_id=s.candidate_id, invoice_number=s.invoice_number, invoice_date=to_date(s.invoice_date, 'M/d/yy'), due_date=to_date(s.due_date, 'M/d/yy'), invoice_amount=CAST(s.invoice_amount AS DOUBLE), tax_amount=CAST(s.tax_amount AS DOUBLE), total_invoice_amount=CAST(s.total_invoice_amount AS DOUBLE), invoice_status=s.invoice_status, payment_terms_days=CAST(s.payment_terms_days AS INT), updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (invoice_id, placement_id, company_id, candidate_id, invoice_number, invoice_date, due_date, invoice_amount, tax_amount, total_invoice_amount, invoice_status, payment_terms_days, created_ts, updated_ts) VALUES (s.invoice_id, s.placement_id, s.company_id, s.candidate_id, s.invoice_number, to_date(s.invoice_date, 'M/d/yy'), to_date(s.due_date, 'M/d/yy'), CAST(s.invoice_amount AS DOUBLE), CAST(s.tax_amount AS DOUBLE), CAST(s.total_invoice_amount AS DOUBLE), s.invoice_status, CAST(s.payment_terms_days AS INT), current_timestamp(), current_timestamp())""",
    
    'fact_payment': """MERGE INTO recruitment.silver.fact_payment AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_payments WHERE payment_id IS NOT NULL) WHERE rn = 1) AS s
        ON t.payment_id = s.payment_id
        WHEN MATCHED THEN UPDATE SET invoice_id=s.invoice_id, company_id=s.company_id, payment_date=to_date(s.payment_date, 'M/d/yy'), payment_amount=CAST(s.payment_amount AS DOUBLE), payment_reference=s.payment_reference, payment_status=s.payment_status, payment_mode=s.payment_mode, notes=s.notes, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (payment_id, invoice_id, company_id, payment_date, payment_amount, payment_reference, payment_status, payment_mode, notes, created_ts, updated_ts) VALUES (s.payment_id, s.invoice_id, s.company_id, to_date(s.payment_date, 'M/d/yy'), CAST(s.payment_amount AS DOUBLE), s.payment_reference, s.payment_status, s.payment_mode, s.notes, current_timestamp(), current_timestamp())"""
}

def run_silver_merge(silver_table, run_id):
    """Run the Silver MERGE for a specific table."""
    if silver_table in SILVER_MERGE_SQL and not SILVER_MERGE_SQL[silver_table].startswith('--'):
        try:
            spark.sql(SILVER_MERGE_SQL[silver_table])
            print(f"  MERGE OK: {silver_table}")
            # Record job run
            spark.sql(f"""
                INSERT INTO recruitment.silver.control_job_run
                VALUES ('{run_id}_{silver_table}', 'silver_merge_{silver_table}', CAST('{datetime.now()}' AS TIMESTAMP), CAST('{datetime.now()}' AS TIMESTAMP), 'SUCCESS', 0, NULL)
            """)
        except Exception as e:
            error_msg = str(e).replace("'", "''")[:200]
            print(f"  MERGE FAILED: {silver_table} - {error_msg}")
            spark.sql(f"""
                INSERT INTO recruitment.silver.control_job_run
                VALUES ('{run_id}_{silver_table}', 'silver_merge_{silver_table}', CAST('{datetime.now()}' AS TIMESTAMP), CAST('{datetime.now()}' AS TIMESTAMP), 'FAILED', 0, '{error_msg}')
            """)
    else:
        print(f"  SKIP (run manually): {silver_table}")

# Determine which silver tables need updating based on changed files
silver_tables_to_update = set()
for fname in changed_files:
    if fname in SOURCE_MAP:
        for st in SOURCE_MAP[fname]['downstream']:
            silver_tables_to_update.add(st)

print(f"Silver tables to update: {silver_tables_to_update}")
for st in sorted(silver_tables_to_update):
    run_silver_merge(st, run_id)

# COMMAND ----------

# DBTITLE 1,Step 6-8: Gold Views & Candidate Matching
# Step 6: Run DQ checks (refresh gold_data_quality_summary)
# Step 7: Gold views are views — they auto-update, no action needed
# Step 8: Run candidate matching if jobs or candidates changed

needs_matching = ('candidate_matching' in silver_tables_to_update or
                  'dim_job' in silver_tables_to_update or
                  'dim_candidate' in silver_tables_to_update)

# --- DQ CHECKS (always run if any silver table was updated) ---
if silver_tables_to_update:
    print("Running DQ checks...")
    dq_checks = [
        ("dim_company", "company_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.dim_company", "SELECT COUNT(*) FROM recruitment.silver.dim_company WHERE company_id IS NULL"),
        ("dim_company", "company_name_not_null", "SELECT COUNT(*) FROM recruitment.silver.dim_company", "SELECT COUNT(*) FROM recruitment.silver.dim_company WHERE company_name IS NULL"),
        ("dim_company", "company_id_unique", "SELECT COUNT(*) FROM recruitment.silver.dim_company", "SELECT COUNT(*) - COUNT(DISTINCT company_id) FROM recruitment.silver.dim_company"),
        ("dim_contact", "contact_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.dim_contact", "SELECT COUNT(*) FROM recruitment.silver.dim_contact WHERE contact_id IS NULL"),
        ("dim_contact", "company_id_fk", "SELECT COUNT(*) FROM recruitment.silver.dim_contact", "SELECT COUNT(*) FROM recruitment.silver.dim_contact c LEFT JOIN recruitment.silver.dim_company d ON c.company_id=d.company_id WHERE d.company_id IS NULL"),
        ("dim_candidate", "candidate_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.dim_candidate", "SELECT COUNT(*) FROM recruitment.silver.dim_candidate WHERE candidate_id IS NULL"),
        ("dim_candidate", "candidate_id_unique", "SELECT COUNT(*) FROM recruitment.silver.dim_candidate", "SELECT COUNT(*) - COUNT(DISTINCT candidate_id) FROM recruitment.silver.dim_candidate"),
        ("dim_candidate", "experience_not_negative", "SELECT COUNT(*) FROM recruitment.silver.dim_candidate WHERE total_experience_years IS NOT NULL", "SELECT COUNT(*) FROM recruitment.silver.dim_candidate WHERE total_experience_years < 0"),
        ("dim_candidate", "ctc_not_negative", "SELECT COUNT(*) FROM recruitment.silver.dim_candidate WHERE current_ctc IS NOT NULL", "SELECT COUNT(*) FROM recruitment.silver.dim_candidate WHERE current_ctc < 0"),
        ("dim_job", "job_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.dim_job", "SELECT COUNT(*) FROM recruitment.silver.dim_job WHERE job_id IS NULL"),
        ("dim_job", "company_id_fk", "SELECT COUNT(*) FROM recruitment.silver.dim_job", "SELECT COUNT(*) FROM recruitment.silver.dim_job j LEFT JOIN recruitment.silver.dim_company c ON j.company_id=c.company_id WHERE c.company_id IS NULL"),
        ("fact_submission", "submission_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.fact_submission", "SELECT COUNT(*) FROM recruitment.silver.fact_submission WHERE submission_id IS NULL"),
        ("fact_submission", "job_id_fk", "SELECT COUNT(*) FROM recruitment.silver.fact_submission", "SELECT COUNT(*) FROM recruitment.silver.fact_submission s LEFT JOIN recruitment.silver.dim_job j ON s.job_id=j.job_id WHERE j.job_id IS NULL"),
        ("fact_interview", "interview_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.fact_interview", "SELECT COUNT(*) FROM recruitment.silver.fact_interview WHERE interview_id IS NULL"),
        ("fact_offer", "offer_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.fact_offer", "SELECT COUNT(*) FROM recruitment.silver.fact_offer WHERE offer_id IS NULL"),
        ("fact_offer", "joining_after_offer", "SELECT COUNT(*) FROM recruitment.silver.fact_offer WHERE offer_date IS NOT NULL AND joining_date IS NOT NULL", "SELECT COUNT(*) FROM recruitment.silver.fact_offer WHERE offer_date IS NOT NULL AND joining_date IS NOT NULL AND joining_date < offer_date"),
        ("fact_placement", "placement_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.fact_placement", "SELECT COUNT(*) FROM recruitment.silver.fact_placement WHERE placement_id IS NULL"),
        ("fact_invoice", "invoice_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.fact_invoice", "SELECT COUNT(*) FROM recruitment.silver.fact_invoice WHERE invoice_id IS NULL"),
        ("fact_invoice", "amount_not_negative", "SELECT COUNT(*) FROM recruitment.silver.fact_invoice", "SELECT COUNT(*) FROM recruitment.silver.fact_invoice WHERE invoice_amount < 0"),
        ("fact_payment", "payment_id_not_null", "SELECT COUNT(*) FROM recruitment.silver.fact_payment", "SELECT COUNT(*) FROM recruitment.silver.fact_payment WHERE payment_id IS NULL"),
        ("fact_payment", "amount_not_negative", "SELECT COUNT(*) FROM recruitment.silver.fact_payment", "SELECT COUNT(*) FROM recruitment.silver.fact_payment WHERE payment_amount < 0"),
        ("fact_payment", "invoice_id_fk", "SELECT COUNT(*) FROM recruitment.silver.fact_payment", "SELECT COUNT(*) FROM recruitment.silver.fact_payment p LEFT JOIN recruitment.silver.fact_invoice i ON p.invoice_id=i.invoice_id WHERE i.invoice_id IS NULL"),
    ]
    spark.sql("DELETE FROM recruitment.gold.gold_data_quality_summary")
    dq_results = []
    for table_name, check_name, total_sql, failed_sql in dq_checks:
        total = spark.sql(total_sql).collect()[0][0]
        failed = spark.sql(failed_sql).collect()[0][0]
        pct = builtins.round(((total - failed) / total * 100) if total > 0 else 100.0, 2)
        status = "PASS" if failed == 0 else ("WARNING" if pct >= 95 else "FAIL")
        dq_results.append((table_name, check_name, total, failed, pct, status))
    schema_dq = StructType([StructField("table_name", StringType()), StructField("check_name", StringType()), StructField("total_records", LongType()), StructField("failed_records", LongType()), StructField("pass_percentage", DoubleType()), StructField("dq_status", StringType())])
    df_dq = spark.createDataFrame(dq_results, schema_dq).withColumn("run_ts", F.current_timestamp())
    df_dq.write.mode("append").format("delta").saveAsTable("recruitment.gold.gold_data_quality_summary")
    pass_count = builtins.sum(1 for r in dq_results if r[5] == "PASS")
    fail_count = builtins.sum(1 for r in dq_results if r[5] != "PASS")
    print(f"  DQ checks: {pass_count} PASS, {fail_count} issues")
else:
    print("No DQ checks needed (no silver tables updated)")

# --- CANDIDATE MATCHING ENGINE ---
if needs_matching:
    print("\nRunning candidate matching engine...")
    # Read configurable weights
    config = {r.rule_name: r.rule_value for r in spark.sql(
        "SELECT rule_name, CAST(rule_value AS DOUBLE) AS rule_value FROM recruitment.silver.config_business_rules WHERE active_flag = true"
    ).collect()}
    w_mand = config.get("match_weight_mandatory_skills", 0.40)
    w_pref = config.get("match_weight_preferred_skills", 0.15)
    w_exp = config.get("match_weight_experience", 0.20)
    w_loc = config.get("match_weight_location", 0.10)
    w_ctc = config.get("match_weight_ctc", 0.10)
    w_np = config.get("match_weight_notice_period", 0.05)
    th_strong = config.get("match_threshold_strong", 85)
    th_good = config.get("match_threshold_good", 70)
    th_review = config.get("match_threshold_review", 50)

    jobs = spark.sql("SELECT * FROM recruitment.silver.dim_job").collect()
    candidates = spark.sql("""
        SELECT candidate_id, candidate_name, skills, total_experience_years,
               preferred_location, current_ctc, expected_ctc, notice_period_days
        FROM recruitment.silver.dim_candidate
    """).collect()

    match_results = []
    for job in jobs:
        job_mand = set([s.strip().lower() for s in (job.mandatory_skills or "").split(",") if s.strip()])
        job_pref = set([s.strip().lower() for s in (job.preferred_skills or "").split(",") if s.strip()])
        job_loc = (job.job_location or "").lower()
        for cand in candidates:
            cand_skills = set([s.strip().lower() for s in (cand.skills or "").split(",") if s.strip()])
            # Mandatory skills (40%)
            mand_score = (len(job_mand & cand_skills) / len(job_mand) * 100) if job_mand else 100.0
            # Preferred skills (15%)
            pref_score = (len(job_pref & cand_skills) / len(job_pref) * 100) if job_pref else 100.0
            # Experience (20%)
            if job.experience_min_years and job.experience_max_years and cand.total_experience_years:
                if job.experience_min_years <= cand.total_experience_years <= job.experience_max_years:
                    exp_score = 100.0
                elif cand.total_experience_years < job.experience_min_years:
                    exp_score = builtins.max(0, (cand.total_experience_years / job.experience_min_years) * 100)
                else:
                    exp_score = builtins.max(0, 100 - (cand.total_experience_years - job.experience_max_years) * 5)
            else:
                exp_score = 50.0
            # Location (10%)
            if job_loc and cand.preferred_location:
                cl = (cand.preferred_location or "").lower()
                loc_score = 100.0 if (job_loc == cl or cl == "any" or job_loc == "remote" or cl == "remote") else 0.0
            else:
                loc_score = 50.0
            # CTC (10%)
            if job.min_ctc and job.max_ctc and cand.expected_ctc:
                if job.min_ctc <= cand.expected_ctc <= job.max_ctc:
                    ctc_score = 100.0
                elif cand.expected_ctc < job.min_ctc:
                    ctc_score = 80.0
                else:
                    ctc_score = builtins.max(0, 100 - ((cand.expected_ctc - job.max_ctc) / job.max_ctc * 100 * 2))
            else:
                ctc_score = 50.0
            # Notice period (5%)
            if job.notice_period_requirement and cand.notice_period_days is not None:
                np_score = 100.0 if cand.notice_period_days <= job.notice_period_requirement else builtins.max(0, 100 - (cand.notice_period_days - job.notice_period_requirement) * 2)
            else:
                np_score = 50.0
            total_score = builtins.round(mand_score * w_mand + pref_score * w_pref + exp_score * w_exp + loc_score * w_loc + ctc_score * w_ctc + np_score * w_np, 2)
            rec = "Strong Match" if total_score >= th_strong else ("Good Match" if total_score >= th_good else ("Review" if total_score >= th_review else "Weak Match"))
            match_results.append((job.job_id, cand.candidate_id, cand.candidate_name, job.job_title, total_score, builtins.round(mand_score, 2), builtins.round(exp_score, 2), builtins.round(loc_score, 2), builtins.round(ctc_score, 2), builtins.round(np_score, 2), rec))

    schema_m = StructType([StructField("job_id", StringType()), StructField("candidate_id", StringType()), StructField("candidate_name", StringType()), StructField("job_title", StringType()), StructField("match_score", DoubleType()), StructField("skill_match_score", DoubleType()), StructField("experience_score", DoubleType()), StructField("location_score", DoubleType()), StructField("ctc_score", DoubleType()), StructField("notice_period_score", DoubleType()), StructField("recommendation", StringType())])
    df_match = spark.createDataFrame(match_results, schema_m)
    df_match.write.mode("overwrite").format("delta").saveAsTable("recruitment.gold.gold_candidate_matching")
    print(f"  Candidate matching: {len(match_results)} job-candidate pairs scored")
else:
    print("No candidate matching needed (jobs/candidates unchanged)")

# Step 9: Final control table update
run_end = datetime.now()
duration = (run_end - run_start).total_seconds()
spark.sql(f"""
    INSERT INTO recruitment.silver.control_job_run
    VALUES ('{run_id}', 'orchestration_full',
            CAST('{run_start}' AS TIMESTAMP), CAST('{run_end}' AS TIMESTAMP),
            'SUCCESS', {len(changed_files)}, NULL)
""")
print(f"\n{'='*60}")
print(f"Orchestration complete in {duration:.1f}s")
print(f"Run ID: {run_id}")
print(f"Changed files: {len(changed_files)}")
print(f"Silver tables updated: {len(silver_tables_to_update)}")
print(f"DQ checks run: {'Yes' if silver_tables_to_update else 'No'}")
print(f"Candidate matching run: {'Yes' if needs_matching else 'No'}")
print(f"{'='*60}")

# COMMAND ----------

# DBTITLE 1,Step 10: Local File Sync Script Specification
# LOCAL FILE SYNC SCRIPT SPECIFICATION
# ================================================================
# Databricks CANNOT directly monitor your laptop filesystem.
# You need a small Python script on your laptop that:
#   1. Watches the source folder for changes
#   2. Uploads changed files to Databricks (via Volume or DBFS)
#   3. Triggers the orchestration notebook (via Jobs API)
#
# The script below is a TEMPLATE to run on your laptop (not in Databricks).
# Install: pip install databricks-sdk watchdog
# ================================================================
#
# import os, hashlib, time
# from databricks.sdk import WorkspaceClient
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
#
# SOURCE_DIR = "./source_files"
# VOLUME_PATH = "/Volumes/recruitment/bronze/uploads"
# JOB_ID = "<your-orchestration-job-id>"
#
# def file_hash(filepath):
#     with open(filepath, 'rb') as f:
#         return hashlib.md5(f.read()).hexdigest()
#
# class SyncHandler(FileSystemEventHandler):
#     def on_modified(self, event):
#         if event.src_path.endswith('.xlsx'):
#             filename = os.path.basename(event.src_path)
#             fhash = file_hash(event.src_path)
#             mod_ts = os.path.getmtime(event.src_path)
#             print(f"Detected change: {filename} (hash: {fhash[:8]}...)")
#             # Upload to Databricks volume
#             w = WorkspaceClient()
#             w.files.upload(f"{VOLUME_PATH}/{filename}", open(event.src_path, 'rb'), overwrite=True)
#             # Trigger orchestration job
#             w.jobs.run_now(JOB_ID)
#             print(f"Uploaded and triggered pipeline for {filename}")
#
# observer = Observer()
# observer.schedule(SyncHandler(), SOURCE_DIR, recursive=False)
# observer.start()
# print(f"Watching {SOURCE_DIR} for changes...")
# try:
#     while True:
#         time.sleep(1)
# except KeyboardInterrupt:
#     observer.stop()
# observer.join()
# ================================================================

# COMMAND ----------

# DBTITLE 1,Example SQL Queries
# %sql
# -- EXAMPLE SQL QUERIES FOR BUSINESS QUESTIONS
# -- ================================================================

# -- 1. All open jobs
# SELECT * FROM recruitment.gold.vw_open_jobs ORDER BY days_open DESC;

# -- 2. Jobs open for more than 30 days
# SELECT * FROM recruitment.gold.vw_open_jobs WHERE days_open > 30 ORDER BY days_open DESC;

# -- 3. Top candidates for a specific job
# SELECT candidate_name, match_score, recommendation
# FROM recruitment.gold.vw_candidate_matching
# WHERE job_id = 'JOB-3000' ORDER BY match_score DESC LIMIT 10;

# -- 4. Candidates with highest match scores across all jobs
# SELECT candidate_name, job_title, match_score, recommendation
# FROM recruitment.gold.vw_candidate_matching
# WHERE recommendation = 'Strong Match'
# ORDER BY match_score DESC LIMIT 20;

# -- 5. Candidates waiting for client feedback
# SELECT submission_id, company_name, candidate_name, submission_status, submission_date
# FROM recruitment.gold.vw_candidate_pipeline
# WHERE submission_status = 'SUBMITTED' ORDER BY submission_date;

# -- 6. Interviews scheduled this week
# SELECT interview_id, submission_id, job_id, candidate_id, interview_type,
#        scheduled_ts, interviewer, interview_status
# FROM recruitment.silver.fact_interview
# WHERE scheduled_ts >= CURRENT_DATE()
#   AND scheduled_ts < DATE_ADD(CURRENT_DATE(), 7)
#   AND interview_status = 'SCHEDULED';

# -- 7. Candidates who received offers but haven't joined
# SELECT cp.candidate_name, cp.company_name, cp.job_title, cp.offer_status, o.joining_date
# FROM recruitment.gold.vw_candidate_pipeline cp
# JOIN recruitment.silver.fact_offer o ON o.submission_id = cp.submission_id
# WHERE cp.offer_status = 'PENDING'
#   AND o.offer_expiry_date >= CURRENT_DATE();

# -- 8. Recruitment funnel
# SELECT * FROM recruitment.gold.vw_recruitment_funnel;

# -- 9. Conversion rate from submission to placement
# SELECT overall_placement_conversion_rate FROM recruitment.gold.vw_management_dashboard;

# -- 10. Clients with most open jobs
# SELECT company_name, open_jobs FROM recruitment.gold.vw_client_pipeline
# WHERE open_jobs > 0 ORDER BY open_jobs DESC;

# -- 11. Overdue invoices
# SELECT placement_id, company_name, invoice_number, total_invoice_amount,
#        outstanding_amount, days_overdue
# FROM recruitment.gold.vw_revenue
# WHERE payment_status = 'OVERDUE' ORDER BY days_overdue DESC;

# -- 12. Outstanding receivable
# SELECT SUM(outstanding_amount) AS total_outstanding FROM recruitment.gold.vw_revenue;

# -- 13. Clients with highest revenue
# SELECT company_name, total_revenue FROM recruitment.gold.vw_client_pipeline
# ORDER BY total_revenue DESC;

# -- 14. Candidate source performance
# SELECT cd.candidate_source, COUNT(*) AS submissions,
#        COUNT(DISTINCT CASE WHEN p.placement_id IS NOT NULL THEN cd.candidate_id END) AS placements
# FROM recruitment.silver.fact_submission s
# JOIN recruitment.silver.dim_candidate cd ON s.candidate_id = cd.candidate_id
# LEFT JOIN recruitment.silver.fact_placement p ON p.candidate_id = cd.candidate_id AND p.job_id = s.job_id
# GROUP BY cd.candidate_source ORDER BY placements DESC;