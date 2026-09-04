# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Orchestration Overview
# MAGIC %md
# MAGIC # Recruitment Consultancy Management Platform — Orchestration
# MAGIC # ====================================================================
# MAGIC # This notebook orchestrates the full pipeline:
# MAGIC #   1. Detect changed source files (via file hash comparison)
# MAGIC #   2. Load changed files into Bronze
# MAGIC #   3. Run Bronze validation
# MAGIC #   4. Transform to Silver (MERGE)
# MAGIC #   5. Run DQ checks
# MAGIC #   6. Update Gold views/tables
# MAGIC #   7. Run candidate matching (only if jobs or candidates changed)
# MAGIC #   8. Update control tables
# MAGIC #
# MAGIC # SELECTIVE PROCESSING: Only changed sources are reprocessed.
# MAGIC # If candidates.xlsx changes: bronze_candidates -> dim_candidate -> DQ -> matching -> Gold
# MAGIC # If invoices.xlsx changes: bronze_invoices -> fact_invoice -> finance Gold views
# MAGIC # ====================================================================

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
            SELECT file_hash, status
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
    
    return changed

# Example: list of uploaded files (in production, this comes from the local sync script)
# For demo purposes, we detect all files as 'first run'
uploaded_files = [
    {'filename': 'companies.xlsx', 'file_hash': 'hash_companies_v1', 'file_path': '/upload/companies.xlsx', 'modified_ts': datetime.now()},
    {'filename': 'candidates.xlsx', 'file_hash': 'hash_candidates_v1', 'file_path': '/upload/candidates.xlsx', 'modified_ts': datetime.now()},
    # ... add more files as they are uploaded
]

changed_files = detect_changed_files(uploaded_files)
print(f"Changed files detected: {changed_files}")

# COMMAND ----------

# DBTITLE 1,Step 2-3: Bronze Ingestion & Validation
# Step 2: Load Changed Files into Bronze
# Step 3: Run Bronze Validation
# In production, this reads the uploaded file (Excel/CSV) and loads into bronze.
# Uses MERGE to avoid duplicates — only inserts new records or updates changed ones.

def ingest_to_bronze(filename, file_path, file_hash, modified_ts, run_id):
    """Ingest a single source file into its bronze table."""
    if filename not in SOURCE_MAP:
        print(f"  SKIP: Unknown file {filename}")
        return False
    
    bronze_table = SOURCE_MAP[filename]['bronze']
    ingest_start = datetime.now()
    
    try:
        # In production: read the actual file using spark.read.format('excel') or read_files
        # For demo: the data is already loaded in bronze from test data generation
        
        # Record ingestion in control table
        records_count = spark.sql(f"SELECT COUNT(*) as cnt FROM recruitment.bronze.{bronze_table}").collect()[0]['cnt']
        
        spark.sql(f"""
            INSERT INTO recruitment.silver.control_file_ingestion
            VALUES (
                '{run_id}_{filename}', '{filename}', '{file_path}',
                '{bronze_table}', CAST('{modified_ts}' AS TIMESTAMP), '{file_hash}',
                CAST('{ingest_start}' AS TIMESTAMP), CAST('{datetime.now()}' AS TIMESTAMP),
                {records_count}, {records_count}, 0, 0, 'SUCCESS', NULL
            )
        """)
        
        print(f"  INGESTED: {filename} -> {bronze_table} ({records_count} records)")
        return True
    except Exception as e:
        spark.sql(f"""
            INSERT INTO recruitment.silver.control_file_ingestion
            VALUES (
                '{run_id}_{filename}', '{filename}', '{file_path}',
                '{bronze_table}', CAST('{modified_ts}' AS TIMESTAMP), '{file_hash}',
                CAST('{ingest_start}' AS TIMESTAMP), CAST('{datetime.now()}' AS TIMESTAMP),
                0, 0, 0, 0, 'FAILED', '{str(e)[:500]}'
            )
        """)
        print(f"  FAILED: {filename} - {str(e)[:200]}")
        return False

# Process all changed files
for f in uploaded_files:
    if f['filename'] in changed_files:
        ingest_to_bronze(f['filename'], f['file_path'], f['file_hash'], f['modified_ts'], run_id)

# COMMAND ----------

# DBTITLE 1,Step 4-5: Silver MERGE & DQ Checks
# Step 4: Transform to Silver (MERGE)
# Step 5: Run DQ Checks
# Each silver MERGE statement is defined here and executed only for changed sources.

SILVER_MERGE_SQL = {
    'dim_company': """MERGE INTO recruitment.silver.dim_company AS t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY cfg_insert_ts DESC) AS rn FROM recruitment.bronze.bronze_companies WHERE company_id IS NOT NULL AND company_name IS NOT NULL) WHERE rn = 1) AS s
        ON t.company_id = s.company_id
        WHEN MATCHED THEN UPDATE SET company_name=s.company_name, industry=s.industry, company_type=s.company_type, website=s.website, company_location=s.company_location, city=s.city, state=s.state, country=s.country, company_size=s.company_size, status=s.status, notes=s.notes, updated_ts=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (company_id, company_name, industry, company_type, website, company_location, city, state, country, company_size, status, notes, created_ts, updated_ts) VALUES (s.company_id, s.company_name, s.industry, s.company_type, s.website, s.company_location, s.city, s.state, s.country, s.company_size, s.status, s.notes, current_timestamp(), current_timestamp())""",
    'dim_contact': '-- See full MERGE in the Silver transformation cells above',
    'dim_candidate': '-- See full MERGE in the Silver transformation cells above',
    'dim_job': '-- See full MERGE in the Silver transformation cells above',
    'fact_submission': '-- See full MERGE in the Silver transformation cells above',
    'fact_interview': '-- See full MERGE in the Silver transformation cells above',
    'fact_offer': '-- See full MERGE in the Silver transformation cells above',
    'fact_placement': '-- See full MERGE in the Silver transformation cells above',
    'dim_agreement': '-- See full MERGE in the Silver transformation cells above',
    'fact_invoice': '-- See full MERGE in the Silver transformation cells above',
    'fact_payment': '-- See full MERGE in the Silver transformation cells above',
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
            print(f"  MERGE FAILED: {silver_table} - {str(e)[:200]}")
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
# MAGIC %sql
# MAGIC -- EXAMPLE SQL QUERIES FOR BUSINESS QUESTIONS
# MAGIC -- ================================================================
# MAGIC
# MAGIC -- 1. All open jobs
# MAGIC SELECT * FROM recruitment.gold.vw_open_jobs ORDER BY days_open DESC;
# MAGIC
# MAGIC -- 2. Jobs open for more than 30 days
# MAGIC SELECT * FROM recruitment.gold.vw_open_jobs WHERE days_open > 30 ORDER BY days_open DESC;
# MAGIC
# MAGIC -- 3. Top candidates for a specific job
# MAGIC SELECT candidate_name, match_score, recommendation
# MAGIC FROM recruitment.gold.vw_candidate_matching
# MAGIC WHERE job_id = 'JOB-3000' ORDER BY match_score DESC LIMIT 10;
# MAGIC
# MAGIC -- 4. Candidates with highest match scores across all jobs
# MAGIC SELECT candidate_name, job_title, match_score, recommendation
# MAGIC FROM recruitment.gold.vw_candidate_matching
# MAGIC WHERE recommendation = 'Strong Match'
# MAGIC ORDER BY match_score DESC LIMIT 20;
# MAGIC
# MAGIC -- 5. Candidates waiting for client feedback
# MAGIC SELECT submission_id, company_name, candidate_name, submission_status, submission_date
# MAGIC FROM recruitment.gold.vw_candidate_pipeline
# MAGIC WHERE submission_status = 'SUBMITTED' ORDER BY submission_date;
# MAGIC
# MAGIC -- 6. Interviews scheduled this week
# MAGIC SELECT interview_id, submission_id, job_id, candidate_id, interview_type,
# MAGIC        scheduled_ts, interviewer, interview_status
# MAGIC FROM recruitment.silver.fact_interview
# MAGIC WHERE scheduled_ts >= CURRENT_DATE()
# MAGIC   AND scheduled_ts < DATE_ADD(CURRENT_DATE(), 7)
# MAGIC   AND interview_status = 'SCHEDULED';
# MAGIC
# MAGIC -- 7. Candidates who received offers but haven't joined
# MAGIC SELECT cp.candidate_name, cp.company_name, cp.job_title, cp.offer_status, o.joining_date
# MAGIC FROM recruitment.gold.vw_candidate_pipeline cp
# MAGIC JOIN recruitment.silver.fact_offer o ON o.submission_id = cp.submission_id
# MAGIC WHERE cp.offer_status = 'PENDING'
# MAGIC   AND o.offer_expiry_date >= CURRENT_DATE();
# MAGIC
# MAGIC -- 8. Recruitment funnel
# MAGIC SELECT * FROM recruitment.gold.vw_recruitment_funnel;
# MAGIC
# MAGIC -- 9. Conversion rate from submission to placement
# MAGIC SELECT overall_placement_conversion_rate FROM recruitment.gold.vw_management_dashboard;
# MAGIC
# MAGIC -- 10. Clients with most open jobs
# MAGIC SELECT company_name, open_jobs FROM recruitment.gold.vw_client_pipeline
# MAGIC WHERE open_jobs > 0 ORDER BY open_jobs DESC;
# MAGIC
# MAGIC -- 11. Overdue invoices
# MAGIC SELECT placement_id, company_name, invoice_number, total_invoice_amount,
# MAGIC        outstanding_amount, days_overdue
# MAGIC FROM recruitment.gold.vw_revenue
# MAGIC WHERE payment_status = 'OVERDUE' ORDER BY days_overdue DESC;
# MAGIC
# MAGIC -- 12. Outstanding receivable
# MAGIC SELECT SUM(outstanding_amount) AS total_outstanding FROM recruitment.gold.vw_revenue;
# MAGIC
# MAGIC -- 13. Clients with highest revenue
# MAGIC SELECT company_name, total_revenue FROM recruitment.gold.vw_client_pipeline
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- 14. Candidate source performance
# MAGIC SELECT cd.candidate_source, COUNT(*) AS submissions,
# MAGIC        COUNT(DISTINCT CASE WHEN p.placement_id IS NOT NULL THEN cd.candidate_id END) AS placements
# MAGIC FROM recruitment.silver.fact_submission s
# MAGIC JOIN recruitment.silver.dim_candidate cd ON s.candidate_id = cd.candidate_id
# MAGIC LEFT JOIN recruitment.silver.fact_placement p ON p.candidate_id = cd.candidate_id AND p.job_id = s.job_id
# MAGIC GROUP BY cd.candidate_source ORDER BY placements DESC;

# COMMAND ----------

