# Databricks notebook source
# DBTITLE 1,README - Recruitment Platform Documentation
# MAGIC %md
# MAGIC # Recruitment Consultancy Management Platform
# MAGIC
# MAGIC ## Overview
# MAGIC A complete MVP for managing the recruitment lifecycle built entirely on Databricks:
# MAGIC **Client Acquisition -> Client Contacts -> Job Requirements -> Candidate Sourcing -> Screening -> Matching -> Submission -> Interviews -> Offers -> Joining -> Placement -> Fee -> Invoice -> Payment -> Analytics**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC Local Laptop (Excel files)
# MAGIC     |
# MAGIC     v  [Local Python Sync Script] -- watches for file changes, uploads to Databricks Volume
# MAGIC     |
# MAGIC     v
# MAGIC BRONZE (recruitment.bronze) -- raw ingested data with ingestion metadata
# MAGIC     |
# MAGIC     v  [MERGE with type casting, dedup, validation]
# MAGIC SILVER (recruitment.silver) -- cleansed, normalized, validated business data model
# MAGIC     |
# MAGIC     v  [Views, aggregations, matching engine]
# MAGIC GOLD (recruitment.gold) -- business-friendly views for analytics and Genie
# MAGIC     |
# MAGIC     v
# MAGIC GENIE SPACE -- natural-language Q&A over Gold layer
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Catalog & Schema Structure
# MAGIC
# MAGIC | Layer | Catalog.Schema | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | Bronze | recruitment.bronze | Raw ingested source data |
# MAGIC | Silver | recruitment.silver | Cleansed, validated, normalized |
# MAGIC | Gold | recruitment.gold | Business-friendly views & matching |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Complete Table List
# MAGIC
# MAGIC ### Bronze Tables (12)
# MAGIC | Table | Source File | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | bronze_companies | companies.xlsx | Raw company data |
# MAGIC | bronze_contacts | contacts.xlsx | Raw contact data |
# MAGIC | bronze_jobs | jobs.xlsx | Raw job data |
# MAGIC | bronze_candidates | candidates.xlsx | Raw candidate data |
# MAGIC | bronze_submissions | submissions.xlsx | Raw submission data |
# MAGIC | bronze_interviews | interviews.xlsx | Raw interview data |
# MAGIC | bronze_offers | offers.xlsx | Raw offer data |
# MAGIC | bronze_placements | placements.xlsx | Raw placement data |
# MAGIC | bronze_agreements | agreements.xlsx | Raw agreement data |
# MAGIC | bronze_invoices | invoices.xlsx | Raw invoice data |
# MAGIC | bronze_payments | payments.xlsx | Raw payment data |
# MAGIC | bronze_quarantine | N/A | Invalid records quarantine |
# MAGIC
# MAGIC ### Silver Tables (15)
# MAGIC | Table | Type | Key |
# MAGIC | --- | --- | --- |
# MAGIC | dim_company | Dimension | company_id |
# MAGIC | dim_contact | Dimension | contact_id |
# MAGIC | dim_candidate | Dimension | candidate_id |
# MAGIC | dim_job | Dimension | job_id |
# MAGIC | dim_agreement | Dimension | agreement_id |
# MAGIC | fact_submission | Fact | submission_id |
# MAGIC | fact_interview | Fact | interview_id |
# MAGIC | fact_offer | Fact | offer_id |
# MAGIC | fact_placement | Fact | placement_id |
# MAGIC | fact_invoice | Fact | invoice_id |
# MAGIC | fact_payment | Fact | payment_id |
# MAGIC | control_file_ingestion | Control | ingestion_id |
# MAGIC | control_job_run | Control | run_id |
# MAGIC | audit_data_changes | Audit | audit_id |
# MAGIC | config_business_rules | Config | rule_name |
# MAGIC
# MAGIC ### Gold Views & Tables (9)
# MAGIC | View/Table | Purpose |
# MAGIC | --- | --- |
# MAGIC | vw_open_jobs | All open jobs with days_open |
# MAGIC | vw_candidate_pipeline | Full pipeline: submission -> interview -> offer -> placement |
# MAGIC | vw_client_pipeline | Client overview: jobs, submissions, placements, revenue |
# MAGIC | vw_recruitment_funnel | Funnel: sourced -> submitted -> interviewed -> offered -> placed |
# MAGIC | vw_revenue | Revenue: placement fees, invoices, payments, outstanding |
# MAGIC | vw_candidate_matching | Candidate/job match scores with recommendations |
# MAGIC | vw_management_dashboard | KPI summary for management |
# MAGIC | gold_candidate_matching | Materialized matching results (table) |
# MAGIC | gold_data_quality_summary | DQ check results |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Primary/Foreign Key Relationships
# MAGIC
# MAGIC | From Table | Column | To Table | Column |
# MAGIC | --- | --- | --- | --- |
# MAGIC | dim_contact | company_id | dim_company | company_id |
# MAGIC | dim_job | company_id | dim_company | company_id |
# MAGIC | dim_agreement | company_id | dim_company | company_id |
# MAGIC | fact_submission | job_id | dim_job | job_id |
# MAGIC | fact_submission | candidate_id | dim_candidate | candidate_id |
# MAGIC | fact_interview | submission_id | fact_submission | submission_id |
# MAGIC | fact_offer | submission_id | fact_submission | submission_id |
# MAGIC | fact_placement | job_id | dim_job | job_id |
# MAGIC | fact_placement | candidate_id | dim_candidate | candidate_id |
# MAGIC | fact_placement | offer_id | fact_offer | offer_id |
# MAGIC | fact_invoice | placement_id | fact_placement | placement_id |
# MAGIC | fact_payment | invoice_id | fact_invoice | invoice_id |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Ingestion Process
# MAGIC 1. Source files (Excel) maintained on local laptop
# MAGIC 2. Local Python sync script (watchdog) detects file changes
# MAGIC 3. Changed files uploaded to Databricks Volume
# MAGIC 4. Orchestration notebook detects changed files via hash comparison
# MAGIC 5. Only changed sources are reprocessed (selective)
# MAGIC 6. Bronze -> Silver via MERGE with type casting
# MAGIC 7. DQ checks run automatically
# MAGIC 8. Gold views auto-update (views)
# MAGIC 9. Candidate matching reruns if jobs/candidates changed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Rules
# MAGIC 30 checks across all Silver tables:
# MAGIC - Primary key NOT NULL and UNIQUE
# MAGIC - Foreign key existence
# MAGIC - Email format validation (RLIKE)
# MAGIC - CTC/experience/invoice/payment NOT negative
# MAGIC - Joining date >= offer date
# MAGIC - All results in gold_data_quality_summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Candidate Matching Algorithm
# MAGIC
# MAGIC Weighted deterministic score (0-100):
# MAGIC | Factor | Weight |
# MAGIC | --- | --- |
# MAGIC | Mandatory skills match | 40% |
# MAGIC | Preferred skills match | 15% |
# MAGIC | Experience range match | 20% |
# MAGIC | Location match | 10% |
# MAGIC | CTC budget match | 10% |
# MAGIC | Notice period match | 5% |
# MAGIC
# MAGIC Recommendation thresholds (configurable):
# MAGIC | Score | Recommendation |
# MAGIC | --- | --- |
# MAGIC | >= 85 | Strong Match |
# MAGIC | >= 70 | Good Match |
# MAGIC | >= 50 | Review |
# MAGIC | < 50 | Weak Match |
# MAGIC
# MAGIC All weights and thresholds stored in config_business_rules table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Modify Candidate Matching Weights
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE recruitment.silver.config_business_rules
# MAGIC SET rule_value = '0.35', updated_ts = current_timestamp()
# MAGIC WHERE rule_name = 'match_weight_mandatory_skills';
# MAGIC ```
# MAGIC Then rerun the candidate matching engine.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Add a New Source File
# MAGIC 1. Create a bronze table with ingestion metadata columns
# MAGIC 2. Create the corresponding silver table
# MAGIC 3. Add the file to SOURCE_MAP in the orchestration notebook
# MAGIC 4. Write the Bronze->Silver MERGE statement
# MAGIC 5. Add DQ checks for the new table
# MAGIC 6. Add any required Gold views
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agent Architecture
# MAGIC All agents are read-only by default. They recommend actions but require human approval before executing.
# MAGIC
# MAGIC | Agent | Responsibilities |
# MAGIC | --- | --- |
# MAGIC | Recruitment Agent | Job requirements, candidate search, ranking, shortlist |
# MAGIC | Client Acquisition Agent | Inactive clients, follow-up priorities, client history |
# MAGIC | Operations Agent | Interview pipeline, feedback tracking, bottleneck identification |
# MAGIC | Finance Agent | Fee calculation, invoice tracking, overdue alerts, revenue forecasting |
# MAGIC | Management Agent | Business summary, funnel, KPIs, bottleneck recommendations |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Features Requiring External Services
# MAGIC These CANNOT be done purely within Databricks:
# MAGIC - LinkedIn messaging/sourcing
# MAGIC - Email automation (sending emails)
# MAGIC - WhatsApp messaging
# MAGIC - Calendar event creation/sync
# MAGIC - E-signature for agreements
# MAGIC - Payment processing
# MAGIC - Resume parsing from PDFs (requires external OCR or AI service)
# MAGIC - Direct laptop filesystem monitoring (requires local Python script)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Genie Space
# MAGIC A Genie space is configured over the Gold layer for natural-language Q&A.
# MAGIC Business terminology definitions:
# MAGIC - 'placement' = candidate who has joined the client company
# MAGIC - 'open job' = job_status = 'OPEN'
# MAGIC - 'active candidate' = candidate_status indicates availability
# MAGIC - 'revenue' = recruitment fee associated with confirmed placements
# MAGIC - 'outstanding invoice' = total invoice amount minus valid payments
# MAGIC - 'overdue invoice' = due date before current date AND outstanding > 0
# MAGIC - 'candidate match score' = internal matching score, not a hiring decision

# COMMAND ----------

