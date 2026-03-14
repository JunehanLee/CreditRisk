-- 02_create_curated_loan_clean.sql
-- Purpose: create an analysis-ready curated loan table for EDA and downstream PD modelling
-- Design choice: keep decision-time variables, remove post-outcome leakage, and define a closed-loan default label

USE WAREHOUSE COMPUTE_WH;
USE DATABASE CREDITRISK;

CREATE SCHEMA IF NOT EXISTS CURATED_DATA;
USE SCHEMA CURATED_DATA;

CREATE OR REPLACE TABLE LOAN_CLEAN AS
WITH src AS (
  SELECT
    /* Applicant Info */
    addr_state,
    TRY_TO_NUMBER(annual_inc) AS annual_inc,
    CASE
      WHEN emp_length IS NULL THEN NULL
      WHEN LOWER(emp_length) LIKE '%<%1%' THEN 0
      WHEN LOWER(emp_length) LIKE '%10%' THEN 10
      ELSE TRY_TO_NUMBER(REGEXP_SUBSTR(emp_length, '\\d+'))
    END AS emp_length_years,
    home_ownership,
    verification_status,
    purpose,
    application_type,

    /* Loan Characteristics */
    TRY_TO_NUMBER(loan_amnt) AS loan_amnt,
    TRY_TO_NUMBER(funded_amnt) AS funded_amnt,
    TRY_TO_NUMBER(installment) AS installment,
    term,
    TRY_TO_NUMBER(REGEXP_SUBSTR(term, '\\d+')) AS term_months,
    DATE_TRUNC(
      'month',
      COALESCE(
        TRY_TO_DATE(issue_d, 'MON-YYYY'),
        TRY_TO_DATE(issue_d)
      )
    ) AS issue_month,

    /* Credit History */
    TRY_TO_NUMBER(dti) AS dti,
    TRY_TO_NUMBER(delinq_2yrs) AS delinq_2yrs,
    TRY_TO_NUMBER(num_accts_ever_120_pd) AS num_accts_ever_120_pd,
    TRY_TO_NUMBER(num_tl_90g_dpd_24m) AS num_tl_90g_dpd_24m,
    TRY_TO_NUMBER(pct_tl_nvr_dlq) AS pct_tl_nvr_dlq,
    TRY_TO_NUMBER(pub_rec) AS pub_rec,
    TRY_TO_NUMBER(pub_rec_bankruptcies) AS pub_rec_bankruptcies,
    TRY_TO_NUMBER(open_acc) AS open_acc,
    TRY_TO_NUMBER(total_acc) AS total_acc,
    TRY_TO_NUMBER(mort_acc) AS mort_acc,
    TRY_TO_NUMBER(num_rev_accts) AS num_rev_accts,
    TRY_TO_NUMBER(inq_last_6mths) AS inq_last_6mths,
    TRY_TO_NUMBER(revol_bal) AS revol_bal,
    TRY_TO_NUMBER(revol_util) AS revol_util_pct,
    TRY_TO_NUMBER(bc_util) AS bc_util_pct,
    TRY_TO_NUMBER(percent_bc_gt_75) AS percent_bc_gt_75,
    TRY_TO_NUMBER(num_rev_tl_bal_gt_0) AS num_rev_tl_bal_gt_0,
    TRY_TO_NUMBER(total_rev_hi_lim) AS total_rev_hi_lim,
    TRY_TO_NUMBER(tot_cur_bal) AS tot_cur_bal,
    TRY_TO_NUMBER(tot_hi_cred_lim) AS tot_hi_cred_lim,
    TRY_TO_NUMBER(total_bal_ex_mort) AS total_bal_ex_mort,
    TRY_TO_NUMBER(total_bc_limit) AS total_bc_limit,
    COALESCE(
      TRY_TO_DATE(earliest_cr_line, 'MON-YYYY'),
      TRY_TO_DATE(earliest_cr_line)
    ) AS earliest_cr_line_date,

    /* Label */
    CASE
      WHEN loan_status ILIKE 'Fully Paid%' THEN 'FULLY_PAID'
      WHEN loan_status ILIKE 'Charged Off%' THEN 'CHARGED_OFF'
      WHEN loan_status ILIKE 'Current%' THEN 'CURRENT'
      ELSE 'OTHER'
    END AS loan_status_norm,
    CASE
      WHEN loan_status ILIKE 'Charged Off%' THEN 1
      WHEN loan_status ILIKE 'Fully Paid%' THEN 0
      ELSE NULL
    END AS y_default

  FROM RAW_DATA.LOAN_DATA_RAW
),
final AS (
  SELECT
    *,
    CASE
      WHEN issue_month IS NOT NULL AND earliest_cr_line_date IS NOT NULL
      THEN DATEDIFF('month', earliest_cr_line_date, issue_month)
      ELSE NULL
    END AS credit_history_months
  FROM src
)
SELECT *
FROM final;
