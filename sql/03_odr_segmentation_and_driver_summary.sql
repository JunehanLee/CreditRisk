-- 03_odr_segmentation_and_driver_summary.sql
-- Step: ODR-based segmentation analysis and driver ranking
-- Purpose:
--   1) Create TABLE_DATA.ODR_BY_COLUMN from the curated loan table
--   2) Create TABLE_DATA.ODR_SUMMARY with driver-level ranking metrics
-- Notes:
--   - Analysis uses closed loans only (y_default is not null)
--   - Numeric variables are bucketed into deciles
--   - Sparse count variables use simple grouped buckets for interpretability

USE WAREHOUSE COMPUTE_WH;
USE DATABASE CREDITRISK;

CREATE SCHEMA IF NOT EXISTS TABLE_DATA;
USE SCHEMA TABLE_DATA;

CREATE OR REPLACE TABLE TABLE_DATA.ODR_BY_COLUMN AS
WITH base AS (
  SELECT *
  FROM CURATED_DATA.LOAN_CLEAN
  WHERE y_default IS NOT NULL
),

/* Continuous variables (deciles) */
d_annual_inc AS (
  SELECT NTILE(10) OVER (ORDER BY annual_inc) AS decile, y_default
  FROM base
  WHERE annual_inc IS NOT NULL
),
d_dti AS (
  SELECT NTILE(10) OVER (ORDER BY dti) AS decile, y_default
  FROM base
  WHERE dti IS NOT NULL
),
d_revol_util AS (
  SELECT NTILE(10) OVER (ORDER BY revol_util_pct) AS decile, y_default
  FROM base
  WHERE revol_util_pct IS NOT NULL
),
d_bc_util AS (
  SELECT NTILE(10) OVER (ORDER BY bc_util_pct) AS decile, y_default
  FROM base
  WHERE bc_util_pct IS NOT NULL
),
d_loan_amnt AS (
  SELECT NTILE(10) OVER (ORDER BY loan_amnt) AS decile, y_default
  FROM base
  WHERE loan_amnt IS NOT NULL
),
d_funded_amnt AS (
  SELECT NTILE(10) OVER (ORDER BY funded_amnt) AS decile, y_default
  FROM base
  WHERE funded_amnt IS NOT NULL
),
d_installment AS (
  SELECT NTILE(10) OVER (ORDER BY installment) AS decile, y_default
  FROM base
  WHERE installment IS NOT NULL
),
d_revol_bal AS (
  SELECT NTILE(10) OVER (ORDER BY revol_bal) AS decile, y_default
  FROM base
  WHERE revol_bal IS NOT NULL
),
d_total_rev_hi_lim AS (
  SELECT NTILE(10) OVER (ORDER BY total_rev_hi_lim) AS decile, y_default
  FROM base
  WHERE total_rev_hi_lim IS NOT NULL
),
d_tot_cur_bal AS (
  SELECT NTILE(10) OVER (ORDER BY tot_cur_bal) AS decile, y_default
  FROM base
  WHERE tot_cur_bal IS NOT NULL
),
d_tot_hi_cred_lim AS (
  SELECT NTILE(10) OVER (ORDER BY tot_hi_cred_lim) AS decile, y_default
  FROM base
  WHERE tot_hi_cred_lim IS NOT NULL
),
d_total_bal_ex_mort AS (
  SELECT NTILE(10) OVER (ORDER BY total_bal_ex_mort) AS decile, y_default
  FROM base
  WHERE total_bal_ex_mort IS NOT NULL
),
d_total_bc_limit AS (
  SELECT NTILE(10) OVER (ORDER BY total_bc_limit) AS decile, y_default
  FROM base
  WHERE total_bc_limit IS NOT NULL
),
d_credit_hist_months AS (
  SELECT NTILE(10) OVER (ORDER BY credit_history_months) AS decile, y_default
  FROM base
  WHERE credit_history_months IS NOT NULL
),
d_open_acc AS (
  SELECT NTILE(10) OVER (ORDER BY open_acc) AS decile, y_default
  FROM base
  WHERE open_acc IS NOT NULL
),
d_total_acc AS (
  SELECT NTILE(10) OVER (ORDER BY total_acc) AS decile, y_default
  FROM base
  WHERE total_acc IS NOT NULL
),
d_num_rev_accts AS (
  SELECT NTILE(10) OVER (ORDER BY num_rev_accts) AS decile, y_default
  FROM base
  WHERE num_rev_accts IS NOT NULL
),
d_pct_tl_nvr_dlq AS (
  SELECT NTILE(10) OVER (ORDER BY pct_tl_nvr_dlq) AS decile, y_default
  FROM base
  WHERE pct_tl_nvr_dlq IS NOT NULL
)

SELECT
  'annual_inc_decile' AS col_name,
  'D' || decile AS bucket,
  COUNT(*) AS n_closed,
  SUM(y_default) AS n_default,
  AVG(y_default) AS odr
FROM d_annual_inc
GROUP BY 1, 2

UNION ALL
SELECT 'dti_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_dti
GROUP BY 1, 2

UNION ALL
SELECT 'revol_util_pct_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_revol_util
GROUP BY 1, 2

UNION ALL
SELECT 'bc_util_pct_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_bc_util
GROUP BY 1, 2

UNION ALL
SELECT 'loan_amnt_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_loan_amnt
GROUP BY 1, 2

UNION ALL
SELECT 'funded_amnt_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_funded_amnt
GROUP BY 1, 2

UNION ALL
SELECT 'installment_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_installment
GROUP BY 1, 2

UNION ALL
SELECT 'revol_bal_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_revol_bal
GROUP BY 1, 2

UNION ALL
SELECT 'total_rev_hi_lim_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_total_rev_hi_lim
GROUP BY 1, 2

UNION ALL
SELECT 'tot_cur_bal_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_tot_cur_bal
GROUP BY 1, 2

UNION ALL
SELECT 'tot_hi_cred_lim_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_tot_hi_cred_lim
GROUP BY 1, 2

UNION ALL
SELECT 'total_bal_ex_mort_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_total_bal_ex_mort
GROUP BY 1, 2

UNION ALL
SELECT 'total_bc_limit_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_total_bc_limit
GROUP BY 1, 2

UNION ALL
SELECT 'credit_history_months_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_credit_hist_months
GROUP BY 1, 2

UNION ALL
SELECT 'open_acc_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_open_acc
GROUP BY 1, 2

UNION ALL
SELECT 'total_acc_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_total_acc
GROUP BY 1, 2

UNION ALL
SELECT 'num_rev_accts_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_num_rev_accts
GROUP BY 1, 2

UNION ALL
SELECT 'pct_tl_nvr_dlq_decile', 'D' || decile, COUNT(*), SUM(y_default), AVG(y_default)
FROM d_pct_tl_nvr_dlq
GROUP BY 1, 2

/* Sparse grouped variables: 0 / 1 / 2+ */
UNION ALL
SELECT
  'delinq_2yrs',
  CASE
    WHEN delinq_2yrs IS NULL THEN '__NULL__'
    WHEN delinq_2yrs = 0 THEN '0'
    WHEN delinq_2yrs = 1 THEN '1'
    ELSE '2+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT
  'mort_acc',
  CASE
    WHEN mort_acc IS NULL THEN '__NULL__'
    WHEN mort_acc = 0 THEN '0'
    WHEN mort_acc = 1 THEN '1'
    ELSE '2+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT
  'inq_last_6mths',
  CASE
    WHEN inq_last_6mths IS NULL THEN '__NULL__'
    WHEN inq_last_6mths = 0 THEN '0'
    WHEN inq_last_6mths = 1 THEN '1'
    ELSE '2+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

/* Sparse grouped variables: 0 / 1+ */
UNION ALL
SELECT
  'num_accts_ever_120_pd',
  CASE
    WHEN num_accts_ever_120_pd IS NULL THEN '__NULL__'
    WHEN num_accts_ever_120_pd = 0 THEN '0'
    ELSE '1+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT
  'num_tl_90g_dpd_24m',
  CASE
    WHEN num_tl_90g_dpd_24m IS NULL THEN '__NULL__'
    WHEN num_tl_90g_dpd_24m = 0 THEN '0'
    ELSE '1+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT
  'pub_rec',
  CASE
    WHEN pub_rec IS NULL THEN '__NULL__'
    WHEN pub_rec = 0 THEN '0'
    ELSE '1+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT
  'pub_rec_bankruptcies',
  CASE
    WHEN pub_rec_bankruptcies IS NULL THEN '__NULL__'
    WHEN pub_rec_bankruptcies = 0 THEN '0'
    ELSE '1+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

/* Applicant info grouped variable */
UNION ALL
SELECT
  'emp_length_years',
  CASE
    WHEN emp_length_years IS NULL THEN '__NULL__'
    WHEN emp_length_years = 0 THEN '0'
    WHEN emp_length_years BETWEEN 1 AND 2 THEN '1-2'
    WHEN emp_length_years BETWEEN 3 AND 5 THEN '3-5'
    WHEN emp_length_years BETWEEN 6 AND 9 THEN '6-9'
    ELSE '10+'
  END,
  COUNT(*),
  SUM(y_default),
  AVG(y_default)
FROM base
GROUP BY 1, 2

/* Categorical variables */
UNION ALL
SELECT 'term_months', COALESCE(TO_VARCHAR(term_months), '__NULL__'), COUNT(*), SUM(y_default), AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT 'purpose', COALESCE(TO_VARCHAR(purpose), '__NULL__'), COUNT(*), SUM(y_default), AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT 'application_type', COALESCE(TO_VARCHAR(application_type), '__NULL__'), COUNT(*), SUM(y_default), AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT 'home_ownership', COALESCE(TO_VARCHAR(home_ownership), '__NULL__'), COUNT(*), SUM(y_default), AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT 'verification_status', COALESCE(TO_VARCHAR(verification_status), '__NULL__'), COUNT(*), SUM(y_default), AVG(y_default)
FROM base
GROUP BY 1, 2

UNION ALL
SELECT 'addr_state', COALESCE(TO_VARCHAR(addr_state), '__NULL__'), COUNT(*), SUM(y_default), AVG(y_default)
FROM base
GROUP BY 1, 2
;

CREATE OR REPLACE TABLE TABLE_DATA.ODR_SUMMARY AS
WITH base AS (
  SELECT *
  FROM TABLE_DATA.ODR_BY_COLUMN
  WHERE bucket <> '__NULL__'
),
agg_all AS (
  SELECT
    col_name,
    COUNT(*) AS bucket_count,
    SUM(n_closed) AS total_n_closed,
    MIN(n_closed) AS min_bucket_n,
    MAX(n_closed) AS max_bucket_n,
    MIN(odr) AS min_odr,
    MAX(odr) AS max_odr,
    MAX(odr) - MIN(odr) AS odr_range,
    (MAX(odr) - MIN(odr)) / NULLIF(COUNT(*), 0) AS range_per_bucket,
    (MAX(n_closed) / NULLIF(SUM(n_closed), 0))::FLOAT AS top_bucket_share,
    STDDEV_POP(odr) AS odr_std_unweighted
  FROM base
  GROUP BY 1
),
agg_filt AS (
  SELECT
    col_name,
    COUNT(*) AS bucket_count_filt,
    MIN(odr) AS min_odr_filt,
    MAX(odr) AS max_odr_filt,
    MAX(odr) - MIN(odr) AS odr_range_filt
  FROM base
  WHERE n_closed >= 200
  GROUP BY 1
)
SELECT
  CASE
    WHEN a.col_name IN (
      'annual_inc_decile',
      'home_ownership',
      'verification_status',
      'purpose',
      'application_type',
      'addr_state',
      'emp_length_years'
    ) THEN 'Applicant Info'

    WHEN a.col_name IN (
      'loan_amnt_decile',
      'funded_amnt_decile',
      'installment_decile',
      'term_months'
    ) THEN 'Loan Characteristics'

    WHEN a.col_name IN (
      'dti_decile',
      'revol_util_pct_decile',
      'bc_util_pct_decile',
      'revol_bal_decile',
      'total_rev_hi_lim_decile',
      'tot_cur_bal_decile',
      'tot_hi_cred_lim_decile',
      'total_bal_ex_mort_decile',
      'total_bc_limit_decile',
      'credit_history_months_decile',
      'delinq_2yrs',
      'mort_acc',
      'inq_last_6mths',
      'num_accts_ever_120_pd',
      'num_tl_90g_dpd_24m',
      'pub_rec',
      'pub_rec_bankruptcies',
      'open_acc_decile',
      'total_acc_decile',
      'num_rev_accts_decile',
      'pct_tl_nvr_dlq_decile'
    ) THEN 'Credit History'

    ELSE 'Other'
  END AS feature_category,
  a.*,
  f.bucket_count_filt,
  f.min_odr_filt,
  f.max_odr_filt,
  f.odr_range_filt,
  f.odr_range_filt / NULLIF(f.bucket_count_filt, 0) AS range_per_bucket_filt
FROM agg_all a
LEFT JOIN agg_filt f
  USING (col_name)
ORDER BY odr_range_filt DESC NULLS LAST, odr_range DESC;
