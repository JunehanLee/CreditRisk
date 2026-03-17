-- 03_odr_segmentation_and_summary.sql
-- Purpose:
--   1) Create TABLE_DATA.ODR_BY_COLUMN from CURATED_DATA.LOAN_CLEAN
--   2) Create TABLE_DATA.ODR_SUMMARY with driver-level ranking metrics
-- Notes:
--   - Analysis uses closed loans only (y_default is not null)
--   - Continuous variables are grouped into deciles
--   - Sparse count variables use interpretable grouped buckets
--   - Ranking is primarily based on odr_range_filt (n_closed >= 200)

USE WAREHOUSE COMPUTE_WH;
USE DATABASE CREDITRISK;

CREATE SCHEMA IF NOT EXISTS TABLE_DATA;
USE SCHEMA TABLE_DATA;

-- =========================================================
-- 1) ODR by variable/bucket
-- =========================================================
CREATE OR REPLACE TABLE TABLE_DATA.ODR_BY_COLUMN AS
WITH base AS (
    SELECT *
    FROM CURATED_DATA.LOAN_CLEAN
    WHERE y_default IS NOT NULL
),

/* -----------------------------------------
   Continuous variables -> deciles
----------------------------------------- */
unpivoted_deciles AS (
    SELECT
        col_name,
        val,
        y_default
    FROM (
        SELECT
            y_default,
            CAST(annual_inc AS FLOAT) AS annual_inc,
            CAST(dti AS FLOAT) AS dti,
            CAST(revol_util_pct AS FLOAT) AS revol_util_pct,
            CAST(bc_util_pct AS FLOAT) AS bc_util_pct,
            CAST(loan_amnt AS FLOAT) AS loan_amnt,
            CAST(funded_amnt AS FLOAT) AS funded_amnt,
            CAST(installment AS FLOAT) AS installment,
            CAST(revol_bal AS FLOAT) AS revol_bal,
            CAST(total_rev_hi_lim AS FLOAT) AS total_rev_hi_lim,
            CAST(tot_cur_bal AS FLOAT) AS tot_cur_bal,
            CAST(tot_hi_cred_lim AS FLOAT) AS tot_hi_cred_lim,
            CAST(total_bal_ex_mort AS FLOAT) AS total_bal_ex_mort,
            CAST(total_bc_limit AS FLOAT) AS total_bc_limit,
            CAST(credit_history_months AS FLOAT) AS credit_history_months,
            CAST(open_acc AS FLOAT) AS open_acc,
            CAST(total_acc AS FLOAT) AS total_acc,
            CAST(num_rev_accts AS FLOAT) AS num_rev_accts,
            CAST(pct_tl_nvr_dlq AS FLOAT) AS pct_tl_nvr_dlq
        FROM base
    )
    UNPIVOT (
        val FOR col_name IN (
            annual_inc,
            dti,
            revol_util_pct,
            bc_util_pct,
            loan_amnt,
            funded_amnt,
            installment,
            revol_bal,
            total_rev_hi_lim,
            tot_cur_bal,
            tot_hi_cred_lim,
            total_bal_ex_mort,
            total_bc_limit,
            credit_history_months,
            open_acc,
            total_acc,
            num_rev_accts,
            pct_tl_nvr_dlq
        )
    )
),
decile_buckets AS (
    SELECT
        col_name || '_decile' AS col_name,
        'D' || NTILE(10) OVER (PARTITION BY col_name ORDER BY val) AS bucket,
        y_default
    FROM unpivoted_deciles
    WHERE val IS NOT NULL
),

decile_odr AS (
    SELECT
        col_name,
        bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM decile_buckets
    GROUP BY 1, 2
),

/* -----------------------------------------
   Sparse / grouped / categorical variables
----------------------------------------- */
other_odr AS (

    /* sparse: 0 / 1 / 2+ */
    SELECT
        'delinq_2yrs' AS col_name,
        CASE
            WHEN delinq_2yrs IS NULL THEN '__NULL__'
            WHEN delinq_2yrs = 0 THEN '0'
            WHEN delinq_2yrs = 1 THEN '1'
            ELSE '2+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'mort_acc' AS col_name,
        CASE
            WHEN mort_acc IS NULL THEN '__NULL__'
            WHEN mort_acc = 0 THEN '0'
            WHEN mort_acc = 1 THEN '1'
            ELSE '2+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'inq_last_6mths' AS col_name,
        CASE
            WHEN inq_last_6mths IS NULL THEN '__NULL__'
            WHEN inq_last_6mths = 0 THEN '0'
            WHEN inq_last_6mths = 1 THEN '1'
            ELSE '2+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    /* sparse: 0 / 1+ */
    UNION ALL

    SELECT
        'num_accts_ever_120_pd' AS col_name,
        CASE
            WHEN num_accts_ever_120_pd IS NULL THEN '__NULL__'
            WHEN num_accts_ever_120_pd = 0 THEN '0'
            ELSE '1+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'num_tl_90g_dpd_24m' AS col_name,
        CASE
            WHEN num_tl_90g_dpd_24m IS NULL THEN '__NULL__'
            WHEN num_tl_90g_dpd_24m = 0 THEN '0'
            ELSE '1+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'pub_rec' AS col_name,
        CASE
            WHEN pub_rec IS NULL THEN '__NULL__'
            WHEN pub_rec = 0 THEN '0'
            ELSE '1+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'pub_rec_bankruptcies' AS col_name,
        CASE
            WHEN pub_rec_bankruptcies IS NULL THEN '__NULL__'
            WHEN pub_rec_bankruptcies = 0 THEN '0'
            ELSE '1+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    /* grouped employment length */
    UNION ALL

    SELECT
        'emp_length_years' AS col_name,
        CASE
            WHEN emp_length_years IS NULL THEN '__NULL__'
            WHEN emp_length_years = 0 THEN '0'
            WHEN emp_length_years BETWEEN 1 AND 2 THEN '1-2'
            WHEN emp_length_years BETWEEN 3 AND 5 THEN '3-5'
            WHEN emp_length_years BETWEEN 6 AND 9 THEN '6-9'
            ELSE '10+'
        END AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    /* categoricals */
    UNION ALL

    SELECT
        'term_months' AS col_name,
        COALESCE(TO_VARCHAR(term_months), '__NULL__') AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'purpose' AS col_name,
        COALESCE(purpose, '__NULL__') AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'application_type' AS col_name,
        COALESCE(application_type, '__NULL__') AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'home_ownership' AS col_name,
        COALESCE(home_ownership, '__NULL__') AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'verification_status' AS col_name,
        COALESCE(verification_status, '__NULL__') AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'addr_state' AS col_name,
        COALESCE(addr_state, '__NULL__') AS bucket,
        COUNT(*) AS n_closed,
        SUM(y_default) AS n_default,
        AVG(y_default) AS odr
    FROM base
    GROUP BY 1, 2
)

SELECT * FROM decile_odr WHERE n_closed >= 200
UNION ALL
SELECT * FROM other_odr WHERE n_closed >= 200
;

-- =========================================================
-- 2) Driver-level summary / ranking
-- =========================================================
CREATE OR REPLACE TABLE TABLE_DATA.ODR_SUMMARY AS
WITH base AS (
    SELECT *
    FROM TABLE_DATA.ODR_BY_COLUMN
    WHERE bucket <> '__NULL__'
),

feature_map AS (
    SELECT 'ANNUAL_INC_decile' AS col_name, 'Applicant Info' AS feature_category UNION ALL
    SELECT 'home_ownership', 'Applicant Info' UNION ALL
    SELECT 'verification_status', 'Applicant Info' UNION ALL
    SELECT 'purpose', 'Applicant Info' UNION ALL
    SELECT 'application_type', 'Applicant Info' UNION ALL
    SELECT 'addr_state', 'Applicant Info' UNION ALL
    SELECT 'emp_length_years', 'Applicant Info' UNION ALL

    SELECT 'LOAN_AMNT_decile', 'Loan Characteristics' UNION ALL
    SELECT 'FUNDED_AMNT_decile', 'Loan Characteristics' UNION ALL
    SELECT 'INSTALLMENT_decile', 'Loan Characteristics' UNION ALL
    SELECT 'term_months', 'Loan Characteristics' UNION ALL

    SELECT 'DTI_decile', 'Credit History' UNION ALL
    SELECT 'REVOL_UTIL_PCT_decile', 'Credit History' UNION ALL
    SELECT 'BC_UTIL_PCT_decile', 'Credit History' UNION ALL
    SELECT 'REVOL_BAL_decile', 'Credit History' UNION ALL
    SELECT 'TOTAL_REV_HI_LIM_decile', 'Credit History' UNION ALL
    SELECT 'TOT_CUR_BAL_decile', 'Credit History' UNION ALL
    SELECT 'TOT_HI_CRED_LIM_decile', 'Credit History' UNION ALL
    SELECT 'TOTAL_BAL_EX_MORT_decile', 'Credit History' UNION ALL
    SELECT 'TOTAL_BC_LIMIT_decile', 'Credit History' UNION ALL
    SELECT 'CREDIT_HISTORY_MONTHS_decile', 'Credit History' UNION ALL
    SELECT 'delinq_2yrs', 'Credit History' UNION ALL
    SELECT 'mort_acc', 'Credit History' UNION ALL
    SELECT 'inq_last_6mths', 'Credit History' UNION ALL
    SELECT 'num_accts_ever_120_pd', 'Credit History' UNION ALL
    SELECT 'num_tl_90g_dpd_24m', 'Credit History' UNION ALL
    SELECT 'pub_rec', 'Credit History' UNION ALL
    SELECT 'pub_rec_bankruptcies', 'Credit History' UNION ALL
    SELECT 'OPEN_ACC_decile', 'Credit History' UNION ALL
    SELECT 'TOTAL_ACC_decile', 'Credit History' UNION ALL
    SELECT 'NUM_REV_ACCTS_decile', 'Credit History' UNION ALL
    SELECT 'PCT_TL_NVR_DLQ_decile', 'Credit History'
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
    COALESCE(m.feature_category, 'Other') AS feature_category,
    a.col_name,
    a.bucket_count,
    a.total_n_closed,
    a.min_bucket_n,
    a.max_bucket_n,
    a.min_odr,
    a.max_odr,
    a.odr_range,
    a.range_per_bucket,
    a.top_bucket_share,
    a.odr_std_unweighted,
    f.bucket_count_filt,
    f.min_odr_filt,
    f.max_odr_filt,
    f.odr_range_filt,
    f.odr_range_filt / NULLIF(f.bucket_count_filt, 0) AS range_per_bucket_filt
FROM agg_all a
LEFT JOIN agg_filt f
    USING (col_name)
LEFT JOIN feature_map m
    USING (col_name)
ORDER BY odr_range_filt DESC NULLS LAST, odr_range DESC
;
