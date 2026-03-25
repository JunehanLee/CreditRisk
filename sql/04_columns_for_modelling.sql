CREATE OR REPLACE TABLE CURATED_DATA.MODELLING_DATA AS
SELECT
    /* =========================
       Target & Tracking
    ========================= */
    issue_month,
    y_default,

    /* =========================
       Applicant Info
    ========================= */
    annual_inc,
    CASE
        WHEN emp_length_years IS NULL THEN '__NULL__'
        WHEN emp_length_years = 0 THEN '0'
        WHEN emp_length_years BETWEEN 1 AND 2 THEN '1-2'
        WHEN emp_length_years BETWEEN 3 AND 5 THEN '3-5'
        WHEN emp_length_years BETWEEN 6 AND 9 THEN '6-9'
        ELSE '10+'
    END AS emp_length_years,

    home_ownership,
    verification_status,
    purpose,

    /* =========================
       Loan Characteristics
    ========================= */
    loan_amnt,
    installment,
    term_months,

    /* =========================
       Credit / Bureau Features
    ========================= */
    dti,
    total_bc_limit,
    bc_util_pct,
    revol_util_pct,

    CASE
        WHEN mort_acc IS NULL THEN '__NULL__'
        WHEN mort_acc = 0 THEN '0'
        WHEN mort_acc = 1 THEN '1'
        ELSE '2+'
    END AS mort_acc,

    CASE
        WHEN inq_last_6mths IS NULL THEN '__NULL__'
        WHEN inq_last_6mths = 0 THEN '0'
        WHEN inq_last_6mths = 1 THEN '1'
        ELSE '2+'
    END AS inq_last_6mths,

    CASE
        WHEN num_accts_ever_120_pd IS NULL THEN '__NULL__'
        WHEN num_accts_ever_120_pd = 0 THEN '0'
        WHEN num_accts_ever_120_pd = 1 THEN '1'
        ELSE '2+'
    END AS num_accts_ever_120_pd,

    /* =========================
       Benchmark (NOT for training)
    ========================= */
    grade,
    sub_grade

FROM CURATED_DATA.LOAN_CLEAN

/* =========================
   Closed Loans Only
========================= */
WHERE y_default IS NOT NULL;
