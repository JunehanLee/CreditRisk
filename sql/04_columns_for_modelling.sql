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
    emp_length_years,
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
    mort_acc,
    inq_last_6mths,
    num_accts_ever_120_pd,

    /* =========================
       Benchmark (NOT for training)
    ========================= */
    grade,
    sub_grade

FROM CURATED_DATA.LOAN_CLEAN

/* =========================
   Closed Loans Only
========================= */
WHERE y_default IS NOT NULL
;
