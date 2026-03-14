# LendingClub Credit Risk Segmentation & Policy Analysis

This project analyses borrower risk segmentation using LendingClub loan data.

Instead of focusing purely on predictive performance (e.g. AUC), the objective is to connect **probability of default (PD)** to underwriting decisions such as:

- approval rate  
- expected default rate (EDR)  
- expected loss proxy  

The project is structured as a two-phase workflow.

**Phase 1 — Risk segmentation (EDA)**  
Identify borrower attributes that meaningfully separate default risk.

**Phase 2 — Policy modelling (planned)**  
Evaluate approval strategies using predicted PD.

This repository currently documents **Phase 1: Risk Segmentation Analysis**.

---

# Dataset

Raw data stored in Snowflake

`RAW_DATA.LOAN_DATA_RAW`

Clean dataset used for analysis

`CURATED_DATA.LOAN_CLEAN`

Target definition

`default = 1 → Charged Off`  
`default = 0 → Fully Paid`

Other loan statuses were excluded to avoid ambiguous outcomes.  
The analysis therefore uses **closed loans only**.

---

# Feature Selection Framework

To ensure realistic underwriting analysis, candidate variables were selected using the following rules.

Each feature must satisfy:

1. **Decision-time availability**  
   Observable at the loan approval stage.

2. **Policy relevance**  
   Contributes to underwriting decisions or risk interpretation.

3. **Low leakage risk**  
   Excludes post-loan variables related to repayment outcomes.

4. **Coverage**  
   Acceptable missingness or meaningful missing values.

5. **Stability & interpretability**  
   Clear definition and interpretable relationship with risk.

---

# Risk Segmentation Method

Borrowers were segmented across multiple variables and the **Observed Default Rate (ODR)** was calculated.

`ODR = number of defaults / number of closed loans`

Segmentation rules:

**Numeric variables**

Grouped into deciles using:

`NTILE(10)`

Examples:

- annual_inc_decile  
- dti_decile  
- revol_util_decile  

**Sparse count variables**

Grouped into:

- 0  
- 1  
- 2+  

Examples:

- delinq_2yrs  
- mort_acc  
- inq_last_6mths  

**Rare event variables**

Grouped into:

- 0  
- 1+  

Examples:

- num_accts_ever_120_pd  
- num_tl_90g_dpd_24m  
- pub_rec  
- pub_rec_bankruptcies  

Segmentation results were aggregated in the table:

`TABLE_DATA.ODR_BY_COLUMN`

Table structure

| column | bucket | n_closed | n_default | odr |
|------|------|------|------|------|

---

# Driver Ranking

To identify meaningful risk drivers, each variable was summarised using several metrics:

- ODR range across buckets  
- bucket count  
- bucket stability  
- concentration of observations  

Key metrics used:

- odr_range  
- bucket_count  
- range_per_bucket  
- min_bucket_n  
- top_bucket_share  
- odr_range_filt (n_closed ≥ 200)  
- range_per_bucket_filt  

Variables are primarily ranked by:

`odr_range_filt`

This ranking highlights borrower attributes that produce the strongest separation in observed default risk.

---

# Example Result

*(Insert driver ranking chart here)*

The ranking highlights borrower attributes that meaningfully separate default risk across borrower segments.

---

# Next Step

Phase 2 will build probability of default models and evaluate **policy outcomes**, including:

- approval rate  
- expected default rate  
- expected loss proxy  

Two modelling strategies will be compared:

**Model A**  
Full feature set

**Model B**  
Top risk drivers only

The goal is to analyse how model design affects **portfolio-level risk and approval trade-offs**.

---

# Repository Structure
credit-risk-policy
│
├── README.md
│
├── sql
│ ├── odr_calculation.sql
│ ├── driver_ranking.sql
│
├── docs
│ └── phase1_eda_summary.pdf
│
├── tableau
│ └── risk_segmentation_dashboard.twb
