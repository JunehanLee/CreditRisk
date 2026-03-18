# LendingClub Credit Risk Segmentation & Policy Analysis

This project analyses borrower risk segmentation using LendingClub loan data.

The goal is to move beyond model accuracy and connect **probability of default (PD)** to underwriting decisions such as:

- approval rate
- expected default rate (EDR)
- expected loss proxy

This repository currently documents **Phase 1: Risk Segmentation Analysis**.

---

## Dataset

- Source: https://www.kaggle.com/datasets/adarshsng/lending-club-loan-data-csv
- Column description: https://www.notion.so/30fdfd642ca1808e8925dfb0e506253b?v=30fdfd642ca180c58a34000cb40edb45
- Raw table: `RAW_DATA.LOAN_DATA_RAW`
- Clean table: `CURATED_DATA.LOAN_CLEAN`

Target definition:

- `Charged Off = 1`
- `Fully Paid = 0`

All other loan statuses were excluded, so the analysis uses **closed loans only**.

---

## Method

Borrowers were segmented by feature and **Observed Default Rate (ODR)** was calculated.

`ODR = defaults / closed loans`

Bucket rules:

- Numeric variables → deciles using `NTILE(10)`
- Sparse count variables → `0 / 1 / 2+`
- Rare event variables → `0 / 1+`

Results were aggregated in:

`TABLE_DATA.ODR_BY_COLUMN`

Variables were then ranked using summary metrics such as:

- `odr_range`
- `bucket_count`
- `min_bucket_n`
- `top_bucket_share`
- `odr_range_filt (n_closed ≥ 200)`

Primary ranking metric:

`odr_range_filt`

---

## Results

Phase 1 summary is available here:

**[Phase 1 EDA Summary]([docs/Phase1_eda_summary.pdf])**
<img width="1499" height="1199" alt="대시보드 1" src="https://github.com/user-attachments/assets/5d5505cf-908b-4be1-8b3d-0f12bc7a39fc" />

**[Link to Dashboard](https://public.tableau.com/app/profile/junehan.lee/viz/CREDIT_17737007005780/1_1#1)**

---

## Next Step

Phase 2 will build PD models and compare policy outcomes between:

- full feature set
- top risk drivers only

---

## Repository Structure

```text
CreditRisk
├── README.md
├── sql
│   └── 01_Inport_loan_RAW.sql
│   └── 02_create_curated_loan_clean.sql
│   └── 03_odr_segmentation_and_summary.sql
├── docs
│   └── phase1_eda_summary.pdf
└── tableau
