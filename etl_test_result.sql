-- user report

WITH trx AS (SELECT
  transaction_id
  ,user_id
  ,Transaction_Amount
  ,MIN(DATE(Date)) AS first_trx_date
FROM trx
GROUP BY 1,2,3
)
,mdrpr AS (
  SELECT
  Membership_Type
  ,CAST(MDR_Percentage AS BIGNUMERIC) AS mdr_perc
  FROM mdr
)
,member AS (
  SELECT
    user_id
    ,Membership_Type
  FROM memp
)
SELECT
  trx.user_id
  ,member.membership_type
  ,trx.first_trx_date
  ,trx.transaction_amount
FROM trx
LEFT JOIN member ON trx.user_id = member.user_id

;

-- trx report

WITH trx AS (SELECT
  transaction_id
  ,user_id
  ,Transaction_Amount
  ,DATE(Date) AS trx_date
FROM trx
)
,mdrpr AS (
  SELECT
  Membership_Type
  ,CAST(MDR_Percentage AS BIGNUMERIC) AS mdr_perc
  FROM mdr
)
,member AS (
  SELECT
    user_id
    ,member.Membership_Type
    ,mdrpr.mdr_perc
  FROM bigquerydev-303605.frendy_testground.df_memp member
  LEFT JOIN mdrpr ON mdrpr.membership_type = member.membership_type
)
,base AS (
SELECT
  trx.user_id
  ,member.membership_type
  ,trx.trx_date
  ,trx.transaction_amount
  ,member.mdr_perc
FROM trx
LEFT JOIN member ON trx.user_id = member.user_id
)
,FINAL AS (
  SELECT
    base.*
    ,(base.transaction_amount - (base.transaction_amount*(mdr_perc/100))) AS final_amount
  FROM base
)
SELECT * FROM FINAL

-- trx report v2

WITH trx AS (SELECT
  transaction_id
  ,user_id
  ,Transaction_Amount
  ,DATE(Date) AS trx_date
FROM trx
)
,mdrpr AS (
  SELECT
  Membership_Type
  ,CAST(MDR_Percentage AS BIGNUMERIC) AS mdr_perc
  FROM mdr
)
,member AS (
  SELECT
    user_id
    ,member.Membership_Type
    ,mdrpr.mdr_perc
  FROM bigquerydev-303605.frendy_testground.df_memp member
  LEFT JOIN mdrpr ON mdrpr.membership_type = member.membership_type
)
,base AS (
SELECT
  trx.user_id
  ,member.membership_type
  ,trx.trx_date
  ,trx.transaction_amount
  ,member.mdr_perc
FROM trx
LEFT JOIN member ON trx.user_id = member.user_id
)
,FINAL AS (
  SELECT
    base.*
    ,(base.transaction_amount - (base.transaction_amount*(mdr_perc/100))) AS final_amount
  FROM base
)
SELECT 
  EXTRACT(MONTH FROM trx_date) AS month_number
  ,EXTRACT(YEAR FROM trx_date) AS year_number
  ,SUM(final_amount) AS total_amt 
FROM FINAL
GROUP BY 1,2
