WITH crm_latest AS (
SELECT
company_id,
name,
country,
industry_tag,
last_contact_at,
ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY extract_date DESC) AS rn
FROM stg_crm
),

api_daily AS (
SELECT
company_id,
date,
active_users,
events
FROM stg_api
),

rolling AS (
SELECT
a.company_id,
a.date,
SUM(a.active_users) OVER ( PARTITION BY a.company_id ORDER BY a.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS active_users_7d,
SUM(a.events) OVER ( PARTITION BY a.company_id ORDER BY a.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS events_7d
FROM api_daily a
),

churn_flag AS (
SELECT 
r.company_id,
r.date,
CASE 
WHEN r.active_users_7d < 100 OR r.active_users_7d IS NULL THEN 1
ELSE 0 END AS is_churn_risk
FROM rolling r
)

INSERT INTO fact_company_activity (
company_id,
date,
name,
country,
industry_tag,
last_contact_at,
active_users,
events,
active_users_7d,
events_7d,
is_churn_risk,
inserted_at
)
SELECT
api.company_id,
api.date,
crm.name,
crm.country,
crm.industry_tag,
crm.last_contact_at,
api.active_users,
api.events,
roll.active_users_7d,
roll.events_7d,
churn.is_churn_risk,
GETDATE() AS inserted_at
FROM api_daily api
LEFT JOIN crm_latest crm   ON api.company_id = crm.company_id   AND crm.rn = 1
LEFT JOIN rolling roll     ON api.company_id = roll.company_id  AND api.date = roll.date
LEFT JOIN churn_flag churn ON api.company_id = churn.company_id AND api.date = churn.date;
