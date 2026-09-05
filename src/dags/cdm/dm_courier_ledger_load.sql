BEGIN;

-- В витрине сохраняются данные за позапрошлый и более старые месяцы. 
-- Если в этом нет нужды, достаточно поменять следующий запрос на:
-- TRUNCATE TABLE cdm.dm_courier_ledger RESTART IDENTITY;

DELETE FROM cdm.dm_courier_ledger
WHERE (settlement_year, settlement_month) IN (
	SELECT 
		EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month'),
		EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
);

INSERT INTO cdm.dm_courier_ledger(
	courier_id,
	courier_name, 
	settlement_year,
	settlement_month, 
	orders_count, 
	orders_total_sum, 
	rate_avg, 
	order_processing_fee,
	courier_order_sum, 
	courier_tips_sum, 
	courier_reward_sum
)
WITH prev_month_date AS (
	SELECT 
		EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month') AS prev_month_year,
		EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') AS prev_month
), 
orders_totals AS (
	SELECT 
		order_id,
		SUM(total_sum) AS order_total_sum
	FROM dds.fct_product_sales
	GROUP BY order_id
),
courier_prev_month_orders AS (
	SELECT 
		dmd.courier_id,
		dmo.id AS order_id,
		dmd.rate,
		dmd.tip_sum,
		ot.order_total_sum AS order_total_sum,
		pmd.prev_month_year AS settlement_year,
		pmd.prev_month AS settlement_month
	FROM dds.dm_orders dmo
	JOIN dds.dm_timestamps dmt ON dmo.timestamp_id = dmt.id
	JOIN prev_month_date pmd ON EXTRACT(YEAR FROM dmt.ts) = pmd.prev_month_year 
		AND EXTRACT(MONTH FROM dmt.ts) = pmd.prev_month
	JOIN dds.dm_deliveries dmd ON dmo.delivery_id = dmd.id
	JOIN orders_totals ot ON dmo.id = ot.order_id
),
courier_month_stats AS (
	SELECT 
		courier_id,
		settlement_year,
		settlement_month,
		AVG(rate) AS rate_avg,
		SUM(tip_sum) AS courier_tips_sum
	FROM courier_prev_month_orders
	GROUP BY courier_id, settlement_year, settlement_month
),
order_payments AS (
	SELECT 
		co.courier_id,
		co.settlement_year,
		co.settlement_month,
		co.order_id,
		co.order_total_sum,
		CASE 
			WHEN cms.rate_avg < 4.0 THEN GREATEST(co.order_total_sum * 0.05, 100.0)
			WHEN cms.rate_avg >= 4.0 AND cms.rate_avg < 4.5 THEN GREATEST(co.order_total_sum * 0.07, 150.0)
			WHEN cms.rate_avg >= 4.5 AND cms.rate_avg < 4.9 THEN GREATEST(co.order_total_sum * 0.08, 175.0)
			ELSE GREATEST(co.order_total_sum * 0.10, 200.0)
		END AS courier_order_payment
	FROM courier_prev_month_orders co	
	JOIN courier_month_stats cms 
		ON co.courier_id = cms.courier_id 
		AND co.settlement_year = cms.settlement_year 
		AND co.settlement_month = cms.settlement_month
)
SELECT 
	dmc.courier_id AS courier_id,
	dmc.courier_name AS courier_name,
	cms.settlement_year,
	cms.settlement_month,
	COUNT(op.order_id) AS orders_count,
	SUM(op.order_total_sum) AS orders_total_sum,
	cms.rate_avg,
	SUM(op.order_total_sum) * 0.25 AS order_processing_fee,
	SUM(op.courier_order_payment) AS courier_order_sum,
	cms.courier_tips_sum,
	SUM(op.courier_order_payment) + cms.courier_tips_sum * 0.95 AS courier_reward_sum
FROM courier_month_stats cms
JOIN dds.dm_couriers dmc ON cms.courier_id = dmc.id
JOIN order_payments op ON cms.courier_id = op.courier_id 
	AND cms.settlement_year = op.settlement_year 
	AND cms.settlement_month = op.settlement_month
GROUP BY 
	dmc.id,
	dmc.courier_name,
	cms.settlement_year,
	cms.settlement_month,
	cms.rate_avg,
	cms.courier_tips_sum;

COMMIT;