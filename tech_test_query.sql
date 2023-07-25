-- Author: Qingyang Dong
-- Purpose: Data Analyst Technical Test
-- Learning Outcomes: Window functions, access to postgres by using command/pgAdmin4, understand terms in forex
-- Assumptions made: 
-- 1. sum of volume traded by login/server/symbol in previous 7 days = Previous 7 Rows + Current Date Volume
-- 2. sum_volume_2020_08, double, sum of volume traded by login/server/symbol for August 2020
--    only, up to and including current dt_report
--    Implies sum of volume within beginning of Aug till the report date if 20200801 < report date <20200831
--    volume will be 0 if report date < 20200801 or > 20200831. 
-- 3. date_first_trade, timestamp, datetime of first trade by login/server/symbol, up to and
--    including current dt_report.
--    Implies date_first_trade >= dt_report -> NULL(Assume we don't know the date yet) else actual date_first_trade
--    Also assumed open_time instead of close time is used for date_first_trade
-- 4.  a github link to your postgres query called 'tech_test_quesy.sql' assumed 'tech_test_query.sql'
WITH date_range AS (
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval) AS dt_report
),
-- Extract user's first trade date for each server/symbol first
first_trade AS(
	SELECT
		login_hash,
		server_hash,
		symbol,
		min(DATE(open_time)) AS date_first_trade
	FROM trades
	GROUP BY
		login_hash,
		server_hash,
		symbol
),
-- Summarize User Data from trades, users and join the Series Report above
user_data AS (
    SELECT
        dc.login_hash,
        dc.server_hash,
        dc.symbol,
        DATE(dr.dt_report) as dt_report,
		sum(t.volume) as volume, -- sum volume for each login/server/symbol
		count(t.ticket_hash) as count_t -- count #trade for each login/server/symbol on that day
	FROM
        date_range dr
    CROSS JOIN
        (SELECT DISTINCT login_hash, server_hash, symbol FROM trades) dc
    LEFT JOIN
        trades t ON dr.dt_report = DATE(t.close_time) AND dc.login_hash = t.login_hash AND dc.server_hash = t.server_hash AND dc.symbol = t.symbol
    INNER JOIN
        users u ON dc.login_hash = u.login_hash
    WHERE
        u.enable = 1 -- enable user only
	GROUP BY
		dc.login_hash,
        dc.server_hash,
        dc.symbol,
        DATE(dr.dt_report)
),
-- Aggregate user info by using Window function
agg_user AS (
	SELECT
		login_hash,
		server_hash,
		symbol,
		dt_report,
		volume,
		count_t,
		-- sum previous 7 days volume
		COALESCE(sum(volume) 
			OVER (PARTITION BY 
				login_hash, server_hash, symbol ORDER BY dt_report 
			ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 0) AS sum_volume_prev_7d,
		-- sum previous rows volume
		COALESCE(sum(volume) 
			OVER (PARTITION BY 
				login_hash, server_hash, symbol ORDER BY dt_report 
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS sum_volume_prev_all,
		-- sum volume only if dt_report in 2020 Aug
	 	COALESCE(SUM(CASE WHEN 
						DATE(dt_report) >= '2020-08-01' AND DATE(dt_report) <= '2020-08-31'
					THEN volume ELSE 0 END)
			OVER (PARTITION BY 
				  	login_hash, server_hash, symbol ORDER BY dt_report), 0) AS sum_volume_2020_08,
		-- sum #trades in previous 7 days
	 	COALESCE(sum(count_t)
			OVER (PARTITION BY 
				login_hash, server_hash, symbol ORDER BY dt_report 
			ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 0) AS count_trade_prev_7d
		FROM user_data
),
-- Use sum_volume_prev_7d, count_trade_prev_7d compiled from agg_user to get dense rank
dense AS (
	select 
		ag.login_hash,
		ag.server_hash,
		ag.symbol,
		ag.dt_report,
		-- If date_first_trade >= dt_report -> NULL(Assume we don't know the date yet) else actual date_first_trade
		(CASE WHEN
			ft.date_first_trade <= ag.dt_report
		THEN ft.date_first_trade END) as date_first_trade,
		-- dense rank based on sum_volume_prev_7d
		DENSE_RANK() 
			OVER (PARTITION BY 
				  	ag.login_hash, ag.symbol ORDER BY ag.sum_volume_prev_7d DESC) AS rank_volume_symbol_prev_7d,
		-- dense rank based on count_trade_prev_7d
		DENSE_RANK() 
			OVER (PARTITION BY 
				  	ag.login_hash ORDER BY ag.count_trade_prev_7d DESC) AS rank_count_prev_7d
	FROM agg_user ag
	LEFT JOIN
		first_trade ft ON ag.login_hash = ft.login_hash AND ag.symbol = ft.symbol AND ag.server_hash = ft.server_hash
)
-- Main query
SELECT 
	ROW_NUMBER() OVER () AS id,
	ag.login_hash,
	ag.server_hash,
	ag.symbol,
	ag.dt_report,
	ag.sum_volume_prev_7d,
	ag.sum_volume_prev_all,
	ds.rank_volume_symbol_prev_7d,
	ds.rank_count_prev_7d,
	ag.sum_volume_2020_08,
	ds.date_first_trade
FROM agg_user ag
LEFT JOIN
	dense ds ON ag.login_hash = ds.login_hash AND ag.symbol = ds.symbol AND ag.dt_report = ds.dt_report AND ag.server_hash = ds.server_hash
ORDER BY
	ag.login_hash,
	ag.server_hash,
	ag.symbol,
	ag.dt_report;
