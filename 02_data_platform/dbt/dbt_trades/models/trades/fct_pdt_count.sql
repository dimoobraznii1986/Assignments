with prev_trades as (
select
	t.account_id, t.symbol, t.side, t.qty, t.timestamp, coalesce(lag(t.side) over (partition by t.account_id, t.symbol order by t.timestamp asc), t.side) as previous_side, coalesce(lag(t.timestamp) over (partition by t.account_id, t.symbol order by t.timestamp asc), t.timestamp) as previous_timestamp, DATE(t.timestamp) as date, DATE(coalesce(lag(t.timestamp) over (partition by t.account_id, t.symbol order by t.timestamp asc), t.timestamp)) as previous_date,
	case
		when t.side = coalesce(lag(t.side) over (partition by t.account_id, t.symbol order by t.timestamp asc), t.side)
		or DATE(t.timestamp) <> DATE(coalesce(lag(t.timestamp) over (partition by t.account_id, t.symbol order by t.timestamp asc), t.timestamp)) then 0
		else 1
	end as pair_flag
from
	   {{ source('raw','trades')}} t
),
trades_with_flags as (
select
	*, coalesce(lag(pair_flag) over (partition by account_id, symbol order by timestamp asc), 0) as previous_pair_flag
from
	prev_trades ),
trades_with_day_trades as (
select
	trades_with_flags.*,
	case
		when pair_flag = 1
		and previous_pair_flag = 0 then 1
		else 0
	end as day_trades
from
	trades_with_flags ),
update_pair_flag as (
select
	trades_with_day_trades.*,
	case
		when side <> previous_side
		and date = previous_date
		and previous_pair_flag = 1 then 0
		else 1
	end as pair_flag
from
	trades_with_day_trades )
select
	account_id,
	SUM(day_trades) as total_day_trades
from
	update_pair_flag
group by
	account_id