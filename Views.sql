USE [CBS]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[vw_atm_performance] as 
select location, count(*) as total_transactions,
avg(response_time_ms) as avg_response_time
from atm_logs
group by location;
GO

create view [dbo].[vw_channel_transactions_summary] as
select channel, count(*) as total_transactions, sum(amount)
as total_amount
from core_banking_transactions group by channel;
GO

create view [dbo].[vw_peak_transaction_hours] as
select datepart(hour, tnx_timestamp) as tnx_hour,
count(*) as total_transactions
from core_banking_transactions
group by datepart(hour,tnx_timestamp);
GO

create view [dbo].[vw_replication_lag_summary] as
select environment, avg(replicat_lag_sec) as avg_replication_lag,
max(replicat_lag_sec) as max_replication_lag
from replication_metrics
group by environment;
GO

create view [dbo].[vw_tnx_vs_lag] as
with hourly_tnx as (
select datepart(hour,tnx_timestamp) as tnx_hour,
count(*) as tnx_count
from core_banking_transactions
group by datepart(hour,tnx_timestamp) )

select h.tnx_hour, h.tnx_count, r.replicat_lag_sec
from hourly_tnx h join replication_metrics r 
on datepart(hour, r.metric_timestamp)=h.tnx_hour;
GO

create view [dbo].[vw_top_accounts] as 
select top 10 account_id, sum(amount) as total_transaction_value
from core_banking_transactions
group by account_id
order by total_transaction_value desc;
GO

create view [dbo].[vw_transaction_failure_rate] as 
select tnx_status, count(*) as total_tnx,
count(*) *100.0/sum(count(*)) over() as percentages
from core_banking_transactions
group by tnx_status;
GO