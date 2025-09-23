import yaml
import os
import time
import pandas as pd
import numpy as np
import psycopg2
import snowflake.connector  
import json
import warnings
warnings.filterwarnings('ignore')
import datetime 

import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = 'browser'

from db_utils import get_sf_connection, fetch_data, log, write_dataframe_to_snowflake 

conn = get_sf_connection('murtaza.jalal@theporter.in')



alert_query = '''
-- DIALED TAT 


with threshold_cte as (
SELECT *
FROM prod_curated.support_communication.outbound_alert_threshold
)

, dialed_cte as (
SELECT      
    dated,
    lower(vertical) as vertical,
    lower(order_type) as order_type,
    sum(total_requests) as total_request,
    sum(unique_request) as unique_request,
    sum(unique_requests_dialed) as unique_requests_dialed,
    sum(unique_requests_attempted) as unique_requests_attempted,
    sum(unique_requests_connected) as unique_requests_connected,
    
    round(100.0*sum(unique_requests_dialed) / sum(unique_request),2) as dialed_pct,
    round(100.0*sum(unique_requests_attempted) / sum(unique_request),2) as attempted_pct,
    round(100.0*sum(unique_requests_connected) / sum(unique_request),2) as connected_pct
    
FROM prod_eldoria.mart.snc_outbound_dialed
WHERE dated = current_date - 1
GROUP BY 1,2,3
ORDER BY 1,2,3
)

, tat_cte as (
SELECT 
    dated,
    lower(vertical) as vertical,
    order_type,
    
    case 
    when dialed_sec > 60 then cast(round((floor(dialed_sec/60)*100 + mod(dialed_sec,60))/100, 2) as float) 
    when dialed_sec >= 10 then cast(round(dialed_sec/100,2) as float) 
    when dialed_sec < 10 then cast(round(dialed_sec/100,2) as float) 
    end as dialed_tat,
    
    case 
    when attempted_sec > 60 then cast(round((floor(attempted_sec/60)*100 + mod(attempted_sec,60))/100, 2) as float) 
    when attempted_sec >= 10 then cast(round(attempted_sec/100,2) as float) 
    when attempted_sec < 10 then cast(round(attempted_sec/100,2) as float) 
    end as attempted_tat,
    
    case 
    when connected_sec > 60 then cast(round((floor(connected_sec/60)*100 + mod(connected_sec,60))/100, 2) as float) 
    when connected_sec >= 10 then cast(round(connected_sec/100,2) as float) 
    when connected_sec < 10 then cast(round(connected_sec/100,2) as float) 
    end as connected_tat,
    
    
FROM(
SELECT  
    dated, 
    vertical,
    order_type,
    round(percentile_cont(0.9) within group (order by first_dialed_tat asc)) as dialed_sec,
    round(percentile_cont(0.9) within group (order by first_attempted_time asc)) as attempted_sec,
    round(percentile_cont(0.9) within group (order by first_connected_time asc)) as connected_sec
FROM prod_eldoria.mart.snc_outbound_tat
WHERE dated = current_date - 1
GROUP BY 1,2,3
) cte
)




, metric_cte as (
SELECT t1.*, dialed_tat, attempted_tat, connected_tat
FROM dialed_cte t1
JOIN tat_cte t2
ON t1.dated = t2.dated
and t1.vertical = t2.vertical
and t1.order_type = t2.order_type
)



, final_cte as(
SELECT t1.*, t2.* exclude(order_type, vertical)
FROM metric_cte t1
LEFT JOIN threshold_cte t2
ON t1.vertical = t2.vertical
and t1.order_type = t2.order_type
)

, dialed_pct as (
SELECT 
    dated,
    vertical,
    order_type,
    'Dialed %' as metric_name,
    concat(dialed_pct::varchar, ' %')::varchar as metric_value,
    dialed_pct_threshold as metric_threshold
FROM final_cte
WHERE dialed_pct < dialed_pct_threshold
)


, attempted_pct as (
SELECT 
    dated,
    vertical,
    order_type,
    'Attempted %' as metric_name,
    concat(attempted_pct::varchar, ' %')::varchar as metric_value,
    attempted_pct_threshold as metric_threshold
FROM final_cte
WHERE attempted_pct < attempted_pct_threshold
)


, connected_pct as (
SELECT 
    dated,
    vertical,
    order_type,
    'Connected %' as metric_name,
    concat(connected_pct::varchar, ' %')::varchar as metric_value,
    connected_pct_threshold as metric_threshold
FROM final_cte
WHERE connected_pct < connected_pct_threshold
)


, dialed_tat as (
SELECT 
    dated,
    vertical,
    order_type,
    'Dialed Tat' as metric_name,
    concat(dialed_tat::varchar, ' min')::varchar as metric_value,
    dialed_tat_threshold as metric_threshold
FROM final_cte
WHERE dialed_tat > dialed_tat_threshold
)


, attempted_tat as (
SELECT 
    dated,
    vertical,
    order_type,
    'Attempted Tat' as metric_name,
    concat(attempted_tat::varchar, ' min')::varchar as metric_value,
    attempted_tat_threshold as metric_threshold
FROM final_cte
WHERE attempted_tat > attempted_tat_threshold
)


, connected_tat as (
SELECT 
    dated,
    vertical,
    order_type,
    'Connected Tat' as metric_name,
    concat(connected_tat::varchar, ' min')::varchar as metric_value,
    connected_tat_threshold as metric_threshold
FROM final_cte
WHERE connected_tat > connected_tat_threshold
)



, alert_cte as (
SELECT *
FROM dialed_pct
UNION ALL
SELECT *
FROM attempted_pct
UNION ALL
SELECT *
FROM connected_pct
UNION ALL
SELECT *
FROM dialed_tat
UNION ALL
SELECT *
FROM attempted_tat
UNION ALL
SELECT *
FROM connected_tat
)

SELECT *
FROM alert_cte
ORDER BY 1,2,3,4;
'''


alert_df = fetch_data(conn, alert_query)
