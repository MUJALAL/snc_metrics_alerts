import os
import snowflake.connector
import requests
import json
from metrics_queries import alert_df
import pandas as pd

def get_snowflake_connection():
    """Establish a connection to Snowflake using private key authentication."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    private_key_path = os.getenv("PRIVATE_KEY_PATH")
    private_key_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE")

    with open(private_key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=private_key_passphrase.encode() if private_key_passphrase else None,
            backend=default_backend()
        )

    private_key = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=private_key,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE", "ROLE_SUPPO_COMM")  # Default role if not set
    )
    return conn


# owner	slack_id	vertical
# Amir Khan	U01HUD11A83	auto-ivr

# Jyoti Diwanji	U0BQ6R0LU	load-assist
# Jyoti Diwanji	U0BQ6R0LU	pnm

# Venkatesh Mariypol	U0BQ9BS3X	pfe
# Venkatesh Mariypol	U0BQ9BS3X	spot

# Ekanth Eswar	U05P2QS5B7U	ptl

# Avani Kushwaha    U07BDJ9BP8B default
# Murtaza Jalal    U03H17HARTR default




SLACK_TAGS = {
    'spot': "<@U0BQ9BS3X>",
    'pfe': "<@U0BQ9BS3X>",
    'ptl': "<@U05P2QS5B7U>",
    'pnm': "<@U0BQ6R0LU>",
    'load-assist': "<@U0BQ6R0LU>",
    'auto-ivr': "<@U01HUD11A83>",
    'default': "<@U03H17HARTR> <@U07BDJ9BP8B>",
}



# ALERT_QUERY = alert_df.copy()

ALERT_QUERY = '''
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
ORDER BY 1,2,3,4
'''

# print(ALERT_QUERY)


start_date = (pd.to_datetime(ALERT_QUERY.dated.unique()[0]) - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
end_date = ALERT_QUERY.dated.unique()[0]


# Metabase dashboard links per route_id
METABASE_LINKS = {
    'spot': 
        {0:f"https://metabase.prod-internal.porter.in/question/6605-dialed-back-attempted-and-connected?start_date={start_date}&dialer_name=&end_date={end_date}&CALL_TYPE_MODIFIED=&city=&source_type=&vehicle_name=&verticals=spot&request_source=&period=Day&order_status=&vicinity=&call_raisedby=",
        1:f"https://metabase.prod-internal.porter.in/question/6683-outbound-dialed-tat?dialer_name=&call_type_modified=&end_date={end_date}&city=&source_type=&vehicle_name=&verticals=spot&request_source=&start_date={start_date}&period=Day&order_status=&vicinity=&call_raisedby="
        },
    'pfe': 
        {0:f"https://metabase.prod-internal.porter.in/question/6605-dialed-back-attempted-and-connected?start_date={start_date}&dialer_name=&end_date={end_date}&CALL_TYPE_MODIFIED=&city=&source_type=&vehicle_name=&verticals=pfe&request_source=&period=Day&order_status=&vicinity=&call_raisedby=",
        1:f"https://metabase.prod-internal.porter.in/question/6683-outbound-dialed-tat?dialer_name=&call_type_modified=&end_date={end_date}&city=&source_type=&vehicle_name=&verticals=pfe&request_source=&start_date={start_date}&period=Day&order_status=&vicinity=&call_raisedby="
        },
    'ptl': 
        {0:f"https://metabase.prod-internal.porter.in/question/6605-dialed-back-attempted-and-connected?start_date={start_date}&dialer_name=&end_date={end_date}&CALL_TYPE_MODIFIED=&city=&source_type=&vehicle_name=&verticals=PTL&request_source=&period=Day&order_status=&vicinity=&call_raisedby=",
        1:f"https://metabase.prod-internal.porter.in/question/6683-outbound-dialed-tat?dialer_name=&call_type_modified=&end_date={end_date}&city=&source_type=&vehicle_name=&verticals=PTL&request_source=&start_date={start_date}&period=Day&order_status=&vicinity=&call_raisedby="
        },
    'pnm': 
        {0:f"https://metabase.prod-internal.porter.in/question/6605-dialed-back-attempted-and-connected?start_date={start_date}&dialer_name=&end_date={end_date}&CALL_TYPE_MODIFIED=&city=&source_type=&vehicle_name=&verticals=pnm&request_source=&period=Day&order_status=&vicinity=&call_raisedby=",
        1:f"https://metabase.prod-internal.porter.in/question/6683-outbound-dialed-tat?dialer_name=&call_type_modified=&end_date={end_date}&city=&source_type=&vehicle_name=&verticals=pnm&request_source=&start_date={start_date}&period=Day&order_status=&vicinity=&call_raisedby="
        },
    'load-assist': 
        {0:f"https://metabase.prod-internal.porter.in/question/6605-dialed-back-attempted-and-connected?start_date={start_date}&dialer_name=&end_date={end_date}&CALL_TYPE_MODIFIED=&city=&source_type=&vehicle_name=&verticals=load-assist&request_source=&period=Day&order_status=&vicinity=&call_raisedby=",
        1:f"https://metabase.prod-internal.porter.in/question/6683-outbound-dialed-tat?dialer_name=&call_type_modified=&end_date={end_date}&city=&source_type=&vehicle_name=&verticals=load-assist&request_source=&start_date={start_date}&period=Day&order_status=&vicinity=&call_raisedby="
        },
    'auto-ivr': 
        {0:f"https://metabase.prod-internal.porter.in/question/6605-dialed-back-attempted-and-connected?start_date={start_date}&dialer_name=&end_date={end_date}&CALL_TYPE_MODIFIED=&city=&source_type=&vehicle_name=&verticals=auto-ivr&request_source=&period=Day&order_status=&vicinity=&call_raisedby=",
        1:f"https://metabase.prod-internal.porter.in/question/6683-outbound-dialed-tat?dialer_name=&call_type_modified=&end_date={end_date}&city=&source_type=&vehicle_name=&verticals=auto-ivr&request_source=&start_date={start_date}&period=Day&order_status=&vicinity=&call_raisedby="
        },
}

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


# SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

# def get_alert_data():
#     print("Connecting to Snowflake...")
#     ctx = get_snowflake_connection()
#     cs = ctx.cursor()
#     try:
#         print("Executing query...")
#         cs.execute(ALERT_QUERY)
#         rows = cs.fetchall()
#         print(f"Query result: {rows}")
#         return rows
#     finally:
#         cs.close()
#         ctx.close()


def send_slack_message(payload, webhook_url):
    """
    Sends a message to Slack using a webhook URL.
    The payload should be a dictionary formatted for the Slack API.
    """
    try:
        response = requests.post(webhook_url, data=json.dumps(payload),
                                 headers={'Content-Type': 'application/json'})
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        print("Slack message sent successfully!")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error sending Slack message: {e}")
        return False


def main():
    print("Starting SNC alert script...")
    # Assuming alerts is a DataFrame
    alerts = ALERT_QUERY  # Replace with your actual data source

    # Prepare the overall Block Kit message payload
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 Outbound Alerts"
            }
        },
        {
            "type": "divider"
        }
    ]

    # Group the alerts by the 'vertical' column
    grouped_alerts = alerts.groupby('vertical')

    # Iterate through each vertical group and build blocks
    for vertical, vertical_group in grouped_alerts:
        # Get the POC tag for the current vertical
        poc_tag = SLACK_TAGS.get(vertical, SLACK_TAGS["default"])
        metabase_card = METABASE_LINKS.get(vertical, SLACK_TAGS["default"])
        
        # Add a header section block for the current vertical with the POC tag
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🚀 *{vertical.upper()}  * {poc_tag}"
            }
        })
        
        # Add a divider for visual separation
        # blocks.append({"type": "divider"})


        # Add a header section block for the current vertical metabase link
        dialed_pct_link = metabase_card[0]
        dialed_tat_link = metabase_card[1]

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"📈 *PCT Card Link:* <{dialed_pct_link}|View Dashboard> 📈 *TAT Card Link:* <{dialed_tat_link}|View Dashboard>"
            }
        })

        # Add a divider for visual separation
        blocks.append({"type": "divider"})

        # Iterate through each row within the current vertical's group
        for index, row in vertical_group.iterrows():
            order_type = row['order_type']
            metric_name = row['metric_name']
            metric_value = row['metric_value']

            # Create a section block for each alert to form a row in the table
            blocks.append({
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*{metric_name}* ({order_type})"
                    }, 
                    {
                        "type": "mrkdwn",
                        "text": f"*{metric_value}*"
                    }
                ]
            })

        # Add a divider to separate verticals
        blocks.append({"type": "divider"})

    # Combine all blocks into the final payload
    payload = {
        "blocks": blocks
    }

    # Send the final message
    send_slack_message(payload, SLACK_WEBHOOK_URL)

if __name__ == "__main__":
    main()