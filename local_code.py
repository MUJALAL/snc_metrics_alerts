import os
import snowflake.connector
import requests
import json
from metrics_queries import alert_df
import pandas as pd


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



ALERT_QUERY = alert_df.copy()

print(ALERT_QUERY)


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


# SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'


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
