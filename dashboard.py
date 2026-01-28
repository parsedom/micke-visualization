import streamlit as st
import boto3
import pandas as pd
from datetime import datetime, timedelta
import json
import pytz
import plotly.express as px
from boto3.dynamodb.conditions import Key, Attr
import os
import hashlib
import hmac
from st_aggrid import AgGrid, GridOptionsBuilder
import io
import time

st.set_page_config(
    page_title="Hotel Booking Dashboard",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded"
)

AVAILABLE_BOARDS = [
    "price_dashboard",
    "historical_calendar"
]

BOARD_LABELS = {
    "price_dashboard": "📊 Price Dashboard",
    "historical_calendar": "📅 Historical Price Calender",
}

# ==================== CONFIGURATION & CONSTANTS ====================
ZONE1_HOTELS = [
    "Courtyard Tampere City",
    "Forenom Aparthotel Tampere Kauppakatu",
    "H28 - Hotel, Apartments and Suites by UHANDA",
    "Holiday Club Tampereen Kehräämö",
    "Holiday Inn Tampere - Central Station by IHG",
    "Hotel Kauppi",
    "Hotelli Vaakko - Hotel and Apartments by UHANDA",
    "Lapland Hotels Arena",
    "Lapland Hotels Tampere",
    "Lillan Hotel & Kök",
    "Original Sokos Hotel Ilves Tampere",
    "Original Sokos Hotel Villa Tampere",
    "Radisson Blu Grand Hotel Tammer",
    "Scandic Rosendahl",
    "Scandic Tampere City",
    "Scandic Tampere Hämeenpuisto",
    "Scandic Tampere Koskipuisto",
    "Scandic Tampere Station",
    "Solo Sokos Hotel Torni Tampere",
    "Unity Tampere - A Studio Hotel"
]

ZONE2_HOTELS = [
    "Courtyard Tampere City",
    "Forenom Aparthotel Tampere Kauppakatu",
    "H28 - Hotel, Apartments and Suites by UHANDA",
    "Holiday Club Tampereen Kehräämö",
    "Holiday Inn Tampere - Central Station by IHG",
    "Hotel Citi Inn",
    "Hotel Hermica",
    "Hotel Homeland",
    "Hotel Kauppi",
    "Hotelli Vaakko - Hotel and Apartments by UHANDA",
    "Hotelli Ville",
    "Lapland Hotels Arena",
    "Lapland Hotels Tampere",
    "Lillan Hotel & Kök",
    "Mango Hotel",
    "Omena Hotel Tampere",
    "Original Sokos Hotel Ilves Tampere",
    "Original Sokos Hotel Villa Tampere",
    "Radisson Blu Grand Hotel Tammer",
    "Scandic Eden Nokia",
    "Scandic Rosendahl",
    "Scandic Tampere City",
    "Scandic Tampere Hämeenpuisto",
    "Scandic Tampere Koskipuisto",
    "Scandic Tampere Station",
    "Solo Sokos Hotel Torni Tampere",
    "Unity Tampere - A Studio Hotel",
    "Uumen Hotels - Tampere, Finlayson"
]

ZONE3_HOTELS = [
    "H28 - Hotel, Apartments and Suites by UHANDA",
    "Forenom Aparthotel Tampere Kauppakatu",
    "Forenom Serviced Apartments Tampere Pyynikki",
    "Scandic Tampere Hämeenpuisto",
    "Uumen Hotels - Tampere, Finlayson",
    "Scandic Tampere Koskipuisto",
    "Radisson Blu Grand Hotel Tammer",
    "Original Sokos Hotel Ilves Tampere",
    "Omena Hotel Tampere",
    "Scandic Tampere City",
    "Holiday Inn Tampere - Central Station by IHG",
    "Solo Sokos Hotel Torni Tampere",
    "Scandic Tampere Station",
    "Lapland Hotels Arena",
    "Hotel Citi Inn",
    "Scandic Rosendahl",
    "Original Sokos Hotel Villa Tampere",
    "Lapland Hotels Tampere",
    "Unity Tampere - A Studio Hotel",
    "Courtyard Tampere City",
    "Hotelli Ville",
    "Hotel Homeland",
    "Mango Hotel",
    "Holiday Club Tampereen Kehräämö",
    "Hotel Kauppi",
    "Varala Sports & Nature Hotel",
    "Lillan Hotel & Kök",
    "Forenom Aparthotel Tampere Kaleva",
    "Norlandia Tampere Hotel",
    "Hotelli Vaakko - Hotel and Apartments by UHANDA",
    "Hotel Lamminpää",
    "Hotel Hermica",
    "Oma Hotelli",
    "Scandic Eden Nokia",
    "Hotelli Iisoppi",
    "Hotelli Kuohu Kangasala - Hotel and Apartments by UHANDA",
    "Hotel Urkin Pillopirtti",
    "Hotel Waltikka",
    "Aapiskukko Hotel",
    "Hotel Ackas",
    "Hotelli Sointula"
]

Alert_Comparison = [
    "Unity Tampere - A Studio Hotel",
    "Scandic Tampere Station",
    "Scandic Tampere Koskipuisto",
    "Scandic Tampere Hämeenpuisto",
    "Scandic Tampere City",
    "Scandic Rosendahl",
    "Original Sokos Hotel Villa Tampere",
    "Hotelli Vaakko - Hotel and Apartments by UHANDA",
    "Hotel Kauppi",
    "Holiday Inn Tampere - Central Station by IHG",
    "Holiday Club Tampereen Kehräämö",
    "H28 - Hotel, Apartments and Suites by UHANDA",
    "Courtyard Tampere City"
]

aws_key = st.secrets["AWS_ACCESS_KEY_ID"]
aws_secret = st.secrets["AWS_SECRET_ACCESS_KEY"]
region = st.secrets["AWS_DEFAULT_REGION"]


dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret,
    region_name=region
)

table = dynamodb.Table('HotelPrices')
table_calender = dynamodb.Table('HotelPricesCalendar')
table_user = dynamodb.Table('MickeUser')
table_logs = dynamodb.Table('MickeLoginLogs')


def check_password():
    """Returns True if the user has the correct password using DynamoDB."""

    def password_entered():
        username = st.session_state.get("username")
        password = st.session_state.get("password")

        if not username or not password:
            st.session_state["password_correct"] = False
            return

        try:
            response = table_user.get_item(
                Key={"username": username}
            )
            user = response.get("Item")

            if user and hmac.compare_digest(password, user.get("password", "")):
                st.session_state["password_correct"] = True
                st.session_state["authenticated_user"] = user["username"]
                st.session_state["access"] = user.get("access", "user")
                if user.get("access") == "admin":
                    st.session_state["boards"] = AVAILABLE_BOARDS
                    st.session_state["locations"] = ["tampere", "oulu", "rauma", "turku", "jyvaskyla"]
                else:
                    st.session_state["boards"] = user.get("boards", [])
                    st.session_state["locations"] = user.get("locations", [])

                helsinki_tz = pytz.timezone('Europe/Helsinki')
                finland_now = datetime.now(helsinki_tz)

                table_user.update_item(
                    Key={"username": user["username"]},
                    UpdateExpression="SET last_login = :l",
                    ExpressionAttributeValues={
                        ":l": finland_now.strftime("%d/%m/%Y")
                    }
                )

                try:
                    table_logs.put_item(
                        Item={
                            "username": user["username"],
                            "login_ts": finland_now.isoformat(),
                            "login_date": finland_now.strftime("%Y-%m-%d")
                        }
                    )
                except Exception as e:
                    st.warning("Login logged with warning")
                    print(e)

                del st.session_state["password"]
                del st.session_state["username"]
            else:
                st.session_state["password_correct"] = False

        except Exception as e:
            st.session_state["password_correct"] = False
            st.error(f"Login error: {e}")

    if st.session_state.get("password_correct", False):
        return True

    st.markdown("""
    <div style="max-width: 400px; margin: 5rem auto; padding: 2rem; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                border-radius: 15px;">
        <h1 style="color: white; text-align: center;">🏨 Hotel Dashboard Login</h1>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.text_input("Username", key="username")
        st.text_input("Password", type="password", key="password")
        st.button("Login", on_click=password_entered, type="primary", use_container_width=True)

        if "password_correct" in st.session_state and not st.session_state["password_correct"]:
            st.error("😞 User not known or password incorrect")

    return False

def logout():
    """Clear session state for logout"""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

if not check_password():
    st.stop()


# ==================== STYLING ====================
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
    .stDataFrame {
        border: 1px solid #e6e6e6;
        border-radius: 8px;
    }
    .hotel-selector {
        background: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .user-info {
        background: #e8f4fd;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        border-left: 4px solid #1f77b4;
    }
    .calendar-table {
        width: 100%;
        border-collapse: collapse;
        margin: 20px 0;
        border: 2px solid #000;
    }
    .calendar-table td {
        border: 1px solid #000;
        padding: 0;
        height: 80px;
        position: relative;
    }
    .week-label {
        text-align: center;
        font-weight: bold;
        padding: 10px;
        background-color: #f5f5f5;
        border-right: 2px solid #000;
        width: 150px !important;
        height: 80px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        color: #000;
    }
    .day-label {
        text-align: center;
        font-weight: bold;
        padding: 8px;
        background-color: #f5f5f5;
        border: 1px solid #000;
        min-height: 30px;
        flex: 1;
        color: #000;
    }
    .cell-content {
        width: 100%;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        color: white;
        font-weight: bold;
        text-align: center;
        padding: 10px;
        box-sizing: border-box;
        color: #000;
    }
    .cell-date {
        font-size: 12px;
        margin-bottom: 5px;
    }
    .cell-value {
        font-size: 18px;
    }
    .empty-cell {
        background-color: #ffffff;
    }
</style>
""", unsafe_allow_html=True)

def get_default_color_ranges():
    """Get default color ranges for all zones"""
    return {
        "zone1": [
            {'min': 0.0, 'max': 124.99, 'color': '#08306b'},
            {'min': 125.0, 'max': 134.99, 'color': '#2171b5'},
            {'min': 135.0, 'max': 144.99, 'color': '#a2cff8'},
            {'min': 145.0, 'max': 154.99, 'color': '#ffffff'},
            {'min': 155.0, 'max': 164.99, 'color': '#ffa0a0'},
            {'min': 165.0, 'max': 199.99, 'color': '#f86868'},
            {'min': 200.0, 'max': 249.99, 'color': '#d81919'},
            {'min': 250.0, 'max': 999999.0, 'color': '#000000'}
        ],
        "zone2": [
            {'min': 0.0, 'max': 124.99, 'color': '#08306b'},
            {'min': 125.0, 'max': 134.99, 'color': '#2171b5'},
            {'min': 135.0, 'max': 144.99, 'color': '#a2cff8'},
            {'min': 145.0, 'max': 154.99, 'color': '#ffffff'},
            {'min': 155.0, 'max': 164.99, 'color': '#ffa0a0'},
            {'min': 165.0, 'max': 199.99, 'color': '#f86868'},
            {'min': 200.0, 'max': 249.99, 'color': '#d81919'},
            {'min': 250.0, 'max': 999999.0, 'color': '#000000'}
        ],
        "zone3": [
            {'min': 0.0, 'max': 124.99, 'color': '#08306b'},
            {'min': 125.0, 'max': 134.99, 'color': '#2171b5'},
            {'min': 135.0, 'max': 144.99, 'color': '#a2cff8'},
            {'min': 145.0, 'max': 154.99, 'color': '#ffffff'},
            {'min': 155.0, 'max': 164.99, 'color': '#ffa0a0'},
            {'min': 165.0, 'max': 199.99, 'color': '#f86868'},
            {'min': 200.0, 'max': 249.99, 'color': '#d81919'},
            {'min': 250.0, 'max': 999999.0, 'color': '#000000'}
        ],
        "alert": [
            {'min': 0.0, 'max': 114.99, 'color': '#08306b'},
            {'min': 115.0, 'max': 124.99, 'color': '#2171b5'},
            {'min': 125.0, 'max': 134.99, 'color': '#a2cff8'},
            {'min': 135.0, 'max': 144.99, 'color': '#ffffff'},
            {'min': 145.0, 'max': 154.99, 'color': '#ffa0a0'},
            {'min': 155.0, 'max': 189.99, 'color': '#f86868'},
            {'min': 190.0, 'max': 239.99, 'color': '#d81919'},
            {'min': 240.0, 'max': 999999.0, 'color': '#000000'}
        ]
    }


def get_text_color_from_background(hex_color):
    """
    Determine if text should be white or black based on background color brightness.
    Uses relative luminance calculation (WCAG standard).
    """
    # Remove '#' if present
    hex_color = hex_color.lstrip('#')
    
    # Convert hex to RGB
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    
    # Calculate relative luminance using WCAG formula
    def adjust_color(c):
        c = c / 255.0
        if c <= 0.03928:
            return c / 12.92
        else:
            return ((c + 0.055) / 1.055) ** 2.4
    
    r_adjusted = adjust_color(r)
    g_adjusted = adjust_color(g)
    b_adjusted = adjust_color(b)
    
    luminance = 0.2126 * r_adjusted + 0.7152 * g_adjusted + 0.0722 * b_adjusted
    
    # If luminance is high (light background), use dark text; otherwise use white text
    return "#000000" if luminance > 0.5 else "#FFFFFF"


def get_color_from_price_ranges(value, price_ranges):
    """Generate color based on price value and defined ranges."""
    if not price_ranges:
        return "rgb(150, 150, 150)"
    
    # Sort ranges by min value
    sorted_ranges = sorted(price_ranges, key=lambda x: x['min'])
    
    for range_item in sorted_ranges:
        if range_item['min'] <= value <= range_item['max']:
            return range_item['color']
    
    # If value is outside all ranges, use the closest range color
    if value < sorted_ranges[0]['min']:
        return sorted_ranges[0]['color']
    else:
        return sorted_ranges[-1]['color']
    
# ==================== QUERY FUNCTIONS ====================
def query_hotels(filters, date_range, scraped_date_start, scraped_date_end):
    """Query DynamoDB for hotel prices based on filters and date ranges."""
    location = filters.get('location')
    time = filters.get('time')
    persons = filters.get('persons')
    nights = filters.get('nights')
    
    if date_range:
        dates = date_range.split(' - ')
        def convert_date_format(date_str):
            day, month, year = date_str.split('-')
            return f"{year}-{month}-{day}"
        
        checkin_start = convert_date_format(dates[0])
        checkin_end = convert_date_format(dates[1])
    else:
        checkin_start = None
        checkin_end = None

    partition_key = f"{location}#{persons}#{nights}#{time}"
    
    filter_expression = None
    if checkin_start and checkin_end:
        filter_expression = Attr('checkin_date').between(checkin_start, checkin_end)
    
    all_items = []
    
    try:
        key_condition = (
            Key('location#persons#nights#time').eq(partition_key) &
            Key('scraped_date#hotel_id#checkin_date#checkout_date')
                .between(f"{scraped_date_start}#", f"{scraped_date_end}~")
        )
        
        response = table.query(
            KeyConditionExpression=key_condition,
            FilterExpression=filter_expression
        )
        
        items = response['Items']
        
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=key_condition,
                FilterExpression=filter_expression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
        
        all_items.extend(items)
        
        transformed_items = []
        for item in all_items:
            transformed_items.append({
                'name': item.get('hotel_name', ''),
                'price': item.get('price', 0),
                'price_date': item.get('checkin_date', ''),
                'scrape_date': item.get('scraped_date', ''),
                'location': item.get('location', ''),
                'persons': item.get('persons', 0),
                'nights': item.get('nights', 0),
                'time': item.get('time', ''),
                'review_score': item.get('review_score', 0),
                'city': item.get('city', ''),
                'distance': item.get('distance', ''),
                'hotel_url': item.get('hotel_url', ''),
                'breakfast_included': item.get('breakfast_included', False),
                'free_cancellation': item.get('free_cancellation', False)
            })
        
        return transformed_items
    
    except Exception as e:
        st.error(f"Error querying DynamoDB: {str(e)}")
        return []

# def query_calendar_data(price_start_date, price_end_date, zone_filter="zone1", location="tampere"):
#     """Query data for calendar heatmap."""
#     price_dates = []
#     d = price_start_date
#     while d <= price_end_date:
#         price_dates.append(d.strftime("%Y-%m-%d"))
#         d += timedelta(days=1)
    
#     metrics = {
#         'availability': {},
#         'price_avg': {},
#         'free_cancel_avg': {}
#     }

#     for idx, pdate in enumerate(price_dates, 1):
#         partition_key = f"{location}#{zone_filter}#{pdate}"
#         try:
#             response = table_calender.get_item(
#                 Key={'location#zone#checkin_date': partition_key}
#             )
#             item = response.get('Item')
#             if item:
#                 metrics["availability"][pdate] = item.get("availability")
#                 metrics["free_cancel_avg"][pdate] = item.get("free_cancel_avg")
#                 metrics["price_avg"][pdate] = item.get("price_avg")
#         except Exception as e:
#             st.error(f"Error querying DynamoDB: {str(e)}")
#             return metrics

#     return metrics

def query_calendar_hotels(date_range, scraped_date_start, scraped_date_end):
    location = "tampere"
    time = "morning"
    persons = 2
    nights = 1

    if date_range:
        dates = date_range.split(' - ')
        day, month, year = dates[0].split('-')
        checkin_date = f"{year}-{month}-{day}"
    else:
        return []

    partition_key = f"{location}#{persons}#{nights}#{time}"

    all_items = []

    try:
        key_condition = (
            Key('location#persons#nights#time').eq(partition_key) &
            Key('checkin_date#scraped_date').between(
                f"{checkin_date}#{scraped_date_start}",
                f"{checkin_date}#{scraped_date_end}~"
            )
        )

        response = table.query(
            IndexName='hotel_prices_by_checkin_scraped',
            KeyConditionExpression=key_condition
        )

        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.query(
                IndexName='hotel_prices_by_checkin_scraped',
                KeyConditionExpression=key_condition,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])

        all_items.extend(items)

        transformed_items = []
        for item in all_items:
            transformed_items.append({
                'name': item.get('hotel_name', ''),
                'price': item.get('price', 0),
                'price_date': item.get('checkin_date', ''),
                'scrape_date': item.get('scraped_date', ''),
                'location': item.get('location', ''),
                'persons': item.get('persons', 0),
                'nights': item.get('nights', 0),
                'time': item.get('time', ''),
                'review_score': item.get('review_score', 0),
                'city': item.get('city', ''),
                'distance': item.get('distance', ''),
                'hotel_url': item.get('hotel_url', ''),
                'breakfast_included': item.get('breakfast_included', False),
                'free_cancellation': item.get('free_cancellation', False)
            })

        return transformed_items

    except Exception:
        return []

def query_calendar_data(price_start_date, price_end_date, zone_filter="zone1", location="tampere"):
    """Query data for calendar heatmap."""
    price_dates = []
    d = price_start_date
    while d <= price_end_date:
        price_dates.append(d.strftime("%Y-%m-%d"))  # YYYY-MM-DD (DB format)
        d += timedelta(days=1)
    
    time = "morning"
    persons = 2
    nights = 1
    partition_key = f"{location}#{persons}#{nights}#{time}"
    
    metrics = {
        'availability': {},
        'price_avg': {},
        'free_cancel_avg': {}
    }
    
    zone_mapping = {
        'zone1': ZONE1_HOTELS,
        'zone2': ZONE2_HOTELS,
        'zone3': ZONE3_HOTELS,
        'alert': Alert_Comparison
    }
    
    selected_zone = zone_mapping.get(zone_filter, ZONE1_HOTELS)
    
    for idx, pdate in enumerate(price_dates, 1):
        # For this price date, scrape window is 30 days before it
        price_dt = datetime.strptime(pdate, "%Y-%m-%d")
        scrape_start_dt = price_dt - timedelta(days=30)
        
        scraped_start = scrape_start_dt.strftime("%Y-%m-%d")
        scraped_end = pdate

        price_ddmmyyyy = price_dt.strftime("%d-%m-%Y")
        date_range_for_query = f"{price_ddmmyyyy} - {price_ddmmyyyy}"


        results = query_calendar_hotels(
            date_range=date_range_for_query,
            scraped_date_start=scraped_start,
            scraped_date_end=scraped_end
        )

        # Query for price averages (30-day scrape window)
        if not results:
            metrics['free_cancel_avg'][pdate] = 0
            metrics['price_avg'][pdate] = 0
            metrics['availability'][pdate] = 0
            continue

        df = pd.DataFrame(results)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df = df.dropna(subset=['price'])
        fc_df = df.copy()
        wo_df = df[(df['breakfast_included'] == False) & (df['free_cancellation'] == False)]

        fc_df = df[(df['breakfast_included'] == False) & (df['free_cancellation'] == True)]

        wo_unique_hotels = sorted(wo_df['name'].unique())
        fc_unique_hotels = sorted(fc_df['name'].unique())

        wo_available_zone1 = [hotel for hotel in selected_zone if hotel in wo_unique_hotels]
        fc_available_zone1 = [hotel for hotel in selected_zone if hotel in fc_unique_hotels]

        wo_df_zone1 = wo_df[wo_df['name'].isin(wo_available_zone1)]
        fc_df_zone1 = fc_df[fc_df['name'].isin(fc_available_zone1)]

        wo_avg = round(wo_df_zone1['price'].mean(),2) if not wo_df_zone1.empty else 0
        fc_avg = round(fc_df_zone1['price'].mean(),2) if not fc_df_zone1.empty else 0

        total_records = len(df)
        price_ddmmyyyy_avail = price_dt.strftime("%d-%m-%Y")
        date_range_for_avail = f"{price_ddmmyyyy_avail} - {price_ddmmyyyy_avail}"
        
        results_avail = query_calendar_hotels(
            date_range=date_range_for_avail,
            scraped_date_start=pdate,
            scraped_date_end=pdate
        )
        
        if results_avail:
            df_avail = pd.DataFrame(results_avail)
            df_avail['price'] = pd.to_numeric(df_avail['price'], errors='coerce')
            df_avail = df_avail.dropna(subset=['price'])
            df_avail = df_avail[(df_avail['breakfast_included'] == False)]
            unique_hotels_avail = sorted(df_avail['name'].unique())
            available_zone1 = [hotel for hotel in selected_zone if hotel in unique_hotels_avail]
        else:
            available_zone1 = []
        
        num_zone1_available = len(available_zone1)

        TOTAL_ZONE1 = len(selected_zone)
        zone1_avail_pct = round((num_zone1_available / TOTAL_ZONE1) * 100, 1) if TOTAL_ZONE1 > 0 else 0

        metrics['free_cancel_avg'][pdate] = fc_avg
        metrics['price_avg'][pdate] = wo_avg
        metrics['availability'][pdate] = zone1_avail_pct
    
    return metrics
    
def get_color_from_availability(value, min_val, max_val):
    """Generate color based on value (green for high, red for low)."""
    if max_val == min_val:
        normalized = 0.5
    else:
        normalized = (value - min_val) / (max_val - min_val)
    
    if normalized < 0.33:
        r, g, b = 255, 100, 100  # Red
    elif normalized < 0.67:
        r, g, b = 255, 200, 100  # Orange
    else:
        r, g, b = 100, 200, 100  # Green
    
    return f"rgb({int(r)}, {int(g)}, {int(b)})"

# ==================== PAGE NAVIGATION ====================
boards = st.session_state.get("boards", [])

tabs = [
    BOARD_LABELS[b]
    for b in AVAILABLE_BOARDS
    if b in boards
]

if st.session_state.get("access") == "admin":
    tabs.append("🛠️ Admin Panel")

created_tabs = st.tabs(tabs)

tab1 = tab2 = admin_panel = None
tab_index = 0

if "price_dashboard" in boards:
    tab1 = created_tabs[tab_index]
    tab_index += 1

if "historical_calendar" in boards:
    tab2 = created_tabs[tab_index]
    tab_index += 1

if st.session_state.get("access") == "admin":
    admin_panel = created_tabs[tab_index]

# Initialize color ranges in session state (do this near the top of your app)

# ==================== TAB 1: PRICE DASHBOARD ====================
if tab1:
    with tab1:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("""
            <div class="main-header">
                <h1>🏨 Hotel Booking Price Dashboard</h1>
                <p>Analyze hotel prices across different dates and locations</p>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
            <div class="user-info">
                <p><strong>👤 Logged in as:</strong><br>{st.session_state.get('authenticated_user', 'Unknown')}</p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("🚪 Logout", type="secondary", use_container_width=True):
                logout()

        with st.sidebar:
            st.markdown("### 🔧 Configuration")
            
            with st.expander("📍 Location & Booking Details", expanded=True):
                allowed_locations = st.session_state.get("locations", [])
                location = st.selectbox(
                    "Location",
                    allowed_locations,
                    index=0 if allowed_locations else None
                )
                persons = st.selectbox("Persons", [1, 2])
                nights = st.selectbox("Nights", [1,2,3, 7])
                time_of_day = st.selectbox("Time", ["morning", "evening"])
                breakfast_filter = st.checkbox("🍳 Include Breakfast Only", value=False)
                cancellation_filter = st.checkbox("✅ Free Cancellation", value=False)
            
            with st.expander("📅 Date Ranges", expanded=True):
                scraped_date_range = st.date_input(
                    "View Dates", 
                    value=[], 
                    key="scrape_dates_unique"
                )
                price_date_range = st.date_input(
                    "Stay Dates", 
                    value=[], 
                    key="price_dates_unique"
                )
            
            st.markdown("---")
            query_button = st.button("🚀 Execute Query", type="primary", use_container_width=True)

        if query_button:
            if len(scraped_date_range) != 2:
                st.error("⚠️ Please select both scrape dates")
                st.stop()
            if len(price_date_range) != 2:
                st.error("⚠️ Please select both stay dates")
                st.stop()

            scraped_start = scraped_date_range[0].strftime("%Y-%m-%d")
            scraped_end = scraped_date_range[1].strftime("%Y-%m-%d")
            
            try:
                price_start = price_date_range[0].strftime("%d-%m-%Y")
                price_end = price_date_range[1].strftime("%d-%m-%Y")
            except ValueError:
                price_start = price_date_range[0].strftime("%d-%m-%Y")
                price_end = price_date_range[1].strftime("%d-%m-%Y")

            with st.spinner("🔍 Searching hotels..."):
                filters = {
                    "location": location,
                    "time": time_of_day,
                    "persons": str(persons),
                    "nights": str(nights)
                }
                
                date_range = f"{price_start} - {price_end}"
                
                results = query_hotels(
                    filters=filters,
                    date_range=date_range,
                    scraped_date_start=scraped_start,
                    scraped_date_end=scraped_end
                )

            if results:
                st.session_state.results = results
                st.success(f"✅ Found {len(results)} hotel records!")
            else:
                st.error("❌ No data found for your criteria")

        if 'results' in st.session_state and st.session_state.results:
            df = pd.DataFrame(st.session_state.results)
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df.dropna(subset=['price'])

            if breakfast_filter and cancellation_filter:
                df = df[(df['breakfast_included'] == True) & (df['free_cancellation'] == True)]
                st.success(f"🍳✅ Filtered to {len(df)} records with breakfast included and free cancellation")
            elif breakfast_filter and not cancellation_filter:
                df = df[(df['breakfast_included'] == True) & (df['free_cancellation'] == False)]
                st.success(f"🍳 Filtered to {len(df)} records with breakfast included")
            elif cancellation_filter and not breakfast_filter:
                df = df[(df['breakfast_included'] == False) & (df['free_cancellation'] == True)]
                st.success(f"✅ Filtered to {len(df)} records with free cancellation")
            else:
                df = df[(df['breakfast_included'] == False) & (df['free_cancellation'] == False)]
            
            if not df.empty:
                st.markdown('<div class="hotel-selector">', unsafe_allow_html=True)

                col1, col2, col3 = st.columns([3, 1, 1])
                with col1:
                    st.markdown("### 🏨 Hotel Selection")
                    
                    unique_hotels = sorted(df['name'].unique())
                    
                    if 'selected_hotels' not in st.session_state:
                        st.session_state.selected_hotels = []
                    if 'multiselect_key' not in st.session_state:
                        st.session_state.multiselect_key = 0
                    
                    st.markdown("**Quick Selection:**")
                    col_bt1, col_bt2, col_bt3, col_bt4, col_bt5, col_bt6 = st.columns(6)
                    
                    with col_bt1:
                        if st.button("✅ Select All", key="select_all_btn", use_container_width=True):
                            st.session_state.selected_hotels = unique_hotels
                            st.session_state.multiselect_key += 1
                    
                    with col_bt2:
                        if st.button("🌍 Zone 1", key="select_zone1_btn", use_container_width=True):
                            available_zone1 = [hotel for hotel in ZONE1_HOTELS if hotel in unique_hotels]
                            st.session_state.selected_hotels = available_zone1
                            st.session_state.multiselect_key += 1
                    
                    with col_bt3:
                        if st.button("🏙️ Zone 2", key="select_zone2_btn", use_container_width=True):
                            available_zone2 = [hotel for hotel in ZONE2_HOTELS if hotel in unique_hotels]
                            st.session_state.selected_hotels = available_zone2
                            st.session_state.multiselect_key += 1
                    
                    with col_bt4:
                        if st.button("🚩 Zone 3", key="select_zone3_btn", use_container_width=True):
                            available_zone3 = [hotel for hotel in ZONE3_HOTELS if hotel in unique_hotels]
                            st.session_state.selected_hotels = available_zone3
                            st.session_state.multiselect_key += 1
                    
                    with col_bt5:
                        if st.button("🚨 Alerts", key="select_alert_comp_btn", use_container_width=True):
                            available_comp = [hotel for hotel in Alert_Comparison if hotel in unique_hotels]
                            st.session_state.selected_hotels = available_comp
                            st.session_state.multiselect_key += 1
                    with col_bt6:
                        if st.button("❌ Clear All", key="clear_all_btn", use_container_width=True):
                            st.session_state.selected_hotels = []
                            st.session_state.multiselect_key += 1
                    
                    default_hotels = [h for h in st.session_state.selected_hotels if h in unique_hotels]

                    hotels = st.multiselect(
                        "Select hotels to analyze:",
                        unique_hotels,
                        default=default_hotels,
                        key=f"hotel_selector_{st.session_state.multiselect_key}"
                    )
                    
                    st.session_state.selected_hotels = hotels

                with col2:
                    total_hotels = len(unique_hotels)
                    st.metric("Total Hotels", total_hotels)
                    
                    available_zone1 = len([h for h in ZONE1_HOTELS if h in unique_hotels])
                    if available_zone1 > 0:
                        st.metric("🌍 Zone 1 Available", available_zone1)

                    available_zone2 = len([h for h in ZONE2_HOTELS if h in unique_hotels])
                    if available_zone2 > 0:
                        st.metric("🏙️ Zone 2 Available", available_zone2)

                    available_zone3 = len([h for h in ZONE3_HOTELS if h in unique_hotels])
                    if available_zone3 > 0:
                        st.metric("🚩 Zone 3 Available", available_zone3)
                    
                    available_comp = len([h for h in Alert_Comparison if h in unique_hotels])
                    if available_comp > 0:
                        st.metric("🚨Alert Comparison hotels Available", available_comp)

                with col3:
                    if hotels:
                        selected_count = len(hotels)
                        st.metric("Selected", selected_count)
                        
                        zone1_selected = len([h for h in hotels if h in ZONE1_HOTELS])
                        if zone1_selected > 0:
                            st.metric("🌍 Zone 1 Selected", zone1_selected)

                        zone2_selected = len([h for h in hotels if h in ZONE2_HOTELS])
                        if zone2_selected > 0:
                            st.metric("🏙️ Zone 2 Selected", zone2_selected)

                        zone3_selected = len([h for h in hotels if h in ZONE3_HOTELS])
                        if zone3_selected > 0:
                            st.metric("🚩 Zone 3 Selected", zone3_selected)
                        
                        comp_selected = len([h for h in hotels if h in Alert_Comparison])
                        if comp_selected > 0:
                            st.metric("🚨Alert Comparison hotels Selected", comp_selected)

                st.markdown('</div>', unsafe_allow_html=True)

                st.markdown('<div class="hotel-selector">', unsafe_allow_html=True)
                st.markdown("### 📈 Line Chart Hotel Selection (Optional)")

                if 'line_hotels' not in st.session_state:
                    st.session_state.line_hotels = []

                all_hotels = sorted(df['name'].unique())
                valid_defaults = [h for h in st.session_state.get('line_hotels', []) if h in all_hotels]

                line_hotels = st.multiselect(
                    "Select hotels for line chart trend:",
                    options=all_hotels,
                    default=valid_defaults,
                    key="line_chart_selector"
                )
                st.session_state.line_hotels = line_hotels
                st.markdown('</div>', unsafe_allow_html=True)

                if hotels:
                    filtered_df = df[df['name'].isin(hotels)]
                    filtered_df['price_date'] = pd.to_datetime(filtered_df['price_date'], format='%Y-%m-%d')

                    # Bar chart for main selection
                    bar_avg = filtered_df.groupby('price_date')['price'].mean().reset_index()
                    bar_avg['date_label'] = bar_avg['price_date'].dt.strftime('%b %d')
                    
                    bar_avg['day_name'] = bar_avg['price_date'].dt.strftime('%a')
                    bar_avg['week_num'] = bar_avg['price_date'].dt.isocalendar().week
                    # Create combined label with day and week on separate lines
                    bar_avg['x_label'] = bar_avg['date_label'] + '<br>' + bar_avg['day_name'] + '<br>Week ' + bar_avg['week_num'].astype(str)

                    if hotels and line_hotels:
                        # Line over bar chart
                        line_df = df[df['name'].isin(line_hotels)].copy()
                        line_df['price'] = pd.to_numeric(line_df['price'], errors='coerce')

                        # Pivot table
                        pivot_line = line_df.pivot_table(
                            index='scrape_date',
                            columns='price_date',
                            values='price',
                            aggfunc='mean'
                        )

                        # Get mean across scrape dates
                        line_avg = pivot_line.mean(axis=0).reset_index()
                        line_avg.columns = ['price_date', 'price']
                        line_avg = line_avg.dropna(subset=['price'])

                        # Convert to datetime
                        line_avg['price_date'] = pd.to_datetime(line_avg['price_date'])

                        # Create x_label for line data
                        line_avg['date_label'] = line_avg['price_date'].dt.strftime('%b %d')
                        line_avg['day_name'] = line_avg['price_date'].dt.strftime('%a')
                        line_avg['week_num'] = line_avg['price_date'].dt.isocalendar().week
                        line_avg['x_label'] = line_avg['date_label'] + '<br>' + line_avg['day_name'] + '<br>Week ' + line_avg['week_num'].astype(str)

                        # Bar trace
                        fig = px.bar(
                            bar_avg, x='x_label', y='price',
                            color_discrete_sequence=['#7dd3c0'], text='price',
                            labels={'price': 'Average Price (€)', 'x_label': 'Date'},
                            title='Average Prices with Trend Line'
                        )

                        fig.data[0].texttemplate = '€%{text:.1f}'
                        fig.data[0].textposition = 'outside'

                        # Scatter trace - match line data to bar data by price_date value
                        # For each line point, find the matching bar x_label
                        for idx, line_row in line_avg.iterrows():
                            matching_bar = bar_avg[bar_avg['price_date'] == line_row['price_date']]
                            if not matching_bar.empty:
                                x_pos = matching_bar.iloc[0]['x_label']
                                fig.add_scatter(
                                    x=[x_pos],
                                    y=[line_row['price']],
                                    mode='markers',
                                    name='Trend',
                                    marker=dict(color='red', size=14),
                                    showlegend=(idx == 0)  # Only show legend for first point
                                )

                        fig.update_layout(
                            height=500, 
                            xaxis_title="Date", 
                            yaxis_title="Average Price (€)",
                            xaxis=dict(tickangle=0)
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    else:
                        # Only bar chart if no line hotels selected - UPDATED with x_label
                        fig_bar = px.bar(
                            bar_avg, x='x_label', y='price',
                            title='Average Prices Across Selected Hotels',
                            color_discrete_sequence=['#7dd3c0'], text='price'
                        )
                        fig_bar.update_traces(texttemplate='€%{text:.1f}', textposition='outside')
                        fig_bar.update_layout(
                            height=500, 
                            xaxis_title="Date", 
                            yaxis_title="Average Price (€)",
                            xaxis=dict(tickangle=0)
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)
                    
                    # Detailed table section
                    st.markdown("### 📋 Detailed Price Matrix")

                    # Create pivot
                    pivot = filtered_df.pivot_table(
                        index=['scrape_date', 'name'], 
                        columns='price_date',
                        values='price', 
                        aggfunc='mean'
                    )

                    # Format column names
                    pivot.columns = [col.strftime('%Y-%m-%d') if isinstance(col, pd.Timestamp) else col for col in pivot.columns]
                    pivot = pivot.reset_index()

                    numeric_cols = [col for col in pivot.columns if col not in ['scrape_date', 'name', 'breakfast_included']]

                    # Add group separators (use None for numeric/boolean)
                    unique_dates = pivot['scrape_date'].unique()
                    if len(unique_dates) > 1:
                        final_pivot = pd.DataFrame()
                        for i, date in enumerate(unique_dates):
                            group = pivot[pivot['scrape_date'] == date]
                            final_pivot = pd.concat([final_pivot, group], ignore_index=True)
                            if i < len(unique_dates) - 1:
                                empty_row = {col: None for col in pivot.columns}
                                empty_df = pd.DataFrame([empty_row])
                                final_pivot = pd.concat([final_pivot, empty_df], ignore_index=True)
                        pivot = final_pivot

                    # Add average row
                    if numeric_cols:
                        valid_data = pivot[pivot['scrape_date'].notnull()]
                        averages = valid_data[numeric_cols].mean()

                        empty_row = {col: None for col in pivot.columns}
                        empty_df = pd.DataFrame([empty_row])
                        pivot = pd.concat([pivot, empty_df], ignore_index=True)

                        avg_row = {'scrape_date': 'AVERAGE', 'name': 'AVERAGE'}

                        for col in numeric_cols:
                            if not pd.isna(averages[col]):
                                avg_row[col] = round(averages[col], 2)  # round to 2 decimals
                            else:
                                avg_row[col] = None
                        avg_df = pd.DataFrame([avg_row])
                        pivot = pd.concat([pivot, avg_df], ignore_index=True)
                    
                    # pivot['breakfast_included'] = pivot['breakfast_included'].apply(lambda x: 'Yes' if x else 'No' if x is not None else '')

                    # Build AgGrid
                    gb = GridOptionsBuilder.from_dataframe(pivot)

                    # Pin first 3 columns
                    gb.configure_columns(['scrape_date', 'name'], pinned='left', minWidth=150)

                    # Numeric columns - same width, 2 decimal precision
                    gb.configure_columns(
                        numeric_cols,
                        type=['numericColumn'],
                        precision=2,
                        minWidth=100,
                        maxWidth=100
                    )


                    # Make columns resizable
                    gb.configure_default_column(resizable=True)

                    # Optional: set row height
                    gb.configure_grid_options(rowHeight=35)

                    gridOptions = gb.build()

                    AgGrid(
                        pivot,
                        gridOptions=gridOptions,
                        height=600,
                        fit_columns_on_grid_load=False,  # important: don't shrink
                        enable_enterprise_modules=False,
                    )
                
                else:
                    st.info("👆 Please select one or more hotels to view the analysis")
            
            else:
                st.error("❌ No valid price data found")

        else:
            # Welcome screen
            st.markdown("""
            <div style="text-align: center; padding: 3rem; background: #f8f9fa; border-radius: 15px; margin: 2rem 0;">
                <h2>🎯 Welcome to Hotel Price Analytics</h2>
                <p style="font-size: 1.2em; color: #666; margin-bottom: 2rem;">
                    Configure your search parameters in the sidebar and click "Execute Query" to begin analysis
                </p>
            </div>
            """, unsafe_allow_html=True)

if tab2:
# ==================== TAB 2: CALENDAR HEATMAP ====================
    with tab2:
        st.markdown("""
        <div class="main-header">
            <h1>📅 Weekly Calendar Heatmap</h1>
            <p>Hotel availability and pricing trends by week</p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.sidebar:
            st.markdown("### 📅 Calendar Configuration")
            
            with st.expander("📍 Zone Selection", expanded=True):
                zone_selection = st.selectbox(
                    "Select Zone",
                    ["zone1", "zone2", "zone3", "alert"],
                    format_func=lambda x: {"zone1": "🌍 Zone 1", "zone2": "🏙️ Zone 2", "zone3": "🚩 Zone 3", "alert": "🚨 Alert Comparison"}.get(x),
                    key="zone_selection_key"
                )
            
            with st.expander("📅 Date Range", expanded=True):
                calendar_start = st.date_input("Start Date", value=datetime(2025, 12, 1), key="cal_start_key")
                calendar_end = st.date_input("End Date", value=datetime(2025, 12, 15), key="cal_end_key")

            with st.expander("📍 Location Selection", expanded=True):
                allowed_locations = st.session_state.get("locations", [])
                allowed_locations = ["tampere"] if "tampere" in allowed_locations else [] # limit to only tampere for now
                calendar_location = st.selectbox(
                    "Select Location",
                    allowed_locations,
                    index=0 if allowed_locations else None,
                    key="calendar_location_key"
                )
            
            # ========== ADD THIS: COLOR PICKER SECTION ==========
            with st.expander("🎨 Colour Setup", expanded=False):
                st.markdown("##### Configure Price Range Colors")
                
                current_zone = zone_selection
                if 'all_color_ranges' not in st.session_state:
                    # Try to load from storage, otherwise use defaults
                    st.session_state.all_color_ranges = get_default_color_ranges()
                
                # Initialize color_ranges for current zone if it doesn't exist
                if current_zone not in st.session_state.all_color_ranges:
                    st.session_state.all_color_ranges[current_zone] = get_default_color_ranges()[current_zone]
                
                # Reference the current zone's color ranges
                st.session_state.color_ranges = st.session_state.all_color_ranges[current_zone]

                # Display and edit color ranges
                for idx, color_range in enumerate(st.session_state.color_ranges):
                    col1, col2, col3, col4 = st.columns([1.5, 1.5, 1.5, 0.5])
                    
                    with col1:
                        color_range['min'] = st.number_input(
                            f"Min Price {idx+1}", 
                            value=float(color_range['min']),
                            min_value=0.0,
                            step=0.01,
                            key=f"min_{idx}_{current_zone}"
                        )
                    
                    with col2:
                        color_range['max'] = st.number_input(
                            f"Max Price {idx+1}", 
                            value=float(color_range['max']),
                            min_value=0.0,
                            step=0.01,
                            key=f"max_{idx}_{current_zone}"
                        )
                    
                    with col3:
                        color_range['color'] = st.color_picker(
                            f"Colour {idx+1}", 
                            value=color_range['color'],
                            key=f"color_{idx}_{current_zone}"
                        )
                    
                    with col4:
                        if st.button("✕", key=f"remove_{idx}_{current_zone}", use_container_width=True):
                            st.session_state.color_ranges.pop(idx)
                            st.session_state.all_color_ranges[current_zone] = st.session_state.color_ranges
                            st.rerun()

                
                # Update all_color_ranges with current changes
                st.session_state.all_color_ranges[current_zone] = st.session_state.color_ranges
                
                col_reset, col_add = st.columns(2)
                
                # Reset to defaults button
                with col_reset:
                    if st.button("🔄 Reset to Defaults", use_container_width=True, key=f"reset_defaults_btn_{current_zone}"):
                        st.session_state.all_color_ranges[current_zone] = get_default_color_ranges()[current_zone]
                        st.session_state.color_ranges = st.session_state.all_color_ranges[current_zone]
                        st.rerun()
                
                # Add new color range button
                with col_add:
                    if st.button("➕ Add Colour Range", use_container_width=True, key=f"add_range_btn_{current_zone}"):
                        new_range = {
                            'min': 0.0,
                            'max': 100.0,
                            'color': '#667eea'
                        }
                        st.session_state.color_ranges.append(new_range)
                        st.session_state.all_color_ranges[current_zone] = st.session_state.color_ranges
                        st.rerun()
            # ========== END COLOR SETUP SECTION ==========
                
            st.markdown("---")
            calendar_query_button = st.button("🔄 Load Calendar Data", type="primary", use_container_width=True)

        # Load data only when button is clicked or data hasn't been loaded for current date range
        if calendar_query_button:
            with st.spinner("📊 Generating calendar data..."):
                st.session_state.calendar_data = query_calendar_data(
                    price_start_date=calendar_start,
                    price_end_date=calendar_end,
                    zone_filter=zone_selection,
                    location=calendar_location
                )
                st.session_state.calendar_date_range = (calendar_start, calendar_end)
                st.session_state.calendar_zone = zone_selection
                st.session_state.calendar_location = calendar_location
        
        if 'calendar_data' in st.session_state:
            metrics = st.session_state.calendar_data
            
            # Initialize with all three metrics
            df_cal_availability = pd.DataFrame({
                'date': list(metrics['availability'].keys()),
                'value': list(metrics['availability'].values())
            })
            
            df_cal_price = pd.DataFrame({
                'date': list(metrics['price_avg'].keys()),
                'value': list(metrics['price_avg'].values())
            })
            
            df_cal_free_cancel = pd.DataFrame({
                'date': list(metrics['free_cancel_avg'].keys()),
                'value': list(metrics['free_cancel_avg'].values())
            })
            
            # Process all data with common date fields
            for df in [df_cal_availability, df_cal_price, df_cal_free_cancel]:
                df['date'] = pd.to_datetime(df['date'])
                df['year'] = df['date'].dt.year
                df['week'] = df['date'].dt.isocalendar().week
                df['day_name'] = df['date'].dt.strftime('%A')
                df['date_str'] = df['date'].dt.strftime('%m/%d/%Y')
                df['day_num'] = df['date'].dt.dayofweek
            
            # Get available years and weeks from ALL data
            all_years = sorted(set(
                list(df_cal_availability['year'].unique()) + 
                list(df_cal_price['year'].unique()) + 
                list(df_cal_free_cancel['year'].unique())
            ))
            all_weeks = sorted(set(
                list(df_cal_availability['week'].unique()) + 
                list(df_cal_price['week'].unique()) + 
                list(df_cal_free_cancel['week'].unique())
            ))
            
            # Display metric selection (no query triggered)
            with st.expander("📊 Display Metric", expanded=True):
                color_metric = st.selectbox(
                    "Display Metric By",
                    ["availability", "price_avg", "free_cancel_avg"],
                    format_func=lambda x: {"availability": "Hotel Availability %", "price_avg": "Average Price (€)", "free_cancel_avg": "Free Cancellation Avg (€)"}.get(x),
                    key="color_metric_key"
                )
            
            # Filter selections (no query triggered)
            col1, col2 = st.columns(2)
            with col1:
                selected_years = st.multiselect(
                    "Select Years",
                    all_years,
                    default=all_years,
                    key="selected_years_key"
                )
            
            with col2:
                selected_weeks = st.multiselect(
                    "Select Weeks",
                    all_weeks,
                    default=all_weeks,
                    key="selected_weeks_key"
                )
            if not selected_years or not selected_weeks:
                st.error("Please select at least one year and one week")
            else:
                # Select the appropriate dataframe based on metric for DISPLAY
                metric_map = {
                    'availability': df_cal_availability,
                    'price_avg': df_cal_price,
                    'free_cancel_avg': df_cal_free_cancel
                }
                df_cal_display = metric_map[color_metric].copy()
                
                # Filter by selected years and weeks
                filtered_cal_display = df_cal_display[
                    (df_cal_display['year'].isin(selected_years)) & 
                    (df_cal_display['week'].isin(selected_weeks))
                ].copy()
                
                if filtered_cal_display.empty:
                    st.warning("No data available for selected filters")
                else:
                    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    
                    html = '<table class="calendar-table"><tr>'
                    html += '<td class="week-label"></td>'
                    for day in day_order:
                        html += f'<td class="day-label">{day}</td>'
                    html += '</tr>'
                    
                    week_year_pairs = []
                    for week in sorted(selected_weeks):
                        for year in sorted(selected_years):
                            week_year_pairs.append((week, year))
                    
                    for week, year in week_year_pairs:
                        week_data_display = filtered_cal_display[
                            (filtered_cal_display['week'] == week) & 
                            (filtered_cal_display['year'] == year)
                        ].copy()
                        
                        html += '<tr>'
                        html += f'<td class="week-label">Week {week} - {year}</td>'
                        
                        for day in day_order:
                            day_data_display = week_data_display[week_data_display['day_name'] == day]
                            
                            if not day_data_display.empty:
                                row_display = day_data_display.iloc[0]
                                display_value = row_display['value']
                                date_str = datetime.strptime(row_display['date_str'], "%m/%d/%Y").strftime("%d/%m/%Y")
                                
                                # Get background color
                                bg_color = get_color_from_price_ranges(display_value, st.session_state.color_ranges)
                                
                                # Get text color based on background brightness
                                text_color = get_text_color_from_background(bg_color)
                                
                                if color_metric == "availability":
                                    display_text = f"{display_value:.1f}%"
                                else:
                                    display_text = f"€{display_value:.2f}"
                                
                                html += f'''<td style="background-color: {bg_color};">
                                    <div class="cell-content" style="color: {text_color};">
                                        <div class="cell-date">{date_str}</div>
                                        <div class="cell-value">{display_text}</div>
                                    </div>
                                </td>'''
                            else:
                                html += '<td class="empty-cell"></td>'
                        
                        html += '</tr>'
                    
                    html += '</table>'
                    st.markdown(html, unsafe_allow_html=True)
                    
                    
                    st.markdown("---")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.info(f"**Zone:** {zone_selection.replace('_', ' ').title()}")
                    with col2:
                        st.info(f"**Display:** {color_metric.replace('_', ' ').title()}")
                    with col3:
                        st.info(f"**Location:** {st.session_state.calendar_location.title()}")
                    with col4:
                        st.info(f"**Color Range (Availability):** {filtered_cal_display['value'].min():.1f} - {filtered_cal_display['value'].max():.1f}")
        
        else:
            st.info("👈 Configure settings and click 'Load Calendar Data' to begin")

if admin_panel:
    with admin_panel:

        # Fetch all users
        try:
            users = table_user.scan().get("Items", [])
        except Exception as e:
            st.error(f"Failed to load users: {e}")
            users = []

                
        st.markdown("## 📥 Download Login Logs")
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            log_start = st.date_input("Start Date", key="log_start")

        with col2:
            log_end = st.date_input("End Date", key="log_end")

        with col3:
            download_logs = st.button("⬇️ Download Excel", use_container_width=True)

        if download_logs:
            if log_start > log_end:
                st.error("Start date must be before end date")
            else:
                with st.spinner("Preparing login logs..."):
                    try:
                        response = table_logs.scan(
                            FilterExpression=Attr("login_date").between(
                                log_start.strftime("%Y-%m-%d"),
                                log_end.strftime("%Y-%m-%d")
                            )
                        )

                        items = response.get("Items", [])

                        while "LastEvaluatedKey" in response:
                            response = table_logs.scan(
                                FilterExpression=Attr("login_date").between(
                                    log_start.strftime("%Y-%m-%d"),
                                    log_end.strftime("%Y-%m-%d")
                                ),
                                ExclusiveStartKey=response["LastEvaluatedKey"]
                            )
                            items.extend(response.get("Items", []))

                        if not items:
                            st.warning("No login logs found for selected range")
                        else:
                            df_logs = pd.DataFrame(items).sort_values(
                                "login_ts", ascending=False
                            )

                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                                df_logs.to_excel(
                                    writer,
                                    index=False,
                                    sheet_name="Login Logs"
                                )

                            st.download_button(
                                label="📥 Download Login Logs",
                                data=buffer.getvalue(),
                                file_name=f"login_logs_{log_start}_{log_end}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )

                    except Exception as e:
                        st.error(f"Failed to generate logs: {e}")

        # ================= ADD STAFF ACCOUNT =================
        st.divider()
        st.markdown("### ➕ Add Member Account")

        with st.form("add_staff_form"):
            new_username = st.text_input("User name")
            new_password = st.text_input("Password", type="password")

            new_locations = st.multiselect(
                "Locations",
                ["tampere", "oulu", "rauma", "turku", "jyvaskyla"]
            )
            new_boards = st.multiselect(
                "Boards Access",
                AVAILABLE_BOARDS,
                format_func=lambda x: {
                    "price_dashboard": "📊 Price Dashboard",
                    "historical_calendar": "📅 Historical Price Calendar"
                }.get(x, x)
            )
            create_btn = st.form_submit_button("Create account")

        if create_btn:
            if not new_username or not new_password:
                st.error("Username and password are required")
            else:
                try:
                    existing = table_user.get_item(
                        Key={"username": new_username}
                    )

                    if "Item" in existing:
                        st.error("User already exists")
                    else:
                        table_user.put_item(
                            Item={
                                "username": new_username,
                                "password": new_password,
                                "access": "basic",
                                "last_login": "",
                                "locations": new_locations,
                                "boards": new_boards
                            }
                        )
                        st.success("Member account created")
                        st.rerun()

                except Exception as e:
                    st.error(f"Failed to create user: {e}")

        # ================= EXISTING USERS =================
        st.divider()
        st.markdown("## 👥 Member Accounts")

        for user in users:
            if user.get("access") == "admin":
                continue  # skip admin accounts
            col1, col2, col3, col4, col5, col6, col7 = st.columns([2.5, 2.5, 2.5, 2.5, 2.5, 1.5, 1.5])

            with col1:
                st.text_input(
                    "User name",
                    value=user["username"],
                    disabled=True,
                    key=f"username_{user['username']}"
                )

            with col2:
                new_password = st.text_input(
                    "Password",
                    value=user.get("password", ""),
                    key=f"password_{user['username']}"
                )

            with col3:
                st.text_input(
                    "Last log in",
                    value=user.get("last_login", ""),
                    disabled=True,
                    key=f"last_login_{user['username']}"
                )
            
            with col4:
                edit_locations = st.multiselect(
                    "Locations",
                    ["tampere", "oulu", "rauma", "turku", "jyvaskyla"],
                    default=user.get("locations", []),
                    key=f"locations_{user['username']}"
                )

            with col5:
                edit_boards = st.multiselect(
                    "Boards",
                    AVAILABLE_BOARDS,
                    default=user.get("boards", []),
                    format_func=lambda x: {
                        "price_dashboard": "📊 Price Dashboard",
                        "historical_calendar": "📅 Historical Price Calendar"
                    }.get(x, x),
                    key=f"boards_{user['username']}"
                )

            with col6:
                if st.button("💾 Save", key=f"save_{user['username']}"):
                    try:
                        table_user.update_item(
                            Key={"username": user["username"]},
                            UpdateExpression="SET password = :p, locations = :l, boards = :b",
                            ExpressionAttributeValues={
                                ":p": new_password,
                                ":l": edit_locations,
                                ":b": edit_boards
                            }
                        )
                        st.success(f"Updated {user['username']}")
                    except Exception as e:
                        st.error(f"Failed to update user: {e}")

            with col7:
                if st.button("🗑️ Delete", key=f"delete_{user['username']}", use_container_width=True):
                    # Show confirmation dialog
                    st.session_state[f"delete_confirm_{user['username']}"] = True

            # Confirmation dialog
            if st.session_state.get(f"delete_confirm_{user['username']}", False):
                st.warning(f"⚠️ Are you sure you want to delete user **{user['username']}**? This action cannot be undone.")
                col_confirm1, col_confirm2, col_confirm3 = st.columns([1, 1, 2])
                
                with col_confirm1:
                    if st.button("✅ Yes, Delete", key=f"confirm_delete_{user['username']}", use_container_width=True):
                        try:
                            table_user.delete_item(
                                Key={"username": user["username"]}
                            )
                            st.success(f"User **{user['username']}** has been deleted")
                            st.session_state[f"delete_confirm_{user['username']}"] = False
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete user: {e}")
                            st.session_state[f"delete_confirm_{user['username']}"] = False
                
                with col_confirm2:
                    if st.button("❌ Cancel", key=f"cancel_delete_{user['username']}", use_container_width=True):
                        st.session_state[f"delete_confirm_{user['username']}"] = False
                        st.rerun()
                
                st.divider()
