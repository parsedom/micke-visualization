import streamlit as st
import boto3
import pandas as pd
from datetime import datetime
import json
import plotly.express as px
from boto3.dynamodb.conditions import Key,Attr
import os
import hashlib
import hmac
from st_aggrid import AgGrid, GridOptionsBuilder

# Configure page - must be first Streamlit command
st.set_page_config(
    page_title="Hotel Booking Dashboard",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Authentication configuration
USERS = {
    "micke": "micke@vis",  
}

def check_password():
    """Returns `True` if the user had the correct password."""
    
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state["password"], USERS.get(st.session_state["username"], "")):
            st.session_state["password_correct"] = True
            st.session_state["authenticated_user"] = st.session_state["username"]
            del st.session_state["password"]  # Don't store password
            del st.session_state["username"]  # Don't store username in session state
        else:
            st.session_state["password_correct"] = False

    # Return True if password is validated
    if st.session_state.get("password_correct", False):
        return True

    # Show login form
    st.markdown("""
    <div style="max-width: 400px; margin: 5rem auto; padding: 2rem; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.3);">
        <h1 style="color: white; text-align: center; margin-bottom: 2rem;">
            🏨 Hotel Dashboard Login
        </h1>
    </div>
    """, unsafe_allow_html=True)

    # Login form in centered container
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.container():
            st.markdown("""
            <div style="background: white; padding: 2rem; border-radius: 10px; 
                        box-shadow: 0 5px 15px rgba(0,0,0,0.1); margin-top: 2rem;">
            """, unsafe_allow_html=True)
            
            st.markdown("### 🔐 Please Login")
            st.text_input("Username", key="username", placeholder="Enter your username")
            st.text_input("Password", type="password", key="password", placeholder="Enter your password")
            st.button("Login", on_click=password_entered, type="primary", use_container_width=True)
            
            if "password_correct" in st.session_state and not st.session_state["password_correct"]:
                st.error("😞 User not known or password incorrect")
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Add some info about the dashboard
            st.markdown("""
            <div style="text-align: center; margin-top: 2rem; color: #666;">
                <p><strong>Hotel Price Analytics Dashboard</strong></p>
                <p>Analyze hotel prices across different dates and locations</p>
                <p><small>Contact your administrator for login credentials</small></p>
            </div>
            """, unsafe_allow_html=True)

    return False

def logout():
    """Clear session state for logout"""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# Check authentication
if not check_password():
    st.stop()

# Hotel zone definitions
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
    "Lillan Hotel & Kök",
    "Hotelli Vaakko - Hotel and Apartments by UHNDA",
    "Hotel Kauppi",
    "Holiday Inn Tampere - Central Station by IHG",
    "Holiday Club Tampereen Kehräämö",
    "H28 - Hotel, Apartments and Suites by UHNDA",
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

def query_hotels(filters, date_range, scraped_date_start, scraped_date_end):
    """
    Query DynamoDB for hotel prices based on filters and date ranges.
    """
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
    
    # Build filter expression for checkin date range if provided
    filter_expression = None
    if checkin_start and checkin_end:
        filter_expression = Attr('checkin_date').between(checkin_start, checkin_end)
    
    all_items = []
    
    try:
        # Use between condition for the sort key range
        # The '~' character is a good choice as it's ASCII value is higher than '#'
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
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=key_condition,
                FilterExpression=filter_expression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
        
        all_items.extend(items)
        
        # Transform the items
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
                'breakfast_included': item.get('breakfast_included', False)
            })
        
        return transformed_items
    
    except Exception as e:
        st.error(f"Error querying DynamoDB: {str(e)}")
        return []

# Styling
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
</style>
""", unsafe_allow_html=True)

# Header with user info and logout
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

# Sidebar
with st.sidebar:
    st.markdown("### 🔧 Configuration")
    
    with st.expander("📍 Location & Booking Details", expanded=True):
        location = st.selectbox("Location", ["tampere", "oulu","rauma","turku","jyvaskyla"], index=0)
        persons = st.selectbox("Persons", [1, 2])
        nights = st.selectbox("Nights", [1,2,3, 7])
        time_of_day = st.selectbox("Time", ["evening", "morning"])
        breakfast_filter = st.checkbox("🍳 Include Breakfast Only", value=False)
    
    with st.expander("📅 Date Ranges", expanded=True):
        # Use unique keys for each date input to prevent caching issues
        scraped_date_range = st.date_input(
            "Scrape Dates", 
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

# Query execution
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

# Main dashboard
if 'results' in st.session_state and st.session_state.results:
    df = pd.DataFrame(st.session_state.results)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df.dropna(subset=['price'])

    if breakfast_filter:
        df = df[df['breakfast_included'] == True]
        st.success(f"🍳 Filtered to {len(df)} records with breakfast included")
    
    if not df.empty:
        # Hotel selection section
        st.markdown('<div class="hotel-selector">', unsafe_allow_html=True)

        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            st.markdown("### 🏨 Hotel Selection")
            
            # Get unique hotel names from current data
            unique_hotels = sorted(df['name'].unique())
            
            # Initialize session state if it doesn't exist
            if 'selected_hotels' not in st.session_state:
                st.session_state.selected_hotels = []
            if 'multiselect_key' not in st.session_state:
                st.session_state.multiselect_key = 0
            
            # Create quick selection buttons
            st.markdown("**Quick Selection:**")
            col_bt1, col_bt2, col_bt3, col_bt4, col_bt5,col_bt6 = st.columns(6)
            
            # Button actions - update session state and increment key
            with col_bt1:
                if st.button("✅ Select All", key="select_all_btn", use_container_width=True, help="Select all available hotels"):
                    st.session_state.selected_hotels = unique_hotels
                    st.session_state.multiselect_key += 1
            
            with col_bt2:
                if st.button("🌍 Zone 1", key="select_zone1_btn", use_container_width=True, help="Select only Zone 1 hotels"):
                    available_zone1 = [hotel for hotel in ZONE1_HOTELS if hotel in unique_hotels]
                    st.session_state.selected_hotels = available_zone1
                    st.session_state.multiselect_key += 1
            
            with col_bt3:
                if st.button("🏙️ Zone 2", key="select_zone2_btn", use_container_width=True, help="Select only Zone 2 hotels"):
                    available_zone2 = [hotel for hotel in ZONE2_HOTELS if hotel in unique_hotels]
                    st.session_state.selected_hotels = available_zone2
                    st.session_state.multiselect_key += 1
            
            with col_bt4:
                if st.button("🚩 Zone 3", key="select_zone3_btn", use_container_width=True, help="Select only Zone 3 hotels"):
                    available_zone3 = [hotel for hotel in ZONE3_HOTELS if hotel in unique_hotels]
                    st.session_state.selected_hotels = available_zone3
                    st.session_state.multiselect_key += 1
            
            with col_bt5:
                if st.button("🚨Alerts", key="select_alert_comp_btn", use_container_width=True, help="Select only alert comparision hotels"):
                    available_comp = [hotel for hotel in Alert_Comparison if hotel in unique_hotels]
                    st.session_state.selected_hotels = available_comp
                    st.session_state.multiselect_key += 1
            with col_bt6:
                if st.button("❌ Clear All", key="clear_all_btn", use_container_width=True, help="Clear all selections"):
                    st.session_state.selected_hotels = []
                    st.session_state.multiselect_key += 1
            
            # Create the hotel selection dropdown with dynamic key
            default_hotels = [h for h in st.session_state.selected_hotels if h in unique_hotels]

            hotels = st.multiselect(
                "Select hotels to analyze:",
                unique_hotels,
                default=default_hotels,
                key=f"hotel_selector_{st.session_state.multiselect_key}"
            )
            
            # Update session state when user manually changes selection
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
                st.metric("🚨Alert Comparison hotels Available", available_comp)

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
                    st.metric("🚨Alert Comparison hotels Selected", comp_selected)

        st.markdown('</div>', unsafe_allow_html=True)

        # Line Chart Hotel Selector
        st.markdown('<div class="hotel-selector">', unsafe_allow_html=True)
        st.markdown("### 📈 Line Chart Hotel Selection (Optional)")

        if 'line_hotels' not in st.session_state:
            st.session_state.line_hotels = []

        all_hotels = sorted(df['name'].unique())  # or a master list
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

            if hotels and line_hotels:
                # Line over bar chart
                line_df = df[df['name'].isin(line_hotels)].copy()
                line_df['price'] = pd.to_numeric(line_df['price'], errors='coerce')

                # Pivot table like your detailed matrix
                pivot_line = line_df.pivot_table(
                    index='scrape_date',
                    columns='price_date',
                    values='price',
                    aggfunc='mean'
                )

                # If you want a single line chart across all scrape dates, take the mean across scrapes
                line_avg = pivot_line.mean(axis=0).reset_index()
                line_avg.columns = ['price_date', 'price']

                # Format date labels
                line_avg['date_label'] = pd.to_datetime(line_avg['price_date']).dt.strftime('%b %d')

                # Bar trace
                fig = px.bar(
                    bar_avg, x='date_label', y='price',
                    color_discrete_sequence=['#7dd3c0'], text='price',
                    labels={'price': 'Average Price (€)', 'date_label': 'Date'},
                    title='Average Prices with Trend Line'
                )

                # Update only the bar trace for text
                fig.data[0].texttemplate = '€%{text:.1f}'
                fig.data[0].textposition = 'outside'

                # Line/Scatter trace
                fig.add_scatter(
                    x=line_avg['date_label'], y=line_avg['price'],
                    mode='lines+markers',
                    name='Trend',
                    line=dict(color='red', width=3)
                )

                fig.update_layout(height=500, xaxis_title="Date", yaxis_title="Average Price (€)")
                st.plotly_chart(fig, use_container_width=True)

            else:
                # Only bar chart if no line hotels selected
                fig_bar = px.bar(
                    bar_avg, x='date_label', y='price',
                    title='Average Prices Across Selected Hotels',
                    color_discrete_sequence=['#7dd3c0'], text='price'
                )
                fig_bar.update_traces(texttemplate='€%{text:.1f}', textposition='outside')
                fig_bar.update_layout(height=500, xaxis_title="Date", yaxis_title="Average Price (€)")
                st.plotly_chart(fig_bar, use_container_width=True)
            
            # Detailed table section
            st.markdown("### 📋 Detailed Price Matrix")

            # Create pivot
            pivot = filtered_df.pivot_table(
                index=['scrape_date', 'name', 'breakfast_included'], 
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

                avg_row = {'scrape_date': 'AVERAGE', 'name': 'AVERAGE', 'breakfast_included': None}

                for col in numeric_cols:
                    if not pd.isna(averages[col]):
                        avg_row[col] = round(averages[col], 2)  # round to 2 decimals
                    else:
                        avg_row[col] = None
                avg_df = pd.DataFrame([avg_row])
                pivot = pd.concat([pivot, avg_df], ignore_index=True)
            
            pivot['breakfast_included'] = pivot['breakfast_included'].apply(lambda x: 'Yes' if x else 'No' if x is not None else '')

            # Build AgGrid
            gb = GridOptionsBuilder.from_dataframe(pivot)

            # Pin first 3 columns
            gb.configure_columns(['scrape_date', 'name', 'breakfast_included'], pinned='left', minWidth=150)

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