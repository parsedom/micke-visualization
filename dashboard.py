import streamlit as st
import boto3
import pandas as pd
from datetime import datetime
import json
import plotly.express as px
from boto3.dynamodb.conditions import Key,Attr
import os
# import pandas as pd
FLAGGED_HOTELS = [
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

aws_key = st.secrets["AWS_ACCESS_KEY_ID"]
aws_secret = st.secrets["AWS_SECRET_ACCESS_KEY"]
region = st.secrets["AWS_DEFAULT_REGION"]
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret,
    region_name=region
)
# dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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
                'hotel_url': item.get('hotel_url', '')
            })
        
        return transformed_items
    
    except Exception as e:
        st.error(f"Error querying DynamoDB: {str(e)}")
        return []


# Configure page
st.set_page_config(
    page_title="Hotel Booking Dashboard",
    page_icon="🏨",
    layout="wide"
)


st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem
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
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>🏨 Hotel Booking Price Dashboard</h1>
    <p>Analyze hotel prices across different dates and locations</p>
</div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### 🔧 Configuration")
    
    with st.expander("📍 Location & Booking Details", expanded=True):
        location = st.selectbox("Location", ["tampere", "oulu"], index=0)
        persons = st.selectbox("Persons", [1, 2])
        nights = st.selectbox("Nights", [1, 3, 7])
        time_of_day = st.selectbox("Time", ["evening", "morning"])
    
    with st.expander("📅 Date Ranges", expanded=True):
        # Use unique keys for each date input to prevent caching issues
        scraped_date_range = st.date_input(
            "Scrape Dates", 
            value=[], 
            key="scrape_dates_unique"
        )
        price_date_range = st.date_input(
            "Price Dates", 
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
        st.error("⚠️ Please select both price dates")
        st.stop()

    scraped_start = scraped_date_range[0].strftime("%Y-%m-%d")
    scraped_end = scraped_date_range[1].strftime("%Y-%m-%d")
    
    # DEBUG: Show what dates are being used
    # st.write(f"Scrape dates selected: {scraped_date_range}")
    # st.write(f"Formatted scrape dates: {scraped_start} to {scraped_end}")
    
    try:
        price_start = price_date_range[0].strftime("%d-%m-%Y")
        price_end = price_date_range[1].strftime("%d-%m-%Y")
    except ValueError:
        price_start = price_date_range[0].strftime("%d-%m-%Y")
        price_end = price_date_range[1].strftime("%d-%m-%Y")
    
    # st.write(f"Price dates selected: {price_date_range}")
    # st.write(f"Formatted price dates: {price_start} to {price_end}")


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
    
    if not df.empty:
        # Hotel selection section
        st.markdown('<div class="hotel-selector">', unsafe_allow_html=True)

        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            st.markdown("### 🏨 Hotel Selection")
            
            # Get unique hotel names from current data
            unique_hotels = sorted(df['name'].unique())
            
            # Create quick selection buttons
            st.markdown("**Quick Selection:**")
            col_bt1, col_bt2, col_bt3 = st.columns(3)
            
            with col_bt1:
                if st.button("✅ Select All", key="select_all_btn", use_container_width=True, help="Select all available hotels"):
                    st.session_state.selected_hotels = unique_hotels
            
            with col_bt2:
                if st.button("🚩 Flagged Hotels", key="select_flagged_btn", use_container_width=True, help="Select only flagged hotels"):
                    # Get only flagged hotels that exist in current data
                    available_flagged = [hotel for hotel in FLAGGED_HOTELS if hotel in unique_hotels]
                    st.session_state.selected_hotels = available_flagged
            
            with col_bt3:
                if st.button("❌ Clear All", key="clear_all_btn", use_container_width=True, help="Clear all selections"):
                    st.session_state.selected_hotels = []
            
            # Initialize session state if it doesn't exist
            if 'selected_hotels' not in st.session_state:
                st.session_state.selected_hotels = []
            
            # Create the hotel selection dropdown
            hotels = st.multiselect(
                "Select hotels to analyze:",
                unique_hotels,
                default=st.session_state.selected_hotels,
                key="hotel_selector"
            )
            
            # Update session state with current selection
            st.session_state.selected_hotels = hotels

        with col2:
            total_hotels = len(unique_hotels)
            st.metric("Total Hotels", total_hotels)
            
            # Show how many flagged hotels are available
            available_flagged = len([h for h in FLAGGED_HOTELS if h in unique_hotels])
            if available_flagged > 0:
                st.metric("🚩 Available", available_flagged)

        with col3:
            if hotels:
                selected_count = len(hotels)
                st.metric("Selected", selected_count)
                
                # Show flagged hotels count if any are selected
                flagged_selected = len([h for h in hotels if h in FLAGGED_HOTELS])
                if flagged_selected > 0:
                    st.metric("🚩 Flagged", flagged_selected)

        st.markdown('</div>', unsafe_allow_html=True)
        
        if hotels:
            filtered_df = df[df['name'].isin(hotels)]
            # Parse date in DD-MM-YYYY format
            filtered_df['price_date'] = pd.to_datetime(filtered_df['price_date'], format='%Y-%m-%d')
                    
            # Analytics section
            # st.markdown("## 📊 Analytics Dashboard")
            
            # # Key metrics row
            # col1, col2, col3, col4 = st.columns(4)
            # with col1:
            #     avg_price = filtered_df['price'].mean()
            #     st.metric("Average Price", f"€{avg_price:.1f}")
            # with col2:
            #     min_price = filtered_df['price'].min()
            #     st.metric("Lowest Price", f"€{min_price:.1f}")
            # with col3:
            #     max_price = filtered_df['price'].max()
            #     st.metric("Highest Price", f"€{max_price:.1f}")
            # with col4:
            #     price_range = max_price - min_price
            #     st.metric("Price Range", f"€{price_range:.1f}")
            
            # # Charts section
            # col1, col2 = st.columns(2)
            
            # with col1:
            #     st.markdown("### 📈 Price Trends")
            #     fig_line = px.line(
            #         filtered_df, x='price_date', y='price', color='name',
            #         title='Hotel Price Evolution', markers=True,
            #         color_discrete_sequence=px.colors.qualitative.Set3
            #     )
            #     fig_line.update_layout(height=400, showlegend=True)
            #     st.plotly_chart(fig_line, use_container_width=True)
            
            # with col2:
            #     st.markdown("### 📊 Price Distribution")
            #     fig_box = px.box(
            #         filtered_df, x='name', y='price',
            #         title='Price Distribution by Hotel',
            #         color_discrete_sequence=['#7dd3c0']
            #     )
            #     fig_box.update_xaxes(tickangle=45)
            #     fig_box.update_layout(height=400)
            #     st.plotly_chart(fig_box, use_container_width=True)
            
            # Bar chart
            st.markdown("### 📊 Daily Average Prices")
            daily_avg = filtered_df.groupby('price_date')['price'].mean().reset_index()
            daily_avg['date_label'] = daily_avg['price_date'].dt.strftime('%b %d')
            
            fig_bar = px.bar(
                daily_avg, x='date_label', y='price',
                title='Average Prices Across All Selected Hotels',
                color_discrete_sequence=['#7dd3c0'], text='price'
            )
            fig_bar.update_traces(texttemplate='€%{text:.1f}', textposition='outside')
            fig_bar.update_layout(
                height=500, showlegend=False,
                xaxis_title="Date", yaxis_title="Average Price (€)"
            )
            st.plotly_chart(fig_bar, use_container_width=True)
            
            # Detailed table section
            st.markdown("### 📋 Detailed Price Matrix")

            # Create pivot
            pivot = filtered_df.pivot_table(
                index=['scrape_date', 'name'], columns='price_date',
                values='price', aggfunc='mean', fill_value=''
            )
            pivot.columns = [col.strftime('%Y-%m-%d') if isinstance(col, pd.Timestamp) else col for col in pivot.columns]
            pivot = pivot.reset_index()

            # Add group separators (empty rows between scrape dates)
            unique_dates = pivot['scrape_date'].unique()
            if len(unique_dates) > 1:
                final_pivot = pd.DataFrame()
                for i, date in enumerate(unique_dates):
                    group = pivot[pivot['scrape_date'] == date]
                    final_pivot = pd.concat([final_pivot, group], ignore_index=True)
                    if i < len(unique_dates) - 1:
                        # Add empty row as separator
                        empty_row = {col: '' for col in pivot.columns}
                        empty_df = pd.DataFrame([empty_row])
                        final_pivot = pd.concat([final_pivot, empty_df], ignore_index=True)
                pivot = final_pivot

            # Add average row
            numeric_cols = [col for col in pivot.columns if col not in ['scrape_date', 'name']]
            if numeric_cols:
                valid_data = pivot[pivot['scrape_date'] != '']
                
                for col in numeric_cols:
                    valid_data[col] = pd.to_numeric(valid_data[col], errors='coerce')
                
                averages = valid_data[numeric_cols].mean()
                
                # Add empty row as separator before average
                empty_row = {col: '' for col in pivot.columns}
                empty_df = pd.DataFrame([empty_row])
                pivot = pd.concat([pivot, empty_df], ignore_index=True)
                
                # Average row
                avg_row = {'scrape_date': 'AVERAGE', 'name': ''}
                for col in numeric_cols:
                    avg_row[col] = round(averages[col], 2) if not pd.isna(averages[col]) else ''
                
                avg_df = pd.DataFrame([avg_row])
                pivot = pd.concat([pivot, avg_df], ignore_index=True)

            pivot = pivot.dropna(how='all')

            def style_table(row):
                if 'AVERAGE' in str(row['scrape_date']):
                    return ['background-color: #e8f4fd; font-weight: bold; color: #1f77b4'] * len(row)
                return [''] * len(row)

            def format_numeric_value(x):
                if x != '' and not pd.isna(x):
                    try:
                        num = float(x)
                        formatted = f"{num:.2f}"
                        if formatted.endswith('.00'):
                            return formatted[:-3]
                        elif formatted.endswith('0'):
                            return formatted[:-1]
                        else:
                            return formatted
                    except (ValueError, TypeError):
                        return str(x)
                return ''

            for col in numeric_cols:
                pivot[col] = pivot[col].apply(format_numeric_value)

            st.dataframe(pivot.style.apply(style_table, axis=1), 
                        use_container_width=True, height=600, hide_index=True)
            
            # # Export section
            # st.markdown("### 💾 Export Options")
            # col1, col2 = st.columns(2)
            # with col1:
            #     csv_data = filtered_df.to_csv(index=False)
            #     st.download_button(
            #         "📥 Download Raw Data (CSV)",
            #         csv_data,
            #         f"hotel_data_{location}_{datetime.now().strftime('%Y%m%d')}.csv",
            #         "text/csv",
            #         use_container_width=True
            #     )
            # with col2:
            #     pivot_csv = pivot.to_csv(index=False)
            #     st.download_button(
            #         "📊 Download Summary Table (CSV)",
            #         pivot_csv,
            #         f"hotel_summary_{location}_{datetime.now().strftime('%Y%m%d')}.csv",
            #         "text/csv",
            #         use_container_width=True
            #     )
        
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