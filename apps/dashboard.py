import sys
import os
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add local path to import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import config

# Page Configuration
st.set_page_config(
    page_title="Traffic Control Center",
    layout="wide"
)

# --- ADVANCED CSS ---
st.markdown("""
<style>
    /* Global Background */
    .stApp {
        background-color: #050505 !important;
    }
    
    /* Headers Styling */
    h1, h2, h3, h4, h5, h6, .stMarkdown, p {
        color: #FFFFFF !important;
    }
    
    /* Layout Adjustments */
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }

    /* Status Badge Styling */
    .status-badge {
        background-color: #111111;
        border: 1px solid #333;
        color: #00FFCC;
        padding: 5px 10px;
        border-radius: 4px;
        font-family: monospace;
        font-size: 12px;
        margin-right: 10px;
        display: inline-block;
        font-weight: bold;
    }

    /* KPI Cards Styling */
    .kpi-card {
        background-color: #0F0F0F;
        border: 1px solid #222;
        border-left: 4px solid #00FFCC; 
        padding: 15px;
        border-radius: 6px;
        text-align: center; 
        height: 140px; 
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        box-shadow: 0 4px 10px rgba(0,0,0,0.5);
    }
    
    .kpi-title {
        color: #888 !important;
        font-size: 14px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 8px;
    }
    
    .kpi-value {
        color: #FFFFFF;
        font-size: 36px;
        font-weight: 700;
        font-family: 'Roboto', sans-serif;
        line-height: 1;
        margin-bottom: 8px;
    }
    
    .kpi-delta {
        font-size: 14px;
        font-weight: 500;
    }
    
    .delta-pos { color: #00FFCC; }
    .delta-neg { color: #FF4B4B; }
    .delta-neutral { color: #666; }
    
    /* Plotly Background Fix */
    .js-plotly-plot .plotly .main-svg {
        background: transparent !important;
    }

    /* Table Styling */
    div[data-testid="stDataFrame"] {
        background-color: #0F0F0F;
        border: 1px solid #222;
        border-radius: 6px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Header Section with Live Status
c1, c2 = st.columns([3, 1])
with c1:
    st.title("TRAFFIC CONTROL CENTER")
    st.markdown("""
    <div style='margin-bottom: 20px;'>
        <span class="status-badge">SYSTEM ONLINE</span>
        <span class="status-badge">STREAM: ACTIVE</span>
        <span class="status-badge">SPARK CLUSTER: RUNNING</span>
    </div>
    """, unsafe_allow_html=True)

DATA_FILE = config.DASHBOARD_DATA_PATH

def create_gauge(value, title, min_val, max_val, threshold, unit, color_hex, prev_value):
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = value,
        delta = {'reference': prev_value, 'position': "bottom", 'relative': False, 'valueformat': '.0f', 'font': {'size': 18, 'color': '#888'}},
        title = {'text': title, 'font': {'size': 18, 'color': "#AAA"}},
        number = {'suffix': unit, 'font': {'size': 42, 'color': "white", 'weight': 'bold'}},
        gauge = {
            'axis': {'range': [min_val, max_val], 'tickwidth': 1, 'tickcolor': "white", 'tickfont': {'size': 14, 'color': 'white'}},
            'bar': {'color': color_hex, 'thickness': 0.85},
            'bgcolor': "#050505",
            'borderwidth': 0,
            'steps': [
                {'range': [0, threshold], 'color': "#151515"},
                {'range': [threshold, max_val], 'color': "#2a0000"} 
            ],
            'threshold': {
                'line': {'color': "#FF2222", 'width': 4},
                'thickness': 0.85, 
                'value': value
            }
        }
    ))
    fig.update_layout(margin=dict(l=30, r=30, t=40, b=10), height=260, paper_bgcolor="rgba(0,0,0,0)", font={'family': "Arial"})
    return fig

try:
    if not os.path.exists(DATA_FILE):
        st.warning("Waiting for data generation...")
        time.sleep(2)
        st.rerun()

    df = pd.read_csv(DATA_FILE)
    
    if df.empty:
        st.warning("Waiting for data stream to start...")
        time.sleep(2)
        st.rerun()

    # Data Processing
    df['window_start'] = pd.to_datetime(df['window_start'])
    
    # Filter Last 30 Minutes
    latest_time = df['window_start'].max()
    cutoff_time = latest_time - pd.Timedelta(minutes=30)
    filtered_df = df[df['window_start'] >= cutoff_time]

    last_row = df.iloc[-1]
    prev_row = df.iloc[-2] if len(df) > 1 else last_row

    # --- ROW 1: KPIs ---
    col1, col2, col3 = st.columns(3)

    # 1. Current Velocity
    curr_alerts = int(last_row["num_of_rows"])
    prev_alerts = int(prev_row["num_of_rows"])
    delta_alerts = curr_alerts - prev_alerts
    delta_class = "delta-pos" if delta_alerts >= 0 else "delta-neg"
    delta_sign = "+" if delta_alerts >= 0 else ""
    
    with col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Current Velocity (Last Minute)</div>
            <div class="kpi-value">{curr_alerts}</div>
            <div class="kpi-delta {delta_class}">{delta_sign}{delta_alerts} vs prev</div>
        </div>
        """, unsafe_allow_html=True)

    # 2. Session Total
    total_alerts = int(df["num_of_rows"].sum())
    
    with col2:
        st.markdown(f"""
        <div class="kpi-card" style="border-left: 4px solid #FF0055;">
            <div class="kpi-title">Total Session Violations</div>
            <div class="kpi-value">{total_alerts:,}</div>
            <div class="kpi-delta delta-neutral">Cumulative</div>
        </div>
        """, unsafe_allow_html=True)

    # 3. Critical RPM Batches
    high_rpm_batches = df[df['maximum_rpm'] > 6500].shape[0]
    
    with col3:
        st.markdown(f"""
        <div class="kpi-card" style="border-left: 4px solid #FFA500;">
            <div class="kpi-title">Critical RPM Batches</div>
            <div class="kpi-value">{high_rpm_batches}</div>
            <div class="kpi-delta" style="color: #FFA500;">Engine Stress Events</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---") 

    # --- ROW 2: LIVE TELEMETRY ---
    col_speed, col_rpm, col_donut = st.columns([1.5, 1.5, 1.5])

    with col_speed:
        current_speed = int(last_row['maximum_speed'])
        prev_speed = int(prev_row['maximum_speed'])
        
        bar_color = "#00FFCC" 
        if current_speed > 130: bar_color = "#FFDD00"
        if current_speed > 160: bar_color = "#FF0055"
        
        fig_speed = create_gauge(current_speed, "Live Max Speed", 0, 260, 130, " km/h", bar_color, prev_speed)
        st.plotly_chart(fig_speed, use_container_width=True, key="speed_gauge")

    with col_rpm:
        current_rpm = int(last_row['maximum_rpm'])
        prev_rpm = int(prev_row['maximum_rpm'])
        fig_rpm = create_gauge(current_rpm, "Live Engine RPM", 0, 9000, 6500, "", "#FF9900", prev_rpm)
        st.plotly_chart(fig_rpm, use_container_width=True, key="rpm_gauge")

    with col_donut:
        st.markdown("<h4 style='text-align: center; color: #888; font-size: 16px; margin-bottom: 0px;'>VIOLATIONS BY COLOR</h4>", unsafe_allow_html=True)
        color_data = pd.DataFrame({
            "Color": ["Black", "White", "Silver"],
            "Count": [last_row["num_of_black"], last_row["num_of_white"], last_row["num_of_silver"]]
        })
        
        fig_pie = px.pie(
            color_data, 
            values="Count", 
            names="Color", 
            color="Color",
            color_discrete_map={
                "Black": "#222222", 
                "White": "#FFFFFF",  
                "Silver": "#888888"  
            },
            hole=0.5
        )
        fig_pie.update_traces(
            textinfo='percent+label', 
            textfont_size=16,
            marker=dict(line=dict(color='#111', width=2)) 
        )
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", 
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False, 
            legend=dict(font=dict(color="#AAA"), orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
            margin=dict(t=20, b=20, l=20, r=20),
            height=320
        )
        st.plotly_chart(fig_pie, use_container_width=True, key="pie_chart")

    # --- ROW 3: TRENDS ---
    st.markdown("### VIOLATION TREND (LAST 30 MIN)")
    
    fig_bar = px.bar(
        filtered_df, 
        x="window_start", 
        y="num_of_rows",
        color="num_of_rows",
        color_continuous_scale="Magma",
        text="num_of_rows", 
        labels={"window_start": "Time", "num_of_rows": "Alert Count"},
        template="plotly_dark"
    )
    
    fig_bar.update_traces(
        marker_line_width=0,
        textposition='outside',
        textfont_size=14,
        textfont_color='white'
    )
    
    fig_bar.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", 
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(
            showgrid=False, 
            tickformat="%H:%M", 
            tickfont=dict(size=14, color="white"),
            title_font=dict(size=18, color="white", family="Arial Black"),
            title_standoff=15
        ),
        yaxis=dict(
            showgrid=True, 
            gridcolor="#222", 
            tickfont=dict(size=14, color="white"),
            title_font=dict(size=18, color="white", family="Arial Black")
        ),
        bargap=0.4,
        coloraxis_showscale=True,
        coloraxis_colorbar=dict(
            title="Intensity",
            tickfont=dict(color="white"),
            title_font=dict(color="white")
        ),    
        height=500,
        margin=dict(t=40, l=20, r=20, b=20)
    )
    st.plotly_chart(fig_bar, use_container_width=True, key="trend_chart")
    
    # --- ROW 4: ADVANCED DATA TABLE ---
    
    # Rename for display
    display_df = filtered_df.rename(columns={
        "window_start": "Timestamp",
        "num_of_rows": "Total Alerts",
        "num_of_black": "Black Cars",
        "num_of_white": "White Cars",
        "num_of_silver": "Silver Cars",
        "maximum_speed": "Top Speed",
        "maximum_rpm": "Peak RPM"
    })
    
    cols_to_show = ["Timestamp", "Total Alerts", "Black Cars", "White Cars", "Silver Cars", "Top Speed", "Peak RPM"]
    display_df = display_df[cols_to_show].sort_values(by="Timestamp", ascending=False).head(20)

    with st.expander("INSPECT LIVE DATA FEED", expanded=True):
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Timestamp": st.column_config.DatetimeColumn(
                    "Time",
                    format="D MMM, HH:mm:ss",
                ),
                "Top Speed": st.column_config.ProgressColumn(
                    "Max Speed (km/h)",
                    format="%d km/h",
                    min_value=0,
                    max_value=250,
                ),
                "Peak RPM": st.column_config.NumberColumn(
                    "Max RPM",
                    format="%d",
                ),
                "Total Alerts": st.column_config.NumberColumn(
                    "Volume",
                    format="%d",
                ),
            }
        )

except Exception as e:
    st.error(f"System Error: {e}")

time.sleep(0.5)
st.rerun()