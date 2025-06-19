import streamlit as st
import requests
import time
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Page config
st.set_page_config(
    page_title="Hospital Data Stream Monitor",
    page_icon="🏥",
    layout="wide"
)

# Constants
CONSUMER_API_URL = "http://127.0.0.1:5000/api"

def get_consumer_stats():
    """Get statistics from consumer API"""
    try:
        response = requests.get(f"{CONSUMER_API_URL}/stats", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        st.error(f"Error connecting to consumer API: {e}")
        return None

def format_uptime(seconds):
    """Format uptime in human readable format"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m {int(seconds%60)}s"
    else:
        hours = int(seconds/3600)
        minutes = int((seconds%3600)/60)
        return f"{hours}h {minutes}m"

def main():
    st.title("🏥 Hospital Data Stream Monitor")
    st.markdown("Real-time monitoring of hospital data streaming pipeline")
    
    # Create placeholders for dynamic content
    status_placeholder = st.empty()
    metrics_placeholder = st.empty()
    file_stats_placeholder = st.empty()
    charts_placeholder = st.empty()
    
    # Initialize session state for historical data
    if 'history' not in st.session_state:
        st.session_state.history = []
    
    # Auto-refresh every 2 seconds
    while True:
        stats = get_consumer_stats()
        
        if stats:
            # Update status
            with status_placeholder.container():
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    status_color = {
                        'initializing': '🟡',
                        'connecting': '🟡',
                        'running': '🟢',
                        'stopped': '🔴'
                    }.get(stats['status'], '⚪')
                    
                    st.markdown(f"**Status:** {status_color} {stats['status'].title()}")
                
                with col2:
                    if stats['last_message_time']:
                        last_msg = datetime.fromisoformat(stats['last_message_time'])
                        time_diff = (datetime.now() - last_msg).total_seconds()
                        if time_diff < 30:
                            st.markdown("**Last Message:** 🟢 Active")
                        else:
                            st.markdown(f"**Last Message:** 🟡 {int(time_diff)}s ago")
                    else:
                        st.markdown("**Last Message:** ⚪ None")
                
                with col3:
                    uptime_str = format_uptime(stats['uptime_seconds'])
                    st.markdown(f"**Uptime:** {uptime_str}")
            
            # Update metrics
            with metrics_placeholder.container():
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        label="Total Messages",
                        value=f"{stats['total_messages']:,}",
                        delta=stats['messages_per_second']
                    )
                
                with col2:
                    st.metric(
                        label="Total Batches",
                        value=stats['total_batches']
                    )
                
                with col3:
                    st.metric(
                        label="Current Batch Size",
                        value=stats['current_batch_size']
                    )
                
                with col4:
                    st.metric(
                        label="Messages/Second",
                        value=f"{stats['messages_per_second']:.1f}"
                    )
            
            # Update file statistics
            with file_stats_placeholder.container():
                st.subheader("📊 File Statistics")
                
                # Create file statistics table
                file_stats_data = {
                    'Metric': ['JSON Files Saved', 'CSV Files Saved', 'Total Errors', 'Success Rate'],
                    'Value': [
                        f"{stats['files_saved']['json']:,}",
                        f"{stats['files_saved']['csv']:,}",
                        f"{stats['errors']:,}",
                        f"{((stats['files_saved']['json'] + stats['files_saved']['csv']) / max(stats['total_batches'], 1) * 100):.1f}%"
                    ],
                    'Status': [
                        '✅' if stats['files_saved']['json'] > 0 else '⚪',
                        '✅' if stats['files_saved']['csv'] > 0 else '⚪',
                        '🔴' if stats['errors'] > 0 else '✅',
                        '✅' if stats['errors'] == 0 else ('🟡' if stats['errors'] < 5 else '🔴')
                    ]
                }
                
                file_stats_df = pd.DataFrame(file_stats_data)
                st.dataframe(
                    file_stats_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Metric": st.column_config.TextColumn("Metric", width="medium"),
                        "Value": st.column_config.TextColumn("Value", width="medium"),
                        "Status": st.column_config.TextColumn("Status", width="small")
                    }
                )
            
            # Add current stats to history
            current_time = datetime.now()
            st.session_state.history.append({
                'timestamp': current_time,
                'total_messages': stats['total_messages'],
                'messages_per_second': stats['messages_per_second'],
                'current_batch_size': stats['current_batch_size']
            })
            
            # Keep only last 50 data points
            if len(st.session_state.history) > 50:
                st.session_state.history = st.session_state.history[-50:]
            
            # Update charts
            with charts_placeholder.container():
                if len(st.session_state.history) > 1:
                    df = pd.DataFrame(st.session_state.history)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("📈 Messages Over Time")
                        fig1 = px.line(
                            df, 
                            x='timestamp', 
                            y='total_messages',
                            title="Total Messages Processed"
                        )
                        fig1.update_layout(height=300)
                        st.plotly_chart(fig1, use_container_width=True)
                    
                    with col2:
                        st.subheader("⚡ Processing Rate")
                        fig2 = px.line(
                            df, 
                            x='timestamp', 
                            y='messages_per_second',
                            title="Messages per Second"
                        )
                        fig2.update_layout(height=300)
                        st.plotly_chart(fig2, use_container_width=True)
                
                # System Details Table
                st.subheader("🔧 System Details")
                system_details_data = {
                    'Component': [
                        'Consumer Status',
                        'Total Messages Processed',
                        'Total Batches Processed',
                        'Current Batch Size',
                        'Processing Rate',
                        'System Uptime',
                        'Last Message Received',
                        'JSON Files Created',
                        'CSV Files Created',
                        'Error Count'
                    ],
                    'Value': [
                        stats['status'].title(),
                        f"{stats['total_messages']:,}",
                        f"{stats['total_batches']:,}",
                        f"{stats['current_batch_size']:,}",
                        f"{stats['messages_per_second']:.2f} msg/sec",
                        format_uptime(stats['uptime_seconds']),
                        datetime.fromisoformat(stats['last_message_time']).strftime('%Y-%m-%d %H:%M:%S') if stats['last_message_time'] else 'Never',
                        f"{stats['files_saved']['json']:,}",
                        f"{stats['files_saved']['csv']:,}",
                        f"{stats['errors']:,}"
                    ],
                    'Health': [
                        '🟢' if stats['status'] == 'running' else ('🟡' if stats['status'] in ['initializing', 'connecting'] else '🔴'),
                        '🟢' if stats['total_messages'] > 0 else '⚪',
                        '🟢' if stats['total_batches'] > 0 else '⚪',
                        '🟢' if stats['current_batch_size'] > 0 else '⚪',
                        '🟢' if stats['messages_per_second'] > 0 else '⚪',
                        '🟢' if stats['uptime_seconds'] > 0 else '⚪',
                        '🟢' if stats['last_message_time'] and (datetime.now() - datetime.fromisoformat(stats['last_message_time'])).total_seconds() < 30 else '🟡',
                        '🟢' if stats['files_saved']['json'] > 0 else '⚪',
                        '🟢' if stats['files_saved']['csv'] > 0 else '⚪',
                        '🟢' if stats['errors'] == 0 else '🔴'
                    ]
                }
                
                system_df = pd.DataFrame(system_details_data)
                st.dataframe(
                    system_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Component": st.column_config.TextColumn("Component", width="medium"),
                        "Value": st.column_config.TextColumn("Value", width="large"),
                        "Health": st.column_config.TextColumn("Health", width="small")
                    }
                )
        
        else:
            with status_placeholder.container():
                st.error("❌ Cannot connect to consumer service")
                st.info("Make sure the data-consumer service is running")
        
        # Wait before next refresh
        time.sleep(2)

if __name__ == "__main__":
    main()
