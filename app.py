"""
Tax Loss Harvesting Tracker - Dark Navy Theme
A multi-user tool for tracking 30-day wash sale periods.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import database as db
import time

# Page configuration
st.set_page_config(
    page_title="Tax Loss Harvest Tracker",
    page_icon="📊",
    layout="wide",
)

# Dark Navy Theme CSS
THEME_CSS = """
<style>
    /* Import clean, modern fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap');
    
    /* CSS Variables for easy theming */
    :root {
        --bg-primary: #020617;
        --bg-secondary: #0f172a;
        --bg-tertiary: #1e293b;
        --bg-card: #1e293b;
        --border-color: #334155;
        --border-accent: #3b82f6;
        --text-primary: #f1f5f9;
        --text-secondary: #94a3b8;
        --text-muted: #64748b;
        --accent-blue: #3b82f6;
        --accent-blue-light: #60a5fa;
        --success: #22c55e;
        --warning: #eab308;
        --danger: #ef4444;
    }
    
    /* Remove top padding/margin */
    .block-container {
        padding-top: 1rem !important;
    }
    
    header[data-testid="stHeader"] {
        background: var(--bg-primary) !important;
        height: 0 !important;
        min-height: 0 !important;
        padding: 0 !important;
    }
    
    /* Hide fullscreen button */
    [data-testid="StyledFullScreenButton"] {
        display: none !important;
    }
    
    /* Main app background */
    .stApp {
        background: linear-gradient(180deg, var(--bg-primary) 0%, var(--bg-secondary) 50%, var(--bg-primary) 100%);
    }
    
    /* All text styling */
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
        color: var(--text-primary) !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    
    /* Main Header */
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: var(--text-primary) !important;
        margin-bottom: 0.25rem;
        margin-top: 0 !important;
        padding-top: 0 !important;
        font-family: 'Space Grotesk', sans-serif !important;
        letter-spacing: -0.5px;
    }
    
    .sub-header {
        font-size: 1rem;
        color: var(--text-secondary) !important;
        margin-bottom: 1.5rem;
        font-family: 'Inter', sans-serif !important;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: var(--bg-secondary) !important;
        border-right: 1px solid var(--border-color) !important;
    }
    
    [data-testid="stSidebar"] * {
        color: var(--text-primary) !important;
    }
    
    /* Metric cards */
    [data-testid="stMetric"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
        padding: 16px !important;
        transition: transform 0.2s ease, border-color 0.2s ease !important;
    }
    
    [data-testid="stMetric"]:hover {
        transform: translateY(-2px) !important;
        border-color: var(--accent-blue) !important;
    }
    
    [data-testid="stMetric"] label {
        color: var(--text-secondary) !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.8rem !important;
        font-weight: 500 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
    }
    
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--text-primary) !important;
        font-family: 'Space Grotesk', sans-serif !important;
        font-weight: 600 !important;
    }
    
    [data-testid="stMetric"] [data-testid="stMetricDelta"] {
        color: var(--success) !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: transparent !important;
        border-bottom: 1px solid var(--border-color) !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        color: var(--text-secondary) !important;
        background-color: transparent !important;
        border: none !important;
        border-bottom: 2px solid transparent !important;
    }
    
    .stTabs [aria-selected="true"] {
        color: var(--accent-blue) !important;
        background-color: transparent !important;
        border-bottom: 2px solid var(--accent-blue) !important;
    }
    
    /* Buttons */
    .stButton > button {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 500 !important;
        border-radius: 6px !important;
        transition: all 0.2s ease !important;
    }
    
    .stButton > button:hover {
        background: var(--accent-blue) !important;
        border-color: var(--accent-blue) !important;
        color: white !important;
    }
    
    .stButton > button:active {
        transform: scale(0.98) !important;
    }
    
    /* Input fields */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div,
    .stDateInput > div > div > input {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        color: var(--text-primary) !important;
        font-family: 'Inter', sans-serif !important;
        border-radius: 6px !important;
    }
    
    .stTextInput > div > div > input:focus,
    .stSelectbox > div > div > div:focus {
        border-color: var(--accent-blue) !important;
        box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2) !important;
    }
    
    /* Success/Info/Warning/Error boxes */
    .stSuccess {
        background-color: rgba(34, 197, 94, 0.1) !important;
        border: 1px solid var(--success) !important;
        color: var(--success) !important;
        border-radius: 6px !important;
    }
    
    .stInfo {
        background-color: rgba(59, 130, 246, 0.1) !important;
        border: 1px solid var(--accent-blue) !important;
        color: var(--accent-blue-light) !important;
        border-radius: 6px !important;
    }
    
    .stWarning {
        background-color: rgba(234, 179, 8, 0.1) !important;
        border: 1px solid var(--warning) !important;
        color: var(--warning) !important;
        border-radius: 6px !important;
    }
    
    .stError {
        background-color: rgba(239, 68, 68, 0.1) !important;
        border: 1px solid var(--danger) !important;
        color: var(--danger) !important;
        border-radius: 6px !important;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        color: var(--text-primary) !important;
        border-radius: 6px !important;
    }
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: var(--bg-secondary);
    }
    
    ::-webkit-scrollbar-thumb {
        background: var(--border-color);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: var(--text-muted);
    }
    
    /* Alert box for ACTION REQUIRED */
    .action-alert {
        background: rgba(239, 68, 68, 0.08) !important;
        border: 1px solid var(--danger) !important;
        border-radius: 10px !important;
        padding: 20px !important;
        margin: 15px 0 !important;
    }
    
    .action-alert-header {
        color: var(--danger) !important;
        font-size: 1.2rem !important;
        font-weight: 600 !important;
        margin-bottom: 10px !important;
        font-family: 'Space Grotesk', sans-serif !important;
    }
    
    .action-item {
        background: rgba(239, 68, 68, 0.05) !important;
        border-left: 3px solid var(--danger) !important;
        padding: 12px 16px !important;
        margin: 8px 0 !important;
        color: var(--text-primary) !important;
        border-radius: 0 6px 6px 0 !important;
    }
    
    .action-item:hover {
        background: rgba(239, 68, 68, 0.1) !important;
    }
    
    /* Checkbox styling */
    .stCheckbox label span {
        color: var(--text-primary) !important;
    }
    
    /* Code blocks - inline */
    code {
        background: var(--bg-tertiary) !important;
        color: var(--accent-blue-light) !important;
        border: 1px solid var(--border-color) !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
    }
    
    /* Streamlit code blocks (st.code) */
    [data-testid="stCode"],
    .stCode,
    pre {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 6px !important;
    }
    
    [data-testid="stCode"] code,
    .stCode code,
    pre code {
        background: transparent !important;
        color: var(--text-primary) !important;
        font-family: 'SF Mono', 'Fira Code', monospace !important;
    }
    
    /* Code copy button */
    [data-testid="stCode"] button {
        display: none !important;
    }
    
    /* File uploader styling */
    [data-testid="stFileUploader"] {
        background: var(--bg-tertiary) !important;
        border: 1px dashed var(--border-color) !important;
        border-radius: 8px !important;
        padding: 12px !important;
    }
    
    [data-testid="stFileUploader"] label {
        color: var(--text-primary) !important;
    }
    
    [data-testid="stFileUploader"] section {
        background: transparent !important;
    }
    
    [data-testid="stFileUploader"] button {
        background: var(--bg-secondary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important;
    }
    
    /* Download button styling */
    .stDownloadButton > button {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important;
        font-family: 'Inter', sans-serif !important;
    }
    
    .stDownloadButton > button:hover {
        background: var(--accent-blue) !important;
        border-color: var(--accent-blue) !important;
    }
    
    /* Expander styling */
    [data-testid="stExpander"] {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
    }
    
    [data-testid="stExpander"] summary {
        color: var(--text-primary) !important;
    }
    
    /* Section headers */
    h3, .stMarkdown h3 {
        color: var(--text-primary) !important;
        font-family: 'Space Grotesk', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: -0.3px;
        border-bottom: 1px solid var(--border-color);
        padding-bottom: 0.5rem;
    }
    
    /* Horizontal rule styling */
    hr {
        border: none !important;
        height: 1px !important;
        background: var(--border-color) !important;
        margin: 1.5rem 0 !important;
    }
    
    /* Caption/timestamp styling */
    .stCaption, small {
        color: var(--text-muted) !important;
        font-size: 0.75rem !important;
    }
    
    /* Selection highlight color */
    ::selection {
        background: var(--accent-blue) !important;
        color: white !important;
    }
    
    /* Focus states */
    *:focus {
        outline: 2px solid var(--accent-blue) !important;
        outline-offset: 2px;
    }
    
    /* Tooltip styling */
    [data-baseweb="tooltip"] {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        color: var(--text-primary) !important;
        border-radius: 6px !important;
    }
    
    /* Data Editor (Excel-like grid) styling */
    [data-testid="stDataFrame"],
    [data-testid="stDataFrameResizable"] {
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
        overflow: hidden;
    }
    
    [data-testid="stDataFrame"] > div,
    [data-testid="stDataFrameResizable"] > div {
        background: var(--bg-tertiary) !important;
    }
    
    /* Grid header cells */
    .dvn-scroller .ch {
        background: var(--bg-secondary) !important;
        color: var(--text-secondary) !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        font-size: 0.75rem !important;
        letter-spacing: 0.5px !important;
        border-bottom: 1px solid var(--border-color) !important;
    }
    
    /* Grid data cells */
    .dvn-scroller .cc {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        font-family: 'Inter', sans-serif !important;
        border-color: var(--border-color) !important;
    }
    
    /* Selected/focused cell */
    .dvn-scroller .cc.selected,
    .dvn-scroller .cc:focus {
        background: rgba(59, 130, 246, 0.15) !important;
        border: 1px solid var(--accent-blue) !important;
    }
    
    /* Hover effect on cells */
    .dvn-scroller .cc:hover {
        background: rgba(59, 130, 246, 0.08) !important;
    }
    
    /* Grid scrollbar */
    .dvn-scroller::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    .dvn-scroller::-webkit-scrollbar-track {
        background: var(--bg-secondary);
    }
    
    .dvn-scroller::-webkit-scrollbar-thumb {
        background: var(--border-color);
        border-radius: 4px;
    }
    
    /* Glidedata grid overrides */
    .glideDataEditor {
        --gdg-bg-cell: #1e293b !important;
        --gdg-bg-header: #0f172a !important;
        --gdg-bg-header-has-focus: #1e293b !important;
        --gdg-text-dark: #f1f5f9 !important;
        --gdg-text-medium: #e2e8f0 !important;
        --gdg-text-light: #94a3b8 !important;
        --gdg-border-color: #334155 !important;
        --gdg-accent-color: #3b82f6 !important;
        --gdg-accent-light: rgba(59, 130, 246, 0.2) !important;
    }
    
    /* Data editor container */
    [data-testid="stDataFrame"] {
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2) !important;
        border-radius: 8px !important;
    }
    
    /* Loading/refresh spinner override */
    .stSpinner > div {
        border-color: var(--accent-blue) transparent transparent transparent !important;
    }
    
    /* Checkbox style */
    .stCheckbox label span[data-baseweb="checkbox"] {
        border-color: var(--border-color) !important;
        background: var(--bg-tertiary) !important;
        border-radius: 4px !important;
    }
    
    .stCheckbox label span[data-baseweb="checkbox"][aria-checked="true"] {
        background: var(--accent-blue) !important;
        border-color: var(--accent-blue) !important;
    }
    
    /* Toast notifications */
    .stToast {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        color: var(--text-primary) !important;
        border-radius: 8px !important;
    }
    
    /* Link styling */
    a {
        color: var(--accent-blue-light) !important;
        text-decoration: none !important;
        transition: color 0.2s ease;
    }
    
    a:hover {
        color: var(--accent-blue) !important;
        text-decoration: underline !important;
    }
    
    /* Form styling */
    [data-testid="stForm"] {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 10px !important;
        padding: 1.5rem !important;
    }
</style>
"""


def calculate_days_remaining(target_date: str) -> int:
    """Calculate days remaining until target date."""
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    today = datetime.now().date()
    return (target - today).days


def get_status_display(days_remaining: int, completed: bool) -> str:
    """Get status display text based on days remaining."""
    if completed:
        return "COMPLETED"
    elif days_remaining <= 0:
        return "READY"
    else:
        return "WAITING"


def main():
    # Apply dark navy theme
    st.markdown(THEME_CSS, unsafe_allow_html=True)
    
    # Header
    st.markdown('<p class="main-header">📊 Tax Loss Harvest Tracker</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Track 30-day wash sale periods • Multi-user access</p>', unsafe_allow_html=True)
    
    # Get stats
    stats = db.get_stats()
    
    # Refresh button row
    col1, col2 = st.columns([8, 2])
    with col2:
        if st.button("🔄 REFRESH DATA", use_container_width=True):
            st.rerun()
    
    # Ready entries (ACTION REQUIRED)
    ready_entries = db.get_entries_by_status("ready")
    
    if ready_entries:
        # Get account counts for display
        alert_account_counts = db.get_all_account_counts()
        
        st.markdown(f"""
        <div class="action-alert">
            <div class="action-alert-header">
                ⚠️ ACTION REQUIRED: {len(ready_entries)} ACCOUNT(S) READY FOR BUYBACK ⚠️
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Column headers for ACTION REQUIRED
        hcol1, hcol2, hcol3, hcol4 = st.columns([1.2, 6.8, 1, 1])
        with hcol1:
            st.markdown("**STATUS**")
        with hcol3:
            st.markdown("**DOCS**")
        with hcol4:
            st.markdown("**DEL**")
        
        # Show ready accounts with status buttons
        for entry in ready_entries:
            # Get attachments for this entry
            attachments = db.get_attachments(entry['id'])
            current_status = entry.get('status', 'pending') or 'pending'
            
            col1, col2, col3, col4 = st.columns([1.2, 6.8, 1, 1])
            
            with col1:
                # Status button - click to cycle
                if current_status == 'pending':
                    if st.button("🔴 Pending", key=f"status_{entry['id']}", help="Click → In Progress"):
                        db.cycle_status(entry['id'])
                        st.rerun()
                elif current_status == 'in_progress':
                    if st.button("🟡 In Prog", key=f"status_{entry['id']}", help="Click → Completed"):
                        db.cycle_status(entry['id'])
                        st.rerun()
                else:  # completed - shouldn't show here but just in case
                    st.markdown("✅ Done")
            
            with col2:
                days = calculate_days_remaining(entry["target_date"])
                day_text = ">> TODAY <<" if days == 0 else f"+{abs(days)} DAYS OVERDUE"
                broker_text = entry.get('broker', '')
                acct_count = alert_account_counts.get(entry['account'].upper(), 1)
                count_text = f" (x{acct_count})" if acct_count > 1 else ""
                st.markdown(f"""
                <div class="action-item">
                    <strong style="color: #ef4444;">ACCT: {entry['account']}{count_text}</strong> | 
                    BROKER: {broker_text} |
                    TICKERS: {entry['tickers']} | 
                    HELD: {entry['held_in']} | 
                    <span style="color: #ef4444;">{day_text}</span>
                    {f" | NOTE: {entry['comments']}" if entry['comments'] else ""}
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                # Direct download button for attachment
                if attachments:
                    # Get the first (most recent) attachment for direct download
                    att = attachments[0]
                    filename, ftype, fdata = db.get_attachment_data(att['id'])
                    if fdata:
                        st.download_button(
                            f"📎 ({len(attachments)})" if len(attachments) > 1 else "📎",
                            data=fdata,
                            file_name=filename,
                            mime=ftype or "application/octet-stream",
                            key=f"alert_dl_{entry['id']}",
                            help=f"Download: {filename}"
                        )
                else:
                    st.markdown(":gray[—]")
            
            with col4:
                if st.button("X", key=f"del_alert_{entry['id']}"):
                    db.delete_attachments_for_entry(entry["id"])
                    db.delete_entry(entry["id"])
                    st.rerun()
    
    # Summary metrics
    st.markdown("---")
    st.markdown("### 📊 SYSTEM STATUS")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("WAITING", stats['waiting'])
    with col2:
        st.metric("READY", stats['ready'])
    with col3:
        st.metric("DUE THIS WEEK", stats['due_week'])
    with col4:
        st.metric("COMPLETED", stats['completed'])
    
    # Add New Entry Form
    st.markdown("---")
    st.markdown("### ➕ NEW ENTRY")
    
    # Broker options
    BROKER_OPTIONS = ["", "UBS", "SCHWAB", "JMS", "JANNEY", "WELLS FARGO", "MAC"]
    
    with st.form("add_entry_form", clear_on_submit=True):
        # Row 1: Account → Tickers → Held In (Tab flows left to right)
        row1_col1, row1_col2, row1_col3 = st.columns(3)
        with row1_col1:
            account = st.text_input("ACCOUNT NUMBER *", placeholder="e.g., 1234")
        with row1_col2:
            tickers = st.text_input("TICKERS SOLD *", placeholder="e.g., AAPL, AMZN, AMD")
        with row1_col3:
            held_in = st.text_input("HELD IN (ETF/CASH) *", placeholder="e.g., SPY, QQQ, CASH")
        
        # Row 2: Sell Date → Broker → Comments (Tab continues left to right)
        row2_col1, row2_col2, row2_col3 = st.columns(3)
        with row2_col1:
            sell_date = st.date_input("SELL DATE *", value=datetime.now())
        with row2_col2:
            broker = st.selectbox("BROKER *", options=BROKER_OPTIONS, index=0)
        with row2_col3:
            comments = st.text_input("COMMENTS (optional)", placeholder="Any notes...")
        
        # Row 3: File attachment
        st.markdown("**📎 ATTACHMENT (optional)**")
        uploaded_file = st.file_uploader(
            "Attach documentation (email, PDF, image)",
            type=['pdf', 'png', 'jpg', 'jpeg', 'txt', 'doc', 'docx', 'msg', 'eml'],
            help="Max 5MB - Attach trade confirmation, email, etc.",
            label_visibility="collapsed"
        )
        
        submitted = st.form_submit_button(">> EXECUTE INSERT <<", type="primary", use_container_width=True)
        
        if submitted:
            if not account or not tickers or not held_in or not broker:
                st.error("ERROR: REQUIRED FIELDS MISSING [ACCOUNT, TICKERS, HELD_IN, BROKER]")
            else:
                # Create the entry
                new_entry_id = db.add_entry(
                    account=account.strip(),
                    tickers=tickers.strip(),
                    held_in=held_in.strip(),
                    sell_date=sell_date.strftime("%Y-%m-%d"),
                    broker=broker,
                    comments=comments.strip()
                )
                
                # Add attachment if provided
                if uploaded_file is not None:
                    file_data = uploaded_file.read()
                    if len(file_data) <= 5 * 1024 * 1024:  # 5MB limit
                        db.add_attachment(
                            entry_id=new_entry_id,
                            filename=uploaded_file.name,
                            file_type=uploaded_file.type or "application/octet-stream",
                            file_data=file_data
                        )
                        st.success(f"SUCCESS: ENTRY + ATTACHMENT INSERTED FOR ACCOUNT {account}")
                    else:
                        st.warning("Attachment skipped - file exceeds 5MB limit")
                        st.success(f"SUCCESS: ENTRY INSERTED FOR ACCOUNT {account}")
                else:
                    st.success(f"SUCCESS: ENTRY INSERTED FOR ACCOUNT {account}")
                st.rerun()
    
    # Active Entries Table (non-completed only) - EXCEL-LIKE GRID
    st.markdown("---")
    st.markdown("### 📋 ACTIVE ENTRIES")
    
    # Filters
    col1, col2 = st.columns([2, 2])
    
    with col1:
        filter_status = st.selectbox(
            "FILTER BY STATUS",
            options=["ALL ACTIVE", "WAITING", "READY"],
            index=0,
            label_visibility="collapsed"
        )
    
    with col2:
        search_query = st.text_input(
            "SEARCH",
            placeholder=">> Search by account or ticker...",
            label_visibility="collapsed"
        )
    
    # Get filtered data (exclude completed)
    if search_query:
        all_results = db.search_entries(search_query)
        entries = [e for e in all_results if not e["completed"]]
    elif filter_status == "ALL ACTIVE":
        all_entries = db.get_all_entries()
        entries = [e for e in all_entries if not e["completed"]]
    else:
        entries = db.get_entries_by_status(filter_status.lower())
    
    # Display entries using data_editor (Excel-like grid)
    if entries:
        st.caption(f">> Displaying {len(entries)} entries // Click cells to edit inline")
        
        # Prepare DataFrame for display
        df_data = []
        for entry in entries:
            days = calculate_days_remaining(entry["target_date"])
            current_status = entry.get('status', 'pending') or 'pending'
            
            # Map status to display
            status_map = {'pending': '🔴 Pending', 'in_progress': '🟡 In Progress', 'completed': '✅ Done'}
            status_display = status_map.get(current_status, '🔴 Pending')
            
            # Calculate days display
            if days <= 0:
                days_display = f"READY (+{abs(days)})" if days < 0 else "READY NOW"
            else:
                days_display = f"{days} days"
            
            # Convert sell_date string to date object for DateColumn compatibility
            try:
                sell_date_obj = datetime.strptime(entry['sell_date'], "%Y-%m-%d").date()
            except:
                sell_date_obj = datetime.now().date()
            
            df_data.append({
                'id': entry['id'],
                'STATUS': status_display,
                'ACCOUNT': entry['account'],
                'TICKERS': entry['tickers'],
                'HELD IN': entry['held_in'],
                'BROKER': entry.get('broker', ''),
                'SELL DATE': sell_date_obj,
                'READY DATE': entry['target_date'],
                'DAYS': days_display,
                'NOTES': entry.get('comments', '') or ''
            })
        
        df = pd.DataFrame(df_data)
        
        # Store original for comparison
        if 'original_df' not in st.session_state:
            st.session_state.original_df = df.copy()
        
        # Column configuration for the grid
        column_config = {
            "id": None,  # Hide ID column
            "STATUS": st.column_config.SelectboxColumn(
                "STATUS",
                options=["🔴 Pending", "🟡 In Progress", "✅ Done"],
                width="small",
                required=True
            ),
            "ACCOUNT": st.column_config.TextColumn(
                "ACCOUNT",
                width="small",
                required=True
            ),
            "TICKERS": st.column_config.TextColumn(
                "TICKERS",
                width="medium",
                required=True
            ),
            "HELD IN": st.column_config.TextColumn(
                "HELD IN",
                width="small",
                required=True
            ),
            "BROKER": st.column_config.SelectboxColumn(
                "BROKER",
                options=["", "UBS", "SCHWAB", "JMS", "JANNEY", "WELLS FARGO", "MAC"],
                width="small"
            ),
            "SELL DATE": st.column_config.DateColumn(
                "SELL DATE",
                format="YYYY-MM-DD",
                width="small"
            ),
            "READY DATE": st.column_config.TextColumn(
                "READY DATE",
                width="small",
                disabled=True  # Calculated field
            ),
            "DAYS": st.column_config.TextColumn(
                "DAYS",
                width="small",
                disabled=True  # Calculated field
            ),
            "NOTES": st.column_config.TextColumn(
                "NOTES",
                width="medium"
            )
        }
        
        # The editable grid
        edited_df = st.data_editor(
            df,
            column_config=column_config,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",  # Don't allow adding rows here (use form above)
            key="entries_grid"
        )
        
        # Check for changes and update database
        # Use try-except to handle type comparison issues
        try:
            has_changes = False
            for idx in range(len(edited_df)):
                row = edited_df.iloc[idx]
                orig_row = df.iloc[idx]
                entry_id = int(row['id'])
                
                # Safely convert values to strings for comparison
                row_status = str(row['STATUS']) if row['STATUS'] is not None else ''
                orig_status = str(orig_row['STATUS']) if orig_row['STATUS'] is not None else ''
                row_account = str(row['ACCOUNT']) if row['ACCOUNT'] is not None else ''
                orig_account = str(orig_row['ACCOUNT']) if orig_row['ACCOUNT'] is not None else ''
                row_tickers = str(row['TICKERS']) if row['TICKERS'] is not None else ''
                orig_tickers = str(orig_row['TICKERS']) if orig_row['TICKERS'] is not None else ''
                row_held = str(row['HELD IN']) if row['HELD IN'] is not None else ''
                orig_held = str(orig_row['HELD IN']) if orig_row['HELD IN'] is not None else ''
                row_broker = str(row['BROKER']) if row['BROKER'] is not None else ''
                orig_broker = str(orig_row['BROKER']) if orig_row['BROKER'] is not None else ''
                row_sell = str(row['SELL DATE'])[:10] if row['SELL DATE'] is not None else ''
                orig_sell = str(orig_row['SELL DATE'])[:10] if orig_row['SELL DATE'] is not None else ''
                row_notes = str(row['NOTES']) if row['NOTES'] is not None else ''
                orig_notes = str(orig_row['NOTES']) if orig_row['NOTES'] is not None else ''
                
                # Check if status changed
                if row_status != orig_status:
                    has_changes = True
                    status_map_reverse = {'🔴 Pending': 'pending', '🟡 In Progress': 'in_progress', '✅ Done': 'completed'}
                    new_status = status_map_reverse.get(row_status, 'pending')
                    
                    # Update status directly
                    completed = 1 if new_status == 'completed' else 0
                    completed_date = datetime.now().strftime("%Y-%m-%d") if completed else None
                    
                    with db.get_connection() as conn:
                        conn.execute("""
                            UPDATE entries 
                            SET status = ?, completed = ?, completed_date = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (new_status, completed, completed_date, entry_id))
                
                # Check if other fields changed
                if (row_account != orig_account or 
                    row_tickers != orig_tickers or 
                    row_held != orig_held or
                    row_broker != orig_broker or
                    row_sell != orig_sell or
                    row_notes != orig_notes):
                    
                    has_changes = True
                    db.update_entry(
                        entry_id=entry_id,
                        account=row_account,
                        tickers=row_tickers,
                        held_in=row_held,
                        sell_date=row_sell if row_sell else orig_sell,
                        broker=row_broker,
                        comments=row_notes
                    )
            
            if has_changes:
                st.rerun()
        except Exception as e:
            st.error(f"Error updating entry: {str(e)}")
        
        # Attachment and Delete buttons row (outside the grid)
        st.markdown("##### 📎 Manage Attachments & Delete")
        
        # Create columns for attachment/delete controls
        num_entries = len(entries)
        cols_per_row = 4
        
        for i in range(0, num_entries, cols_per_row):
            cols = st.columns(cols_per_row)
            for j, col in enumerate(cols):
                if i + j < num_entries:
                    entry = entries[i + j]
                    entry_attachments = db.get_attachments(entry['id'])
                    
                    with col:
                        with st.container():
                            st.caption(f"**{entry['account']}** - {entry['tickers'][:15]}...")
                            btn_col1, btn_col2 = st.columns(2)
                            
                            with btn_col1:
                                if entry_attachments:
                                    att = entry_attachments[0]
                                    filename, ftype, fdata = db.get_attachment_data(att['id'])
                                    if fdata:
                                        st.download_button(
                                            f"📎 {len(entry_attachments)}",
                                            data=fdata,
                                            file_name=filename,
                                            mime=ftype or "application/octet-stream",
                                            key=f"dl_{entry['id']}",
                                            use_container_width=True
                                        )
                                else:
                                    st.button("📎 0", key=f"no_dl_{entry['id']}", disabled=True, use_container_width=True)
                            
                            with btn_col2:
                                if st.button("🗑️", key=f"del_{entry['id']}", use_container_width=True):
                                    db.delete_attachments_for_entry(entry["id"])
                                    db.delete_entry(entry["id"])
                                    st.rerun()
        
        st.markdown("---")
        st.caption(f">> Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    else:
        st.markdown("""
        <div style="text-align: center; padding: 40px; border: 1px solid #334155; border-radius: 10px; background: rgba(30, 41, 59, 0.5); margin: 20px 0;">
            <p style="font-size: 1.5rem; color: #f1f5f9;">
                No Active Entries
            </p>
            <p style="font-size: 1rem; color: #94a3b8;">
                Add your first entry to start tracking wash sale periods
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    # Completed Entries Section - GRID VIEW
    st.markdown("---")
    completed_entries = db.get_entries_by_status("completed")
    
    if completed_entries:
        st.markdown(f"### ✅ COMPLETED ({len(completed_entries)})")
        
        # Show/hide completed entries
        show_completed = st.checkbox("Show completed entries", value=False, key="show_completed")
        
        if show_completed:
            # Prepare DataFrame for completed entries
            completed_data = []
            for entry in completed_entries:
                completed_data.append({
                    'id': entry['id'],
                    'ACCOUNT': entry['account'],
                    'TICKERS': entry['tickers'],
                    'HELD IN': entry['held_in'],
                    'BROKER': entry.get('broker', ''),
                    'SELL DATE': entry['sell_date'],
                    'COMPLETED': entry.get('completed_date', 'N/A')
                })
            
            completed_df = pd.DataFrame(completed_data)
            
            # Column configuration for completed grid (read-only)
            completed_column_config = {
                "id": None,  # Hide ID column
                "ACCOUNT": st.column_config.TextColumn("ACCOUNT", width="small"),
                "TICKERS": st.column_config.TextColumn("TICKERS", width="medium"),
                "HELD IN": st.column_config.TextColumn("HELD IN", width="small"),
                "BROKER": st.column_config.TextColumn("BROKER", width="small"),
                "SELL DATE": st.column_config.TextColumn("SELL DATE", width="small"),
                "COMPLETED": st.column_config.TextColumn("COMPLETED", width="small")
            }
            
            # Display as read-only dataframe
            st.dataframe(
                completed_df,
                column_config=completed_column_config,
                use_container_width=True,
                hide_index=True
            )
            
            # Action buttons for completed entries
            st.markdown("##### Actions")
            cols_per_row = 6
            num_completed = len(completed_entries)
            
            for i in range(0, num_completed, cols_per_row):
                cols = st.columns(cols_per_row)
                for j, col in enumerate(cols):
                    if i + j < num_completed:
                        entry = completed_entries[i + j]
                        with col:
                            st.caption(f"{entry['account']}")
                            btn1, btn2 = st.columns(2)
                            with btn1:
                                if st.button("↩️", key=f"reopen_{entry['id']}", help="Reopen"):
                                    db.cycle_status(entry['id'])
                                    st.rerun()
                            with btn2:
                                if st.button("🗑️", key=f"del_comp_{entry['id']}", help="Delete"):
                                    db.delete_attachments_for_entry(entry["id"])
                                    db.delete_entry(entry["id"])
                                    st.rerun()
            
            st.caption(f">> {len(completed_entries)} completed entries")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #64748b; font-size: 0.8rem;">
        <strong>Quick Guide:</strong> Add entries on sell date • Click status to cycle: 🔴 Pending → 🟡 In Progress → ✅ Done • Click REFRESH to sync
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
