"""
Tax Loss Harvesting Tracker - MATRIX MODE
A multi-user tool for tracking 30-day wash sale periods.
"""

import streamlit as st
from datetime import datetime, timedelta
import database as db
import time

# Page configuration
st.set_page_config(
    page_title="Tax Loss Harvest Tracker",
    page_icon="💊",
    layout="wide",
)

# Matrix Theme CSS (matching Proxy Voting style)
MATRIX_CSS = """
<style>
    /* Import Matrix-style font */
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
    
    /* Remove top padding/margin */
    .block-container {
        padding-top: 1rem !important;
    }
    
    header[data-testid="stHeader"] {
        background: transparent !important;
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
        background: linear-gradient(180deg, #000000 0%, #001a00 50%, #000000 100%);
    }
    
    /* All text in Matrix green */
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
        color: #00ff41 !important;
        font-family: 'Share Tech Mono', 'Courier New', monospace !important;
    }
    
    /* Headers */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #00ff41 !important;
        text-shadow: 0 0 10px #00ff41, 0 0 20px #00ff41, 0 0 30px #00ff41;
        margin-bottom: 0.25rem;
        margin-top: 0 !important;
        padding-top: 0 !important;
        font-family: 'Share Tech Mono', monospace !important;
        letter-spacing: 3px;
    }
    
    .sub-header {
        font-size: 1rem;
        color: #008f11 !important;
        margin-bottom: 1.5rem;
        font-family: 'Share Tech Mono', monospace !important;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #000000 0%, #001100 100%) !important;
        border-right: 1px solid #00ff41 !important;
    }
    
    [data-testid="stSidebar"] * {
        color: #00ff41 !important;
    }
    
    /* Metric cards */
    [data-testid="stMetric"] {
        background: rgba(0, 255, 65, 0.05) !important;
        border: 1px solid #00ff41 !important;
        border-radius: 5px !important;
        padding: 15px !important;
        box-shadow: 0 0 10px rgba(0, 255, 65, 0.3) !important;
    }
    
    [data-testid="stMetric"] label {
        color: #008f11 !important;
    }
    
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #00ff41 !important;
        text-shadow: 0 0 5px #00ff41 !important;
    }
    
    [data-testid="stMetric"] [data-testid="stMetricDelta"] {
        color: #39ff14 !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: transparent !important;
        border-bottom: 1px solid #00ff41 !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        color: #008f11 !important;
        background-color: transparent !important;
        border: 1px solid #003300 !important;
        border-bottom: none !important;
    }
    
    .stTabs [aria-selected="true"] {
        color: #00ff41 !important;
        background-color: rgba(0, 255, 65, 0.1) !important;
        border: 1px solid #00ff41 !important;
        border-bottom: none !important;
        text-shadow: 0 0 5px #00ff41 !important;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(180deg, #003300 0%, #001a00 100%) !important;
        color: #00ff41 !important;
        border: 1px solid #00ff41 !important;
        font-family: 'Share Tech Mono', monospace !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button:hover {
        background: linear-gradient(180deg, #004400 0%, #002200 100%) !important;
        box-shadow: 0 0 15px #00ff41 !important;
        text-shadow: 0 0 5px #00ff41 !important;
    }
    
    /* Input fields */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div,
    .stDateInput > div > div > input {
        background: #001a00 !important;
        border: 1px solid #00ff41 !important;
        color: #00ff41 !important;
        font-family: 'Share Tech Mono', monospace !important;
    }
    
    /* Success/Info/Warning/Error boxes */
    .stSuccess {
        background-color: rgba(0, 255, 65, 0.1) !important;
        border: 1px solid #00ff41 !important;
        color: #00ff41 !important;
    }
    
    .stInfo {
        background-color: rgba(0, 143, 17, 0.2) !important;
        border: 1px solid #008f11 !important;
        color: #00ff41 !important;
    }
    
    .stWarning {
        background-color: rgba(255, 200, 0, 0.1) !important;
        border: 1px solid #ffc800 !important;
        color: #ffc800 !important;
    }
    
    .stError {
        background-color: rgba(255, 0, 0, 0.1) !important;
        border: 1px solid #ff0040 !important;
        color: #ff0040 !important;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: rgba(0, 255, 65, 0.05) !important;
        border: 1px solid #003300 !important;
        color: #00ff41 !important;
    }
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #001100;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #00ff41;
        border-radius: 4px;
    }
    
    /* HR/Dividers */
    hr {
        border-color: #003300 !important;
    }
    
    /* Alert box for ACTION REQUIRED */
    .action-alert {
        background: rgba(255, 0, 64, 0.1) !important;
        border: 2px solid #ff0040 !important;
        border-radius: 10px !important;
        padding: 20px !important;
        margin: 15px 0 !important;
        box-shadow: 0 0 20px rgba(255, 0, 64, 0.3) !important;
    }
    
    .action-alert-header {
        color: #ff0040 !important;
        font-size: 1.3rem !important;
        font-weight: bold !important;
        text-shadow: 0 0 10px #ff0040 !important;
        margin-bottom: 10px !important;
    }
    
    .action-item {
        background: rgba(255, 0, 64, 0.05) !important;
        border-left: 3px solid #ff0040 !important;
        padding: 10px 15px !important;
        margin: 5px 0 !important;
        color: #00ff41 !important;
    }
    
    /* Checkbox styling */
    .stCheckbox label span {
        color: #00ff41 !important;
    }
    
    /* Code blocks - inline */
    code {
        background: rgba(0, 255, 65, 0.1) !important;
        color: #00ff41 !important;
        border: 1px solid #003300 !important;
        padding: 2px 6px !important;
    }
    
    /* Streamlit code blocks (st.code) */
    [data-testid="stCode"],
    .stCode,
    pre {
        background: #001a00 !important;
        border: 1px solid #00ff41 !important;
    }
    
    [data-testid="stCode"] code,
    .stCode code,
    pre code {
        background: transparent !important;
        color: #00ff41 !important;
        font-family: 'Share Tech Mono', monospace !important;
    }
    
    /* Code copy button */
    [data-testid="stCode"] button {
        display: none !important;
    }
    
    /* File uploader styling */
    [data-testid="stFileUploader"] {
        background: rgba(0, 255, 65, 0.02) !important;
        border: 1px dashed #00ff41 !important;
        border-radius: 5px !important;
        padding: 10px !important;
    }
    
    [data-testid="stFileUploader"] label {
        color: #00ff41 !important;
    }
    
    [data-testid="stFileUploader"] section {
        background: transparent !important;
    }
    
    [data-testid="stFileUploader"] button {
        background: #001a00 !important;
        color: #00ff41 !important;
        border: 1px solid #00ff41 !important;
    }
    
    /* Download button styling */
    .stDownloadButton > button {
        background: linear-gradient(180deg, #003300 0%, #001a00 100%) !important;
        color: #00ff41 !important;
        border: 1px solid #00ff41 !important;
        font-family: 'Share Tech Mono', monospace !important;
    }
    
    /* Expander styling */
    [data-testid="stExpander"] {
        background: rgba(0, 255, 65, 0.02) !important;
        border: 1px solid #003300 !important;
    }
    
    [data-testid="stExpander"] summary {
        color: #00ff41 !important;
    }
    
    /* Glowing border animation */
    .glow-box {
        animation: glow 2s ease-in-out infinite alternate;
    }
    
    @keyframes glow {
        from { box-shadow: 0 0 5px #00ff41, 0 0 10px #00ff41; }
        to { box-shadow: 0 0 10px #00ff41, 0 0 20px #00ff41, 0 0 30px #00ff41; }
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; text-shadow: 0 0 10px #00ff41, 0 0 20px #00ff41; }
        50% { opacity: 0.7; text-shadow: 0 0 5px #00ff41, 0 0 10px #00ff41; }
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
    # Apply Matrix theme
    st.markdown(MATRIX_CSS, unsafe_allow_html=True)
    
    # Header
    st.markdown('<p class="main-header">💊 TAX LOSS HARVEST TRACKER</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">>> Track 30-day wash sale periods // Multi-user access enabled</p>', unsafe_allow_html=True)
    
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
        hcol1, hcol2, hcol3, hcol4 = st.columns([0.5, 7.5, 1, 1])
        with hcol3:
            st.markdown("**DOCS**")
        with hcol4:
            st.markdown("**DEL**")
        
        # Show ready accounts with checkboxes
        for entry in ready_entries:
            # Get attachments for this entry
            attachments = db.get_attachments(entry['id'])
            
            col1, col2, col3, col4 = st.columns([0.5, 7.5, 1, 1])
            
            with col1:
                is_done = st.checkbox(
                    "X",
                    value=bool(entry["completed"]),
                    key=f"check_{entry['id']}",
                    label_visibility="collapsed"
                )
                if is_done != bool(entry["completed"]):
                    db.mark_completed(entry["id"], is_done)
                    st.rerun()
            
            with col2:
                days = calculate_days_remaining(entry["target_date"])
                day_text = ">> TODAY <<" if days == 0 else f"+{abs(days)} DAYS OVERDUE"
                broker_text = entry.get('broker', '')
                acct_count = alert_account_counts.get(entry['account'].upper(), 1)
                count_text = f" (x{acct_count})" if acct_count > 1 else ""
                st.markdown(f"""
                <div class="action-item">
                    <strong style="color: #ff0040;">ACCT: {entry['account']}{count_text}</strong> | 
                    BROKER: {broker_text} |
                    TICKERS: {entry['tickers']} | 
                    HELD: {entry['held_in']} | 
                    <span style="color: #ff0040;">{day_text}</span>
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
    
    # Active Entries Table (non-completed only)
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
    
    # Display entries
    if entries:
        st.caption(f">> Displaying {len(entries)} entries")
        
        # Get account entry counts
        account_counts = db.get_all_account_counts()
        
        # Column headers
        hcol1, hcol2, hcol3, hcol4, hcol5, hcol6, hcol7, hcol8, hcol9, hcol10, hcol11 = st.columns([0.4, 1.2, 1.6, 0.9, 0.9, 0.9, 0.9, 0.5, 0.7, 0.6, 0.4])
        with hcol1:
            st.markdown("**✓**")
        with hcol2:
            st.markdown("**ACCT**")
        with hcol3:
            st.markdown("**TICKERS**")
        with hcol4:
            st.markdown("**HELD**")
        with hcol5:
            st.markdown("**BROKER**")
        with hcol6:
            st.markdown("**SOLD**")
        with hcol7:
            st.markdown("**READY**")
        with hcol8:
            st.markdown("**DAYS**")
        with hcol9:
            st.markdown("**STATUS**")
        with hcol10:
            st.markdown("**DOCS**")
        with hcol11:
            st.markdown("**DEL**")
        
        st.markdown("---")
        
        for entry in entries:
            days = calculate_days_remaining(entry["target_date"])
            status = get_status_display(days, bool(entry["completed"]))
            
            # Format days display
            if entry["completed"]:
                days_display = "---"
            elif days == 0:
                days_display = "NOW!"
            elif days < 0:
                days_display = f"+{abs(days)}"
            else:
                days_display = str(days)
            
            # Get attachments for this entry
            entry_attachments = db.get_attachments(entry['id'])
            
            col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11 = st.columns([0.4, 1.2, 1.6, 0.9, 0.9, 0.9, 0.9, 0.5, 0.7, 0.6, 0.4])
            
            with col1:
                is_completed = st.checkbox(
                    "Done",
                    value=bool(entry["completed"]),
                    key=f"table_check_{entry['id']}",
                    label_visibility="collapsed"
                )
                if is_completed != bool(entry["completed"]):
                    db.mark_completed(entry["id"], is_completed)
                    st.rerun()
            
            with col2:
                # Show account with entry count
                acct_upper = entry['account'].upper()
                entry_count = account_counts.get(acct_upper, 1)
                if entry_count > 1:
                    st.markdown(f"**{entry['account']}** :orange[(x{entry_count})]")
                else:
                    st.markdown(f"**{entry['account']}**")
            
            with col3:
                st.code(entry['tickers'])
            
            with col4:
                st.markdown(entry["held_in"])
            
            with col5:
                st.markdown(entry.get("broker", ""))
            
            with col6:
                st.markdown(entry["sell_date"])
            
            with col7:
                st.markdown(entry["target_date"])
            
            with col8:
                if days_display == "NOW!":
                    st.markdown(f"**:red[{days_display}]**")
                elif status == "READY":
                    st.markdown(f":green[{days_display}]")
                elif status == "WAITING":
                    st.markdown(f":orange[{days_display}]")
                else:
                    st.markdown(days_display)
            
            with col9:
                if status == "READY":
                    st.markdown(":green[READY]")
                elif status == "WAITING":
                    st.markdown(":orange[WAITING]")
                else:
                    st.markdown(":gray[DONE]")
            
            with col10:
                # Direct download button for attachments
                if entry_attachments:
                    att = entry_attachments[0]  # Most recent attachment
                    filename, ftype, fdata = db.get_attachment_data(att['id'])
                    if fdata:
                        st.download_button(
                            f"📎({len(entry_attachments)})" if len(entry_attachments) > 1 else "📎",
                            data=fdata,
                            file_name=filename,
                            mime=ftype or "application/octet-stream",
                            key=f"dl_{entry['id']}",
                            help=f"Download: {filename}"
                        )
                else:
                    st.markdown(":gray[—]")
            
            with col11:
                if st.button("X", key=f"del_{entry['id']}"):
                    db.delete_attachments_for_entry(entry["id"])  # Delete attachments first
                    db.delete_entry(entry["id"])
                    st.rerun()
        
        st.markdown("---")
        st.caption(f">> Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    else:
        st.markdown("""
        <div style="text-align: center; padding: 40px; border: 1px solid #00ff41; border-radius: 10px; background: rgba(0,255,65,0.02); margin: 20px 0;">
            <p style="font-size: 1.5rem; color: #00ff41; text-shadow: 0 0 10px #00ff41;">
                NO ACTIVE ENTRIES
            </p>
            <p style="font-size: 1rem; color: #008f11;">
                "Add your first entry to enter the Matrix..."
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    # Completed Entries Section
    st.markdown("---")
    completed_entries = db.get_entries_by_status("completed")
    
    if completed_entries:
        st.markdown(f"### ✅ COMPLETED ({len(completed_entries)})")
        
        # Show/hide completed entries
        show_completed = st.checkbox("Show completed entries", value=False, key="show_completed")
        
        if show_completed:
            # Get account counts
            completed_account_counts = db.get_all_account_counts()
            
            for entry in completed_entries:
                col1, col2, col3, col4, col5, col6, col7 = st.columns([0.4, 1, 1.8, 1, 1, 1.5, 0.5])
                
                with col1:
                    # Checkbox to mark as NOT completed (undo)
                    is_completed = st.checkbox(
                        "Done",
                        value=True,
                        key=f"completed_check_{entry['id']}",
                        label_visibility="collapsed"
                    )
                    if not is_completed:
                        db.mark_completed(entry["id"], False)
                        st.rerun()
                
                with col2:
                    acct_count = completed_account_counts.get(entry['account'].upper(), 1)
                    count_text = f" (x{acct_count})" if acct_count > 1 else ""
                    st.markdown(f":gray[{entry['account']}{count_text}]")
                
                with col3:
                    st.markdown(f":gray[{entry['tickers']}]")
                
                with col4:
                    st.markdown(f":gray[{entry['held_in']}]")
                
                with col5:
                    st.markdown(f":gray[{entry.get('broker', '')}]")
                
                with col6:
                    completed_date = entry.get('completed_date', 'N/A')
                    st.markdown(f":gray[Done: {completed_date}]")
                
                with col7:
                    if st.button("X", key=f"del_completed_{entry['id']}"):
                        db.delete_entry(entry["id"])
                        st.rerun()
            
            st.caption(f">> {len(completed_entries)} completed entries")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #008f11; font-size: 0.8rem;">
        <strong>SYSTEM GUIDE:</strong> Add entries on sell date // Checkbox when buyback complete // Click REFRESH to sync with other users
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
