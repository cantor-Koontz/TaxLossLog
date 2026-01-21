"""
Database operations for Tax Loss Harvesting Tracker.
Uses SQLite for persistent storage with concurrent access support.
"""

import sqlite3
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Optional
import os
import base64

DATABASE_PATH = os.path.join(os.path.dirname(__file__), "tax_loss.db")


@contextmanager
def get_connection():
    """Context manager for database connections with proper locking."""
    conn = sqlite3.connect(DATABASE_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent access
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_database():
    """Initialize the database and create tables if they don't exist."""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                tickers TEXT NOT NULL,
                held_in TEXT NOT NULL,
                broker TEXT DEFAULT '',
                sell_date DATE NOT NULL,
                target_date DATE NOT NULL,
                comments TEXT,
                completed INTEGER DEFAULT 0,
                completed_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Add broker column if it doesn't exist (for existing databases)
        try:
            conn.execute("ALTER TABLE entries ADD COLUMN broker TEXT DEFAULT ''")
        except:
            pass  # Column already exists
        
        # Add status column for 3-stage workflow: pending -> in_progress -> completed
        try:
            conn.execute("ALTER TABLE entries ADD COLUMN status TEXT DEFAULT 'pending'")
            # Migrate existing data: completed=1 -> status='completed', else 'pending'
            conn.execute("UPDATE entries SET status = 'completed' WHERE completed = 1 AND (status IS NULL OR status = 'pending')")
            conn.execute("UPDATE entries SET status = 'pending' WHERE completed = 0 AND status IS NULL")
        except:
            pass  # Column already exists
        # Create index for faster queries
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_target_date ON entries(target_date)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_completed ON entries(completed)
        """)
        
        # Attachments table for file storage
        conn.execute("""
            CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                file_type TEXT,
                file_data TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (entry_id) REFERENCES entries(id) ON DELETE CASCADE
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_attachment_entry ON attachments(entry_id)
        """)


def adjust_for_weekend(dt: datetime) -> datetime:
    """Adjust date to next business day if it falls on a weekend."""
    # Saturday = 5, Sunday = 6
    if dt.weekday() == 5:  # Saturday
        return dt + timedelta(days=2)  # Move to Monday
    elif dt.weekday() == 6:  # Sunday
        return dt + timedelta(days=1)  # Move to Monday
    return dt


def add_entry(account: str, tickers: str, held_in: str, sell_date: str, broker: str = "", comments: str = "") -> int:
    """
    Add a new tax loss harvesting entry.
    
    Args:
        account: Account number
        tickers: Comma-separated list of tickers sold
        held_in: What the funds are held in (ETF name or 'Cash')
        sell_date: Date of sale (YYYY-MM-DD format)
        broker: Broker name (UBS, SCHWAB, JMS, JANNEY, Wells Fargo, MAC)
        comments: Optional comments
        
    Returns:
        The ID of the newly created entry
    """
    sell_dt = datetime.strptime(sell_date, "%Y-%m-%d")
    target_dt = sell_dt + timedelta(days=31)
    target_dt = adjust_for_weekend(target_dt)  # Move weekend dates to Monday
    target_date = target_dt.strftime("%Y-%m-%d")
    
    with get_connection() as conn:
        cursor = conn.execute("""
            INSERT INTO entries (account, tickers, held_in, broker, sell_date, target_date, comments)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (account, tickers.upper(), held_in.upper(), broker, sell_date, target_date, comments))
        return cursor.lastrowid


def get_all_entries() -> list[dict]:
    """Get all entries from the database."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM entries 
            ORDER BY target_date ASC, account ASC
        """).fetchall()
        return [dict(row) for row in rows]


def get_entries_by_status(status: str) -> list[dict]:
    """
    Get entries filtered by status.
    
    Args:
        status: 'waiting', 'ready', 'completed', 'pending', 'in_progress', or 'all'
    """
    today = datetime.now().date()
    
    with get_connection() as conn:
        if status == "completed":
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE status = 'completed'
                ORDER BY completed_date DESC
            """).fetchall()
        elif status == "waiting":
            # Not completed AND target date in future
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE status != 'completed' AND target_date > ?
                ORDER BY target_date ASC
            """, (today.strftime("%Y-%m-%d"),)).fetchall()
        elif status == "ready":
            # Not completed AND target date reached/passed
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE status != 'completed' AND target_date <= ?
                ORDER BY target_date ASC
            """, (today.strftime("%Y-%m-%d"),)).fetchall()
        elif status == "pending":
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE status = 'pending'
                ORDER BY target_date ASC
            """).fetchall()
        elif status == "in_progress":
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE status = 'in_progress'
                ORDER BY target_date ASC
            """).fetchall()
        else:  # all
            rows = conn.execute("""
                SELECT * FROM entries 
                ORDER BY target_date ASC
            """).fetchall()
        
        return [dict(row) for row in rows]


def get_entries_due_today() -> list[dict]:
    """Get entries that are due today (30-day period ends today)."""
    today = datetime.now().strftime("%Y-%m-%d")
    
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM entries 
            WHERE target_date = ? AND completed = 0
            ORDER BY account ASC
        """, (today,)).fetchall()
        return [dict(row) for row in rows]


def get_entries_due_range(start_date: str, end_date: str) -> list[dict]:
    """Get entries due within a date range."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM entries 
            WHERE target_date BETWEEN ? AND ? AND completed = 0
            ORDER BY target_date ASC, account ASC
        """, (start_date, end_date)).fetchall()
        return [dict(row) for row in rows]


def mark_completed(entry_id: int, completed: bool = True):
    """Mark an entry as completed or not completed."""
    completed_date = datetime.now().strftime("%Y-%m-%d") if completed else None
    status = 'completed' if completed else 'pending'
    
    with get_connection() as conn:
        conn.execute("""
            UPDATE entries 
            SET completed = ?, completed_date = ?, status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (1 if completed else 0, completed_date, status, entry_id))


def cycle_status(entry_id: int) -> str:
    """
    Cycle entry status: pending -> in_progress -> completed
    Returns the new status.
    """
    with get_connection() as conn:
        # Get current status
        row = conn.execute("SELECT status FROM entries WHERE id = ?", (entry_id,)).fetchone()
        current_status = row['status'] if row and row['status'] else 'pending'
        
        # Cycle to next status
        if current_status == 'pending':
            new_status = 'in_progress'
            completed = 0
            completed_date = None
        elif current_status == 'in_progress':
            new_status = 'completed'
            completed = 1
            completed_date = datetime.now().strftime("%Y-%m-%d")
        else:  # completed -> back to pending
            new_status = 'pending'
            completed = 0
            completed_date = None
        
        conn.execute("""
            UPDATE entries 
            SET status = ?, completed = ?, completed_date = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (new_status, completed, completed_date, entry_id))
        
        return new_status


def get_entry_status(entry_id: int) -> str:
    """Get the current status of an entry."""
    with get_connection() as conn:
        row = conn.execute("SELECT status FROM entries WHERE id = ?", (entry_id,)).fetchone()
        return row['status'] if row and row['status'] else 'pending'


def update_entry(entry_id: int, account: str, tickers: str, held_in: str, 
                 sell_date: str, broker: str = "", comments: str = ""):
    """Update an existing entry."""
    sell_dt = datetime.strptime(sell_date, "%Y-%m-%d")
    target_dt = sell_dt + timedelta(days=31)
    target_dt = adjust_for_weekend(target_dt)  # Move weekend dates to Monday
    target_date = target_dt.strftime("%Y-%m-%d")
    
    with get_connection() as conn:
        conn.execute("""
            UPDATE entries 
            SET account = ?, tickers = ?, held_in = ?, broker = ?, sell_date = ?, 
                target_date = ?, comments = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (account, tickers.upper(), held_in.upper(), broker, sell_date, target_date, comments, entry_id))


def delete_entry(entry_id: int):
    """Delete an entry from the database."""
    with get_connection() as conn:
        conn.execute("DELETE FROM entries WHERE id = ?", (entry_id,))


def get_stats() -> dict:
    """Get summary statistics."""
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    week_end = (today + timedelta(days=7)).strftime("%Y-%m-%d")
    
    with get_connection() as conn:
        # Waiting (not yet at 30 days)
        waiting = conn.execute("""
            SELECT COUNT(*) FROM entries 
            WHERE completed = 0 AND target_date > ?
        """, (today_str,)).fetchone()[0]
        
        # Ready (at or past 30 days, not completed)
        ready = conn.execute("""
            SELECT COUNT(*) FROM entries 
            WHERE completed = 0 AND target_date <= ?
        """, (today_str,)).fetchone()[0]
        
        # Due today
        due_today = conn.execute("""
            SELECT COUNT(*) FROM entries 
            WHERE completed = 0 AND target_date = ?
        """, (today_str,)).fetchone()[0]
        
        # Due this week
        due_week = conn.execute("""
            SELECT COUNT(*) FROM entries 
            WHERE completed = 0 AND target_date BETWEEN ? AND ?
        """, (today_str, week_end)).fetchone()[0]
        
        # Completed
        completed = conn.execute("""
            SELECT COUNT(*) FROM entries WHERE completed = 1
        """).fetchone()[0]
        
        # Total
        total = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        
        return {
            "waiting": waiting,
            "ready": ready,
            "due_today": due_today,
            "due_week": due_week,
            "completed": completed,
            "total": total
        }


def search_entries(query: str) -> list[dict]:
    """Search entries by account number or ticker."""
    query = f"%{query.upper()}%"
    
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM entries 
            WHERE UPPER(account) LIKE ? OR UPPER(tickers) LIKE ?
            ORDER BY target_date ASC
        """, (query, query)).fetchall()
        return [dict(row) for row in rows]


def get_account_entry_count(account: str) -> int:
    """Get the number of times an account has been entered."""
    with get_connection() as conn:
        result = conn.execute("""
            SELECT COUNT(*) FROM entries 
            WHERE UPPER(account) = UPPER(?)
        """, (account,)).fetchone()
        return result[0] if result else 0


def get_all_account_counts() -> dict:
    """Get entry counts for all accounts."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT UPPER(account) as account, COUNT(*) as count 
            FROM entries 
            GROUP BY UPPER(account)
        """).fetchall()
        return {row['account']: row['count'] for row in rows}


# ============== ATTACHMENT FUNCTIONS ==============

def add_attachment(entry_id: int, filename: str, file_type: str, file_data: bytes) -> int:
    """
    Add an attachment to an entry.
    
    Args:
        entry_id: The ID of the entry to attach to
        filename: Original filename
        file_type: MIME type of the file
        file_data: Raw file bytes
        
    Returns:
        The ID of the newly created attachment
    """
    encoded_data = base64.b64encode(file_data).decode('utf-8')
    
    with get_connection() as conn:
        cursor = conn.execute("""
            INSERT INTO attachments (entry_id, filename, file_type, file_data)
            VALUES (?, ?, ?, ?)
        """, (entry_id, filename, file_type, encoded_data))
        return cursor.lastrowid


def get_attachments(entry_id: int) -> list[dict]:
    """Get all attachments for an entry (metadata only, no file data)."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, entry_id, filename, file_type, uploaded_at 
            FROM attachments WHERE entry_id = ?
            ORDER BY uploaded_at DESC
        """, (entry_id,)).fetchall()
        return [dict(row) for row in rows]


def get_attachment_count(entry_id: int) -> int:
    """Get the number of attachments for an entry."""
    with get_connection() as conn:
        result = conn.execute("""
            SELECT COUNT(*) FROM attachments WHERE entry_id = ?
        """, (entry_id,)).fetchone()
        return result[0] if result else 0


def get_attachment_data(attachment_id: int) -> tuple:
    """
    Get attachment file data for download.
    
    Returns:
        Tuple of (filename, file_type, file_bytes) or (None, None, None) if not found
    """
    with get_connection() as conn:
        row = conn.execute("""
            SELECT filename, file_type, file_data 
            FROM attachments WHERE id = ?
        """, (attachment_id,)).fetchone()
        if row:
            return row['filename'], row['file_type'], base64.b64decode(row['file_data'])
        return None, None, None


def delete_attachment(attachment_id: int):
    """Delete an attachment."""
    with get_connection() as conn:
        conn.execute("DELETE FROM attachments WHERE id = ?", (attachment_id,))


def delete_attachments_for_entry(entry_id: int):
    """Delete all attachments for an entry."""
    with get_connection() as conn:
        conn.execute("DELETE FROM attachments WHERE entry_id = ?", (entry_id,))


# Initialize database on module import
init_database()

