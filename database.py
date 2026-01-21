"""
Database operations for Tax Loss Harvesting Tracker.
Uses SQLite for persistent storage with concurrent access support.
"""

import sqlite3
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Optional
import os

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
        # Create index for faster queries
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_target_date ON entries(target_date)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_completed ON entries(completed)
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
        status: 'waiting', 'ready', 'completed', or 'all'
    """
    today = datetime.now().date()
    
    with get_connection() as conn:
        if status == "completed":
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE completed = 1
                ORDER BY completed_date DESC
            """).fetchall()
        elif status == "waiting":
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE completed = 0 AND target_date > ?
                ORDER BY target_date ASC
            """, (today.strftime("%Y-%m-%d"),)).fetchall()
        elif status == "ready":
            rows = conn.execute("""
                SELECT * FROM entries 
                WHERE completed = 0 AND target_date <= ?
                ORDER BY target_date ASC
            """, (today.strftime("%Y-%m-%d"),)).fetchall()
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
    
    with get_connection() as conn:
        conn.execute("""
            UPDATE entries 
            SET completed = ?, completed_date = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (1 if completed else 0, completed_date, entry_id))


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


# Initialize database on module import
init_database()

