# database_manager.py
import sqlite3
from pathlib import Path
import datetime

# --- Configuration ---
DB_FILE = Path(__file__).parent / "project.db"

# --- Database Setup ---
def setup_database():
    """
    Creates the necessary database tables if they don't already exist.
    This function is idempotent (safe to run multiple times).
    """
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()

    # Videos table: Stores metadata for each successfully processed YouTube video
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY,
        youtube_url TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        download_path TEXT,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Audios table: Stores metadata for downloaded audio files (used when no captions)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS audios (
        id INTEGER PRIMARY KEY,
        video_id INTEGER NOT NULL,
        audio_path TEXT NOT NULL,
        FOREIGN KEY (video_id) REFERENCES videos (id)
    );
    """)

    # Captions table: Stores each segment of an SRT caption
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS captions (
        id INTEGER PRIMARY KEY,
        video_id INTEGER NOT NULL,
        start_time REAL NOT NULL,
        end_time REAL NOT NULL,
        text TEXT NOT NULL,
        sentiment_label TEXT,
        sentiment_score REAL,
        FOREIGN KEY (video_id) REFERENCES videos (id)
    );
    """)

    # NEW: Processing Queue table to manage ETL jobs
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processing_queue (
        id INTEGER PRIMARY KEY,
        youtube_url TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'queued', -- 'queued', 'processing', 'completed', 'failed'
        status_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
    );
    """)

    conn.commit()
    conn.close()
    print("Database setup complete.")

# --- Queue Management Functions ---
def add_urls_to_queue(urls: list[str]):
    """Adds a list of URLs to the processing queue with 'queued' status."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    # Use INSERT OR IGNORE to avoid errors on duplicate URLs
    for url in urls:
        cursor.execute(
            "INSERT OR IGNORE INTO processing_queue (youtube_url) VALUES (?)", (url,)
        )
    conn.commit()
    conn.close()

def get_next_queued_url_and_update():
    """Atomically gets the next 'queued' URL and sets its status to 'processing'."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    # Find the oldest queued item
    cursor.execute("SELECT id, youtube_url FROM processing_queue WHERE status = 'queued' ORDER BY created_at LIMIT 1")
    job = cursor.fetchone()

    if job:
        job_id, url = job
        now = datetime.datetime.now()
        # Update its status to 'processing'
        cursor.execute("UPDATE processing_queue SET status = 'processing', updated_at = ? WHERE id = ?", (now, job_id))
        conn.commit()
        conn.close()
        return url
    else:
        conn.close()
        return None

def update_queue_status(url: str, status: str, message: str = None):
    """Updates the status and message of a URL in the queue."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    now = datetime.datetime.now()
    cursor.execute(
        "UPDATE processing_queue SET status = ?, status_message = ?, updated_at = ? WHERE youtube_url = ?",
        (status, message, now, url)
    )
    conn.commit()
    conn.close()

# --- Write Functions (for etl.py) ---
def add_video(url, title, download_path):
    """Adds a video record and returns its new ID."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO videos (youtube_url, title, download_path) VALUES (?, ?, ?)",
                   (url, title, download_path))
    # Get the ID, whether it was new or replaced
    cursor.execute("SELECT id FROM videos WHERE youtube_url = ?", (url,))
    video_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return video_id

def add_audio(video_id, audio_path):
    """Adds an audio record."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO audios (video_id, audio_path) VALUES (?, ?)",
                   (video_id, audio_path))
    conn.commit()
    conn.close()

def add_caption_segment(video_id, start, end, text, sentiment):
    """Adds a single caption segment with its sentiment."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO captions (video_id, start_time, end_time, text, sentiment_label, sentiment_score)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (video_id, start, end, text, sentiment['label'], sentiment['score']))
    conn.commit()
    conn.close()


# --- Read Functions (for app.py) ---
def get_all_videos_with_status():
    """
    Returns a list of all submitted videos and their current status,
    joining the queue and videos tables.
    """
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = """
    SELECT
        q.youtube_url,
        q.status,
        q.status_message,
        q.updated_at,
        v.id as video_id,
        v.title
    FROM processing_queue q
    LEFT JOIN videos v ON q.youtube_url = v.youtube_url
    ORDER BY q.created_at DESC;
    """
    results = cursor.execute(query).fetchall()
    conn.close()
    return results

def get_captions_for_video(video_id):
    """Returns all caption segments for a given video ID."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    captions = cursor.execute("SELECT start_time, end_time, text, sentiment_label, sentiment_score FROM captions WHERE video_id = ? ORDER BY start_time ASC;", (video_id,)).fetchall()
    conn.close()
    return captions