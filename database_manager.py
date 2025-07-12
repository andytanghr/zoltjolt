# content of: database_manager.py
import sqlite3
from pathlib import Path
import datetime

# --- Configuration ---
DB_FILE = Path(__file__).parent / "project.db"
DB_TIMEOUT = 15 # seconds

# --- Database Setup ---
def setup_database():
    """
    Creates the necessary database tables if they don't already exist.
    This function is idempotent (safe to run multiple times).
    """
    db_existed = DB_FILE.exists()
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()

    # Enable foreign key support
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Videos table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY,
        youtube_url TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        download_path TEXT,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    # Audios table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS audios (
        id INTEGER PRIMARY KEY,
        video_id INTEGER NOT NULL,
        audio_path TEXT NOT NULL,
        FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE CASCADE
    );
    """)
    # Captions table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS captions (
        id INTEGER PRIMARY KEY,
        video_id INTEGER NOT NULL,
        start_time REAL NOT NULL,
        end_time REAL NOT NULL,
        text TEXT NOT NULL,
        sentiment_label TEXT,
        sentiment_score REAL,
        FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE CASCADE
    );
    """)
    # Processing Queue table
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

    if db_existed:
        print("Existing database found. Ensured all tables are present.")
    else:
        print("No database found. Created new database 'project.db' and initialized schema.")


# --- Queue Management Functions ---
def add_urls_to_queue(urls: list[str]):
    """Adds a list of URLs to the processing queue with 'queued' status."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    for url in urls:
        cursor.execute(
            "INSERT OR IGNORE INTO processing_queue (youtube_url) VALUES (?)", (url,)
        )
    conn.commit()
    conn.close()

def get_next_queued_url_and_update():
    """Atomically gets the next 'queued' URL and sets its status to 'processing'."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    cursor.execute("SELECT id, youtube_url FROM processing_queue WHERE status = 'queued' ORDER BY created_at LIMIT 1")
    job = cursor.fetchone()

    if job:
        job_id, url = job
        now = datetime.datetime.now()
        cursor.execute("UPDATE processing_queue SET status = 'processing', updated_at = ? WHERE id = ?", (now, job_id))
        conn.commit()
        conn.close()
        return url
    else:
        conn.close()
        return None

def update_queue_status(url: str, status: str, message: str = None):
    """Updates the status and message of a URL in the queue."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    now = datetime.datetime.now()
    cursor.execute(
        "UPDATE processing_queue SET status = ?, status_message = ?, updated_at = ? WHERE youtube_url = ?",
        (status, message, now, url)
    )
    conn.commit()
    conn.close()

# --- Write Functions (for etl.py) ---
def get_or_create_video(url, title, download_path):
    """
    Gets the ID of an existing video or creates a new one if not found.
    Returns the stable video ID. This is non-destructive.
    """
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    # Check if the video already exists
    cursor.execute("SELECT id FROM videos WHERE youtube_url = ?", (url,))
    video_row = cursor.fetchone()

    if video_row:
        video_id = video_row[0]
    else:
        # Insert a new video record and get its ID
        cursor.execute("INSERT INTO videos (youtube_url, title, download_path) VALUES (?, ?, ?)",
                       (url, title, download_path)) 
        video_id = cursor.lastrowid
    
    conn.commit()
    conn.close()
    return video_id

def update_video_path(video_id, download_path):
    """Updates the download_path for a given video."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    cursor.execute("UPDATE videos SET download_path = ? WHERE id = ?", (download_path, video_id))
    conn.commit()
    conn.close()

def add_audio(video_id, audio_path):
    """Adds an audio record."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO audios (video_id, audio_path) VALUES (?, ?)",
                   (video_id, audio_path))
    conn.commit()
    conn.close()

def add_caption_segment(video_id, start, end, text, sentiment):
    """Adds a single caption segment with its sentiment."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO captions (video_id, start_time, end_time, text, sentiment_label, sentiment_score)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (video_id, start, end, text, sentiment['label'], sentiment['score']))
    conn.commit()
    conn.close()

# --- NEW: Deletion Function ---
def delete_video_and_references(video_id: int):
    """
    Deletes a video and all its associated data from the database,
    and returns the local file paths of the video/audio for filesystem deletion.
    """
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    paths_to_delete = []

    # Step 1: Gather file paths and youtube_url before deleting
    cursor.execute("SELECT download_path, youtube_url FROM videos WHERE id = ?", (video_id,))
    video_info = cursor.fetchone()
    if not video_info:
        conn.close()
        return []

    video_path, youtube_url = video_info
    if video_path:
        paths_to_delete.append(Path(video_path))

    cursor.execute("SELECT audio_path FROM audios WHERE video_id = ?", (video_id,))
    audio_rows = cursor.fetchall()
    for row in audio_rows:
        if row[0]:
            paths_to_delete.append(Path(row[0]))

    # Step 2: Delete database records
    # With "ON DELETE CASCADE" enabled, we only need to delete the video record.
    # The related captions and audios will be deleted automatically.
    cursor.execute("DELETE FROM videos WHERE id = ?", (video_id,))
    
    # Also remove it from the queue
    if youtube_url:
        cursor.execute("DELETE FROM processing_queue WHERE youtube_url = ?", (youtube_url,))

    conn.commit()
    conn.close()
    
    return paths_to_delete

# --- Raw Table Read Functions (for app.py inspector) ---
def get_all_from_table(table_name: str):
    """A generic function to fetch all rows from a given table."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    results = cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC;").fetchall()
    conn.close()
    return results

# --- Read Functions (for app.py) ---
def get_all_videos_with_status():
    """
    Returns a list of all submitted videos and their current status.
    """
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
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
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    captions = cursor.execute("SELECT start_time, end_time, text, sentiment_label, sentiment_score FROM captions WHERE video_id = ? ORDER BY start_time ASC;", (video_id,)).fetchall()
    conn.close()
    return captions

def requeue_stale_jobs(timeout_minutes=60):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE processing_queue
        SET status = 'queued', status_message = 'Re-queued after timeout'
        WHERE status = 'processing'
          AND (strftime('%s', 'now') - strftime('%s', updated_at)) > ?
    """, (timeout_minutes * 60,))
    conn.commit()
    conn.close()