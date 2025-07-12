# database_manager.py
import sqlite3
from pathlib import Path

# --- Configuration ---
DB_FILE = Path(__file__).parent / "project.db"

# --- Database Setup ---
def setup_database():
    """
    Creates the necessary database tables if they don't already exist.
    This function is idempotent (safe to run multiple times).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Videos table: Stores metadata for each processed YouTube video
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
    
    conn.commit()
    conn.close()
    print("Database setup complete.")

# --- Write Functions (for etl.py) ---
def add_video(url, title, download_path):
    """Adds a video record and returns its new ID."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO videos (youtube_url, title, download_path) VALUES (?, ?, ?)",
                   (url, title, download_path))
    video_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return video_id

def add_audio(video_id, audio_path):
    """Adds an audio record."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO audios (video_id, audio_path) VALUES (?, ?)",
                   (video_id, audio_path))
    conn.commit()
    conn.close()

def add_caption_segment(video_id, start, end, text, sentiment):
    """Adds a single caption segment with its sentiment."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO captions (video_id, start_time, end_time, text, sentiment_label, sentiment_score)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (video_id, start, end, text, sentiment['label'], sentiment['score']))
    conn.commit()
    conn.close()

# --- Read Functions (for app.py) ---
def get_all_processed_videos():
    """Returns a list of all videos from the database."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Makes rows accessible by column name
    cursor = conn.cursor()
    videos = cursor.execute("SELECT id, title, youtube_url, processed_at FROM videos ORDER BY processed_at DESC;").fetchall()
    conn.close()
    return videos

def get_captions_for_video(video_id):
    """Returns all caption segments for a given video ID."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    captions = cursor.execute("SELECT start_time, end_time, text, sentiment_label, sentiment_score FROM captions WHERE video_id = ? ORDER BY start_time ASC;", (video_id,)).fetchall()
    conn.close()
    return captions