# etl.py
import time
from pathlib import Path

from pytubefix import YouTube
from pytubefix.exceptions import PytubeFixError

import database_manager as db

# --- Configuration ---
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True) # Ensure download directory exists
WORKER_SLEEP_INTERVAL = 10 # Seconds to wait when queue is empty

# --- Function Stubs ---
def analyze_sentiment(text: str) -> dict:
    """
    STUB: Analyzes the sentiment of a text string.
    In a real implementation, this would use a library like TextBlob,
    VADER, or a Transformers model.
    """
    if "happy" in text.lower() or "love" in text.lower():
        return {"label": "POSITIVE", "score": 0.9}
    if "sad" in text.lower() or "hate" in text.lower():
        return {"label": "NEGATIVE", "score": -0.8}
    return {"label": "NEUTRAL", "score": 0.1}

# --- Core Logic Functions ---
def parse_srt_segment(segment: str):
    """Parses a single SRT block into its components."""
    lines = segment.strip().split('\n')
    if len(lines) < 3:
        return None

    time_line = lines[1]
    text_lines = lines[2:]

    try:
        start_str, end_str = [t.strip().replace(',', '.') for t in time_line.split('-->')]
        def to_seconds(t_str):
            h, m, s_ms = t_str.split(':')
            s, ms = s_ms.split('.')
            return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0
        return {
            "start": to_seconds(start_str),
            "end": to_seconds(end_str),
            "text": " ".join(text_lines)
        }
    except ValueError:
        print(f"  [WARN] Could not parse time: {time_line}")
        return None


def process_youtube_url(url: str):
    """
    Main processing function for a single YouTube URL.
    Downloads, extracts captions, analyzes, and stores everything in the DB.
    Raises exceptions on failure.
    """
    print(f"\nProcessing URL: {url}")
    yt = YouTube(url)
    print(f"  > Title: {yt.title}")

    # 1. Attempt to get English captions
    caption = yt.captions.get_by_language_code('en') or yt.captions.get_by_language_code('a.en')

    if caption:
        print("  > Found English captions. Processing...")
        video_stream = yt.streams.get_highest_resolution()
        video_path = video_stream.download(output_path=str(DOWNLOAD_DIR))
        print(f"  > Video downloaded to: {video_path}")

        video_id = db.add_video(url, yt.title, video_path)

        srt_captions = caption.generate_srt_captions()
        srt_segments = srt_captions.strip().split('\n\n')

        for segment_str in srt_segments:
            segment = parse_srt_segment(segment_str)
            if segment:
                sentiment = analyze_sentiment(segment['text'])
                db.add_caption_segment(
                    video_id, segment['start'], segment['end'], segment['text'], sentiment
                )
        print(f"  > Stored {len(srt_segments)} caption segments in the database.")
        db.update_queue_status(url, 'completed', f'Successfully processed with {len(srt_segments)} captions.')

    else:
        print("  > No English captions found. Downloading audio only.")
        audio_stream = yt.streams.get_audio_only()
        audio_path = audio_stream.download(output_path=str(DOWNLOAD_DIR))
        print(f"  > Audio downloaded to: {audio_path}")

        video_id = db.add_video(url, yt.title, download_path=None) # No video path
        db.add_audio(video_id, audio_path)
        message = "Audio downloaded. No English captions found for analysis."
        print(f"  > {message}")
        db.update_queue_status(url, 'completed', message)

# --- Script Entry Point ---
def main():
    """Main worker loop. Continuously checks for and processes jobs from the queue."""
    print("--- YouTube ETL Worker Started ---")
    print(f"Checking for new jobs every {WORKER_SLEEP_INTERVAL} seconds...")
    db.setup_database()

    while True:
        url_to_process = db.get_next_queued_url_and_update()

        if url_to_process:
            try:
                process_youtube_url(url_to_process)
            except PytubeFixError as e:
                error_msg = f"PytubeFix error: {e}"
                print(f"  [ERROR] Could not process video. {error_msg}")
                db.update_queue_status(url_to_process, 'failed', error_msg)
            except Exception as e:
                error_msg = f"An unexpected error occurred: {e}"
                print(f"  [ERROR] {error_msg}")
                db.update_queue_status(url_to_process, 'failed', error_msg)
        else:
            # If the queue is empty, wait before checking again
            time.sleep(WORKER_SLEEP_INTERVAL)

if __name__ == "__main__":
    main()