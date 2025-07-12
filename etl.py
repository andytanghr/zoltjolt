# etl.py
import time
from pathlib import Path

from pytubefix import YouTube
from pytubefix.exceptions import PytubeFixError

import database_manager as db

# --- Configuration ---
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
WORKER_SLEEP_INTERVAL = 10 # Seconds to wait when queue is empty

# --- Function Stubs ---
def analyze_sentiment(text: str) -> dict:
    """STUB: Analyzes the sentiment of a text string."""
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
            parts = t_str.split(':')
            h = int(parts[0])
            m = int(parts[1])
            s_parts = parts[2].split('.')
            s = int(s_parts[0])
            ms = int(s_parts[1]) if len(s_parts) > 1 else 0
            return h * 3600 + m * 60 + s + ms / 1000.0
        return {
            "start": to_seconds(start_str),
            "end": to_seconds(end_str),
            "text": " ".join(text_lines)
        }
    except (ValueError, IndexError) as e:
        # LOGGING: More specific error on parsing failure
        print(f"  [WARN] Could not parse timestamp line: '{time_line}'. Error: {e}")
        return None

def process_youtube_url(url: str):
    """
    Main processing function for a single YouTube URL with detailed logging.
    Raises exceptions on failure.
    """
    print(f"\nProcessing URL: {url}")
    yt = YouTube(url)
    print(f"  > Title: {yt.title}")

    # LOGGING: Check all available caption codes
    all_caption_codes = [c.code for c in yt.captions]
    print(f"  > Available caption codes: {all_caption_codes if all_caption_codes else 'None'}")

    # 1. Attempt to get English captions
    caption_en = yt.captions.get_by_language_code('en')
    caption_a_en = yt.captions.get_by_language_code('a.en') # auto-generated
    
    caption = None
    if caption_en:
        caption = caption_en
        print("  > Found manually-created English captions ('en').")
    elif caption_a_en:
        caption = caption_a_en
        print("  > Found auto-generated English captions ('a.en').")

    if caption:
        print("  > Proceeding with caption processing...")
        video_stream = yt.streams.get_highest_resolution()
        video_path = video_stream.download(output_path=str(DOWNLOAD_DIR))
        print(f"  > Video downloaded to: {video_path}")

        video_id = db.add_video(url, yt.title, video_path)

        try:
            srt_captions = caption.generate_srt_captions()
            # LOGGING: Confirm that SRT generation worked
            if not srt_captions:
                message = "Caption track was found, but failed to generate SRT content. No transcript available."
                print(f"  [ERROR] {message}")
                db.update_queue_status(url, 'failed', message)
                return
            
            print(f"  > Successfully generated SRT data ({len(srt_captions)} bytes).")
            
            srt_segments = srt_captions.strip().split('\n\n')
            print(f"  > Split SRT data into {len(srt_segments)} potential segments.")

            successful_segments = 0
            failed_segments = 0
            for segment_str in srt_segments:
                segment = parse_srt_segment(segment_str)
                if segment:
                    sentiment = analyze_sentiment(segment['text'])
                    db.add_caption_segment(
                        video_id, segment['start'], segment['end'], segment['text'], sentiment
                    )
                    successful_segments += 1
                else:
                    if segment_str.strip(): # Don't count empty lines as failures
                        failed_segments += 1
            
            # LOGGING: Final summary of parsing
            print(f"  > Summary: {successful_segments} segments parsed successfully, {failed_segments} failed parsing.")

            if successful_segments > 0:
                message = f'Successfully processed with {successful_segments} caption segments.'
                db.update_queue_status(url, 'completed', message)
            else:
                message = f'Processed, but failed to parse any of the {len(srt_segments)} caption segments. Check logs for parsing errors.'
                db.update_queue_status(url, 'failed', message)

        except Exception as e:
            message = f"An error occurred during caption generation/processing: {e}"
            print(f"  [ERROR] {message}")
            db.update_queue_status(url, 'failed', message)

    else:
        # LOGGING: This is the "no captions found" path
        message = "No English captions ('en' or 'a.en') found. Downloading audio only for potential future processing."
        print(f"  > {message}")
        audio_stream = yt.streams.get_audio_only()
        audio_path = audio_stream.download(output_path=str(DOWNLOAD_DIR))
        print(f"  > Audio downloaded to: {audio_path}")
        
        db.add_video(url, yt.title, download_path=None)
        db.update_queue_status(url, 'completed', message) # Completed, but with a note.

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
            time.sleep(WORKER_SLEEP_INTERVAL)

if __name__ == "__main__":
    main()