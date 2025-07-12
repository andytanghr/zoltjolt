# etl.py
import argparse
from pathlib import Path

from pytubefix import YouTube
from pytubefix.exceptions import PytubeFixError

import database_manager as db

# --- Configuration ---
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True) # Ensure download directory exists

# --- Function Stubs ---
def analyze_sentiment(text: str) -> dict:
    """
    STUB: Analyzes the sentiment of a text string.
    In a real implementation, this would use a library like TextBlob,
    VADER, or a Transformers model.
    """
    # Replace this with your actual sentiment analysis logic
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
    
    start_str, end_str = [t.strip().replace(',', '.') for t in time_line.split('-->')]
    
    # Convert HH:MM:SS.ms to seconds
    def to_seconds(t_str):
        h, m, s = map(float, t_str.split(':'))
        return h * 3600 + m * 60 + s

    return {
        "start": to_seconds(start_str),
        "end": to_seconds(end_str),
        "text": " ".join(text_lines)
    }

def process_youtube_url(url: str):
    """
    Main processing function for a single YouTube URL.
    Downloads, extracts captions, analyzes, and stores everything in the DB.
    """
    print(f"\nProcessing URL: {url}")
    try:
        yt = YouTube(url)
        print(f"  > Title: {yt.title}")
        
        # 1. Attempt to get English captions
        caption = yt.captions.get_by_language_code('en')
        
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

        else:
            print("  > No English captions found. Downloading audio only.")
            audio_stream = yt.streams.get_audio_only()
            audio_path = audio_stream.download(output_path=str(DOWNLOAD_DIR))
            print(f"  > Audio downloaded to: {audio_path}")
            
            video_id = db.add_video(url, yt.title, download_path=None) # No video path
            db.add_audio(video_id, audio_path)
            print("  > STUB: Audio metadata stored. A speech-to-text model would be needed here.")

    except PytubeFixError as e:
        print(f"  [ERROR] Could not process video. PytubeFix error: {e}")
    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred: {e}")

# --- Script Entry Point ---
def main():
    """Main function to setup DB and process URLs from command line."""
    parser = argparse.ArgumentParser(description="YouTube Video ETL Processor.")
    parser.add_argument("urls", nargs='+', help="One or more YouTube URLs to process.")
    args = parser.parse_args()
    
    db.setup_database()
    
    for url in args.urls:
        process_youtube_url(url)

if __name__ == "__main__":
    main()