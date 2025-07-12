# app.py
import streamlit as st
import pandas as pd
import database_manager as db
import subprocess # To run the ETL script in the background
import sys # To find the python executable
import time # To add a small delay for user experience

def format_seconds_to_srt(seconds: float) -> str:
    """Converts a float number of seconds to HH:MM:SS,ms format."""
    millis = int((seconds - int(seconds)) * 1000)
    # timedelta handles the conversion to HH:MM:SS
    td = pd.to_timedelta(seconds, unit='s')
    return f"{str(td).split('.')[0]},{millis:03d}"

def render_general_analysis(caption_df: pd.DataFrame):
    """Renders general and sectional sentiment analysis."""
    st.subheader("Overall Sentiment Analysis")
    
    if caption_df.empty:
        st.warning("No caption data available for analysis.")
        return

    average_score = caption_df['sentiment_score'].mean()
    positive_count = len(caption_df[caption_df['sentiment_label'] == 'POSITIVE'])
    negative_count = len(caption_df[caption_df['sentiment_label'] == 'NEGATIVE'])
    neutral_count = len(caption_df[caption_df['sentiment_label'] == 'NEUTRAL'])
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Average Sentiment Score", f"{average_score:.2f}")

    with col2:
        sentiment_dist_df = pd.DataFrame({
            'Sentiment': ['Positive', 'Neutral', 'Negative'],
            'Count': [positive_count, neutral_count, negative_count]
        }).set_index('Sentiment')
        st.bar_chart(sentiment_dist_df)

def main():
    """The main Streamlit application function."""
    st.set_page_config(page_title="YouTube Content Analyzer", layout="wide")
    st.title("YouTube Content Analyzer")
    st.markdown("---")

    # --- UI TO START THE ETL PROCESS ---
    st.header("1. Add a New Video")
    st.info("Enter a YouTube URL below and click 'Start Analysis'. The process will run in the background.", icon="▶️")
    
    url_input = st.text_input("YouTube URL", placeholder="https://www.youtube.com/watch?v=...")
    
    if st.button("Start Analysis", key="start_button"):
        if url_input:
            with st.spinner("Starting the ETL process... This may take a few minutes."):
                # We run etl.py as a separate, non-blocking process
                # This prevents the Streamlit UI from freezing
                python_executable = sys.executable
                process = subprocess.Popen([python_executable, "etl.py", url_input])
                
                # Optional: wait for the process to finish to give a success message
                # In a real heavy-duty app, you might use a more advanced queue system
                process.wait()

            st.success(f"Processing complete for {url_input}! The results below have been updated.")
            # Add a small delay then rerun the page to refresh the data display
            time.sleep(2)
            st.rerun()
        else:
            st.warning("Please enter a URL.")
            
    st.markdown("---")

    # --- UI TO DISPLAY RESULTS ---
    st.header("2. Explore Processed Videos")
    
    all_videos = db.get_all_processed_videos()
    
    if not all_videos:
        st.warning("No videos have been processed yet. Use the tool above to start.")
        return

    all_videos_as_dicts = [dict(row) for row in all_videos]
    video_df = pd.DataFrame(all_videos_as_dicts)

    # Allow user to select a video to see its details
    selected_title = st.selectbox(
        "Select a video to see its detailed analysis:",
        options=video_df['title']
    )
    
    if selected_title:
        video_id = int(video_df[video_df['title'] == selected_title]['id'].iloc[0])
        captions = db.get_captions_for_video(video_id)
        
        if captions:
            captions_as_dicts = [dict(row) for row in captions]
            caption_df = pd.DataFrame(captions_as_dicts)
            
            # --- RENDER ANALYSIS ---
            render_general_analysis(caption_df)

            # --- RENDER TIMESTAMPED CAPTIONS (NEW) ---
            st.subheader("Timestamped Transcript")
            with st.container(height=300): # Makes the list scrollable
                for index, row in caption_df.iterrows():
                    start_time = format_seconds_to_srt(row['start_time'])
                    # Simple color coding for sentiment
                    color = "green" if row['sentiment_label'] == "POSITIVE" else "red" if row['sentiment_label'] == "NEGATIVE" else "gray"
                    st.markdown(f"**`{start_time}`** : {row['text']} [:{color}[{row['sentiment_label']}]]")

        else:
            st.info("This video was processed but had no English captions, so only an audio file was downloaded.")

if __name__ == "__main__":
    db.setup_database() # Ensure the DB is set up when the app starts
    main()