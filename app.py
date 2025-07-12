# app.py
import streamlit as st
import pandas as pd
import database_manager as db
import time

def format_seconds_to_srt(seconds: float) -> str:
    """Converts a float number of seconds to HH:MM:SS,ms format."""
    if seconds is None: return "00:00:00,000"
    millis = int((seconds - int(seconds)) * 1000)
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
    st.title("🎬 YouTube Content Analyzer")
    st.markdown("---")

    # --- INSTRUCTIONS AND ETL MANAGEMENT ---
    st.header("1. Add New Videos to the Queue")
    st.info(
        "**How to use:**\n"
        "1. In a separate terminal, run the command: `python etl.py` to start the background worker.\n"
        "2. Paste one or more YouTube URLs (one per line) into the text area below.\n"
        "3. Click 'Add to Queue'. The worker will automatically pick them up."
    )

    url_input = st.text_area(
        "YouTube URLs (one per line)",
        placeholder="https://www.youtube.com/watch?v=...\nhttps://www.youtube.com/watch?v=...",
        height=150
    )

    if st.button("Add to Queue", key="add_button"):
        if url_input:
            urls = [url.strip() for url in url_input.split('\n') if url.strip()]
            if urls:
                db.add_urls_to_queue(urls)
                st.success(f"Successfully added {len(urls)} URL(s) to the processing queue.")
                time.sleep(1) # Small delay for better UX
                st.rerun()
            else:
                st.warning("Please enter at least one valid URL.")
        else:
            st.warning("The text area is empty.")

    st.markdown("---")

    # --- UI TO DISPLAY RESULTS AND STATUS ---
    st.header("2. Processing Status & Results")

    # Fetch all data once
    all_videos_data = db.get_all_videos_with_status()
    if not all_videos_data:
        st.info("No videos have been submitted yet. Use the tool above to start.")
        return

    all_videos_df = pd.DataFrame([dict(row) for row in all_videos_data])

    # Display the current queue
    st.subheader("Current Queue")
    queue_df = all_videos_df[all_videos_df['status'].isin(['queued', 'processing'])]
    if not queue_df.empty:
        st.dataframe(
            queue_df[['youtube_url', 'status']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("The processing queue is currently empty.")

    # Display selection for completed videos
    st.subheader("Explore Completed Videos")
    completed_df = all_videos_df[all_videos_df['status'] == 'completed'].copy()

    if not completed_df.empty:
        # Use a copy to avoid SettingWithCopyWarning
        completed_df['display_title'] = completed_df['title'].fillna('Title not available')
        selected_title = st.selectbox(
            "Select a video to see its detailed analysis:",
            options=completed_df['display_title']
        )

        if selected_title:
            selected_video = completed_df[completed_df['display_title'] == selected_title].iloc[0]
            video_id = selected_video['video_id']
            captions = db.get_captions_for_video(video_id)

            if captions:
                caption_df = pd.DataFrame([dict(row) for row in captions])
                render_general_analysis(caption_df)

                st.subheader("Timestamped Transcript")
                with st.container(height=400): # Makes the list scrollable
                    for _, row in caption_df.iterrows():
                        start_time = format_seconds_to_srt(row['start_time'])
                        color = "green" if row['sentiment_label'] == "POSITIVE" else "red" if row['sentiment_label'] == "NEGATIVE" else "gray"
                        st.markdown(f"**`{start_time}`** : {row['text']} [:{color}[{row['sentiment_label']}]]")
            else:
                st.info("This video was processed, but no English captions were found. Only the audio file was saved.")
    else:
        st.info("No videos have been successfully completed yet.")


    # --- COLLAPSIBLE SECTION FOR ALL VIDEOS ---
    with st.expander("Show All Submitted Videos & Full Status History"):
        st.dataframe(
            all_videos_df[['youtube_url', 'status', 'title', 'status_message', 'updated_at']],
            use_container_width=True,
            hide_index=True,
            column_config={
                "youtube_url": st.column_config.LinkColumn("YouTube URL", display_text="🔗 Link"),
                "updated_at": st.column_config.DatetimeColumn("Last Updated", format="YYYY-MM-DD HH:mm:ss")
            }
        )

 # --- NEW: DATABASE INSPECTOR SECTION ---
    st.markdown("---")
    with st.expander("🗃️ Database Inspector (Advanced View)"):
        st.info("This section shows the raw data from the project's database tables.")

        # List of all tables we want to display
        table_names = ["processing_queue", "videos", "captions", "audios"]

        for table in table_names:
            st.subheader(f"Table: `{table}`")
            try:
                table_data = db.get_all_from_table(table)
                if table_data:
                    df = pd.DataFrame([dict(row) for row in table_data])
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.write("This table is currently empty.")
            except sqlite3.OperationalError as e:
                st.error(f"Could not read table '{table}'. It might not exist yet. Error: {e}")
            st.markdown("---")

if __name__ == "__main__":
    db.setup_database() # Ensure the DB is set up when the app starts
    main()