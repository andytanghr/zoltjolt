# app.py
import streamlit as st
import pandas as pd
import database_manager as db

def render_general_analysis(caption_df: pd.DataFrame):
    """
    STUB: Renders general and sectional sentiment analysis.
    """
    st.subheader("Overall Sentiment Analysis")
    
    if caption_df.empty:
        st.warning("No caption data available for analysis.")
        return

    # Calculate general sentiment
    average_score = caption_df['sentiment_score'].mean()
    positive_count = len(caption_df[caption_df['sentiment_label'] == 'POSITIVE'])
    negative_count = len(caption_df[caption_df['sentiment_label'] == 'NEGATIVE'])
    neutral_count = len(caption_df[caption_df['sentiment_label'] == 'NEUTRAL'])
    
    st.metric("Average Sentiment Score", f"{average_score:.2f}")
    
    # Sectional analysis
    st.write("Sentiment Distribution:")
    sentiment_dist_df = pd.DataFrame({
        'Sentiment': ['Positive', 'Neutral', 'Negative'],
        'Count': [positive_count, neutral_count, negative_count]
    }).set_index('Sentiment')
    st.bar_chart(sentiment_dist_df)


def main():
    """
    The main Streamlit application function.
    """
    st.set_page_config(page_title="YouTube Content Analyzer", layout="wide")
    st.title("YouTube Content Analyzer Dashboard")
    
    st.info("This dashboard displays results from the ETL process. To add new videos, run the `etl.py` script from your terminal.", icon="ℹ️")

    # Fetch and display the list of all processed videos
    all_videos = db.get_all_processed_videos()
    
    if not all_videos:
        st.warning("No videos have been processed yet. Run the ETL script to begin:")
        st.code("python etl.py \"<your_youtube_url>\"", language="bash")
        return

    # # Convert to DataFrame for easier display
    # video_df = pd.DataFrame(all_videos)
          
    # Convert the list of Row objects into a list of dictionaries first
    all_videos_as_dicts = [dict(row) for row in all_videos]
    video_df = pd.DataFrame(all_videos_as_dicts)
    st.subheader("Processed Videos")
    print("Columns in video_df:", video_df.columns.tolist())
    st.dataframe(video_df[['id', 'title', 'youtube_url', 'processed_at']], use_container_width=True)

    # Allow user to select a video to see its details
    selected_title = st.selectbox(
        "Select a video to see its detailed caption analysis:",
        options=video_df['title']
    )
    
    if selected_title:
        # Get the ID of the selected video
        video_id = int(video_df[video_df['title'] == selected_title]['id'].iloc[0])
        
        # Fetch and display captions for the selected video
        captions = db.get_captions_for_video(video_id)
        
        if captions:
            caption_df = pd.DataFrame(captions)
            
            # --- RENDER ANALYSIS ---
            render_general_analysis(caption_df)

            # --- RENDER CAPTION DATA ---
            st.subheader("Caption Segments and Sentiments")
            st.dataframe(
                caption_df[['start_time', 'text', 'sentiment_label', 'sentiment_score']],
                use_container_width=True
            )
        else:
            st.info("This video was processed but had no English captions, only an audio file was downloaded.")

if __name__ == "__main__":
    main()