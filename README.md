# ZoltJolt, Youtube and Video Content Analyzer

This project is a web-based tool for analyzing the sentiment of English captions from YouTube videos. It features a background worker that processes URLs from a queue and a Streamlit web interface to submit videos and visualize the results.

## Key Features

*   **ETL Pipeline:** A robust worker (`etl.py`) that fetches video metadata and captions from YouTube.
*   **Sentiment Analysis:** Analyzes each caption segment and assigns a sentiment score (Positive, Negative, Neutral).
*   **Interactive UI:** A Streamlit application (`app.py`) for adding videos to a processing queue and exploring the results.
*   **Persistent Storage:** Uses SQLite to store video information, caption data, and processing status.
*   **Flexible Download Options:** Allows users to either download the full video or skip the download to save space and only analyze captions.
*   **Easy Local Deployment:** Uses `honcho` to manage and run the web and worker processes concurrently.

## Project File Structure

*   `app.py`: The main Streamlit web application file. This is the user interface.
*   `etl.py`: The background worker script that processes videos from the queue.
*   `database_manager.py`: Handles all interactions with the SQLite database.
*   `requirements.txt`: A list of all the Python packages required to run the project.
*   `Procfile`: (To be created) A configuration file for `honcho` to define the processes to run.
*   `project.db`: (Auto-generated) The SQLite database file.
*   `downloads/`: (Auto-generated) The directory where video/audio files are stored.

---

## 🚀 Getting Started

Follow these steps to set up and run the project on your local machine.

### 1. Prerequisites

*   **Python 3.8+**: Ensure you have a compatible version of Python installed. You can check your version with `python --version`.

### 2. Initial Setup

These are one-time steps to prepare your environment.

**a. Clone the Repository**

If the project is hosted on Git, clone the repository. Otherwise, ensure all project files are in a single directory.

```bash
# Example for git
git clone <repository-url>
cd <project-directory>
```

**b. Create and Activate a Virtual Environment**

It is highly recommended to use a virtual environment to manage project-specific dependencies.

```bash
# Create a virtual environment named .venv
python -m venv .venv

# Activate the environment
# On Windows:
.\.venv\Scripts\activate

# On macOS/Linux:
source .venv/bin/activate
```

Your terminal prompt should now be prefixed with `(.venv)`.

**c. Install Dependencies**

Install all the required Python packages from the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

### 3. Running the Application

This project requires two processes to run at the same time: the web server and the background worker. We use `honcho` to manage this easily.

**a. Create the `Procfile`**

In the root directory of the project, create a file named `Procfile` (no extension) and add the following content. This tells `honcho` what commands to run.

```Procfile
web: streamlit run app.py
worker: python etl.py
```

**b. Launch the Application**

With your virtual environment still active, run the following command from the project's root directory:

```bash
honcho start
```

This command will:
1.  Initialize the database (`project.db`) and create the necessary tables.
2.  Start the `worker` process (`etl.py`), which will begin listening for jobs.
3.  Start the `web` process (`app.py`), which launches the Streamlit UI.

You will see color-coded output from both processes in your terminal.

**c. Access the Web Interface**

Open your web browser and navigate to the local URL provided by Streamlit in the terminal output. It will typically be:

**[http://localhost:8501](http://localhost:8501)**

---

## ⚙️ How to Use

1.  **Run the App:** Make sure the application is running via the `honcho start` command.
2.  **Add Videos:** In the web UI, paste one or more YouTube URLs into the text area.
3.  **Choose Download Option:** Check the "Skip video download" box if you only want caption analysis.
4.  **Queue for Processing:** Click "Add to Queue".
5.  **Monitor:** The `worker` terminal output will show the processing status of each video. The "Processing Status & Results" section in the UI will also update automatically.
6.  **View Analysis:** Once a video is `completed`, select it from the dropdown to see the detailed sentiment analysis and the color-coded transcript.

## ⏹️ Stopping the Application

To stop both the web server and the worker, return to the terminal where `honcho` is running and press `Ctrl+C`.
