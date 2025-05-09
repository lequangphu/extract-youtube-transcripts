import os
import re  # Import regex for sanitizing filenames
import time
from datetime import datetime  # To parse the published date

import googleapiclient.discovery
import googleapiclient.errors  # Import specific API errors
from dotenv import load_dotenv  # Import dotenv
from youtube_transcript_api import (
    NoTranscriptFound,
    TranscriptsDisabled,
    YouTubeTranscriptApi,
)

# --- Load Environment Variables ---
load_dotenv()  # Load variables from .env file

# --- Configuration ---
# Load API key from environment variable
DEVELOPER_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
# Channel ID for @1000xPodcast (Example)
TARGET_CHANNEL_ID = "UCWrF0oN6unbXrWsTN7RctTw"
# Preferred transcript languages (order matters)
PREFERRED_LANGUAGES = ["en"]
# Maximum approximate words per output file
MAX_WORDS_PER_FILE = 500000
# --- End Configuration ---


# --- Helper Function for Ordinal Numbers ---
def get_ordinal(n):
    """Converts an integer into its ordinal representation (e.g., 1 -> 1st, 2 -> 2nd)."""
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    return str(n) + suffix


# --- Helper Function to Sanitize Filenames ---
def sanitize_filename(name):
    """Removes or replaces characters invalid for directory/file names."""
    # Remove invalid characters: < > : " / \ | ? *
    sanitized = re.sub(r'[<>:"/\\|?*]', "", name)
    # Replace potential leading/trailing whitespace and reduce multiple spaces
    sanitized = " ".join(sanitized.strip().split())
    # Optional: Replace spaces with underscores
    # sanitized = sanitized.replace(' ', '_')
    # Optional: Limit length
    # max_len = 100
    # sanitized = sanitized[:max_len]
    return sanitized if sanitized else "youtube_channel"  # Fallback name


# --- Helper Function to Format Video Entry ---
def format_video_entry(
    video_number, video_id, metadata, transcript_text, transcript_lang
):
    """Formats the text block for a single video entry."""
    published_at_raw = metadata.get("publishedAt", "N/A")
    published_at_formatted = "N/A"
    if published_at_raw != "N/A":
        try:
            # Parse ISO 8601 format and reformat
            dt_obj = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
            published_at_formatted = dt_obj.strftime("%Y-%m-%d %H:%M:%S %Z")
        except ValueError:
            published_at_formatted = published_at_raw  # Keep raw if parsing fails

    entry_lines = [
        f"--- Video {video_number} ---",
        f"Video ID: {video_id}",
        f"URL: {metadata.get('url', 'N/A')}",
        f"Title: {metadata.get('title', 'N/A')}",
        f"Published: {published_at_formatted}",
        f"Description:\n{metadata.get('description', 'N/A')}\n",
        f"Transcript Language: {transcript_lang}",
    ]
    if transcript_text:
        entry_lines.append(f"{transcript_text}")
    else:
        entry_lines.append("--- Transcript not available or fetch failed ---")

    entry_lines.append("\n" + "=" * 40 + "\n")
    return "\n".join(entry_lines)


# --- Existing Functions (get_youtube_service, get_channel_upload_playlist_id, etc.) ---
def get_youtube_service():
    """Builds and returns the YouTube API service object."""
    # Check if the key was loaded correctly before trying to build
    if not DEVELOPER_KEY:
        print("ERROR: YOUTUBE_API_KEY not found in environment variables.")
        print("Make sure you have a .env file with YOUTUBE_API_KEY='YOUR_KEY'")
        return None
    try:
        return googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY
        )
    except googleapiclient.errors.HttpError as e:
        # Check for specific API key related errors if possible
        if e.resp.status == 400 and "API key not valid" in str(e.content):
            print("Error building YouTube service: Invalid API Key provided.")
        else:
            print(f"Error building YouTube service (HTTP Error): {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred building YouTube service: {e}")
        return None


def get_channel_upload_playlist_id(youtube, channel_id):
    """Gets the ID of the channel's 'uploads' playlist and the channel title."""
    if not youtube:
        return None, None
    try:
        request = youtube.channels().list(
            part="contentDetails,snippet",  # Add snippet to get title
            id=channel_id,
        )
        response = request.execute()
        items = response.get("items")
        if not items:
            print(f"Channel not found or no content details for ID: {channel_id}")
            return None, None

        channel_item = items[0]
        content_details = channel_item.get("contentDetails", {})
        snippet = channel_item.get("snippet", {})  # Get the snippet

        related_playlists = content_details.get("relatedPlaylists", {})
        uploads_playlist_id = related_playlists.get("uploads")
        channel_title = snippet.get("title", "Unknown Channel")  # Get the title

        if not uploads_playlist_id:
            print(f"Could not find 'uploads' playlist ID for channel {channel_id}")
            return None, channel_title  # Still return title if found
        return uploads_playlist_id, channel_title
    except googleapiclient.errors.HttpError as e:
        print(f"API Error fetching channel details for {channel_id}: {e}")
        return None


def get_all_video_ids_in_playlist(youtube, playlist_id):
    """Gets all video IDs from a given playlist, handling pagination."""
    if not youtube or not playlist_id:
        return []
    video_ids = []
    next_page_token = None
    retries = 3
    max_retries = 3  # Define max retries
    while retries > 0:
        try:
            while True:
                request = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token,
                )
                response = request.execute()
                for item in response.get("items", []):
                    video_id = item.get("contentDetails", {}).get("videoId")
                    if video_id:
                        video_ids.append(video_id)
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    # Successfully fetched all pages
                    return video_ids  # Return directly from here
                time.sleep(0.1)  # Small delay between page requests
            # This break is now unreachable due to the return inside the loop
            # break
        except googleapiclient.errors.HttpError as e:
            print(
                f"API Error fetching playlist items (Page Token: {next_page_token}): {e}"
            )
            retries -= 1
            if retries > 0:
                wait_time = 2 ** (max_retries - retries)  # Exponential backoff
                print(
                    f"Retrying playlist fetch in {wait_time}s... ({retries} attempts left)"
                )
                time.sleep(wait_time)
            else:
                print("Max retries reached for fetching playlist items.")
                # Return the IDs fetched so far, even if incomplete
                return video_ids
        except Exception as e:
            print(f"Unexpected error fetching playlist items: {e}")
            # Return the IDs fetched so far
            return video_ids
    # This return is reached only if the initial while loop condition (retries > 0) fails,
    # which shouldn't happen unless max_retries is 0 or less.
    # Or if all retries failed.
    return video_ids  # Return potentially incomplete list if all retries failed


def get_videos_metadata(youtube, video_ids):
    """Fetches metadata for a list of video IDs in batches."""
    if not youtube or not video_ids:
        return {}
    metadata_dict = {}
    max_retries = 3  # Define max retries per batch

    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i : i + 50]
        ids_string = ",".join(batch_ids)
        retries = max_retries
        while retries > 0:
            try:
                request = youtube.videos().list(part="snippet", id=ids_string)
                response = request.execute()
                for item in response.get("items", []):
                    video_id = item.get("id")
                    snippet = item.get("snippet", {})
                    metadata_dict[video_id] = {
                        "title": snippet.get("title", "N/A"),
                        "description": snippet.get("description", "N/A"),
                        "publishedAt": snippet.get("publishedAt", "N/A"),
                        "url": f"https://www.youtube.com/watch?v={video_id}",
                    }
                break  # Success, exit retry loop for this batch
            except googleapiclient.errors.HttpError as e:
                print(f"API Error fetching metadata batch starting at index {i}: {e}")
                retries -= 1
                if retries > 0:
                    wait_time = 2 ** (max_retries - retries)  # Exponential backoff
                    print(
                        f"Retrying metadata fetch in {wait_time}s... ({retries} attempts left)"
                    )
                    time.sleep(wait_time)
                else:
                    print(f"Max retries reached fetching metadata batch {i}.")
                    # Mark videos in this failed batch with an error
                    for vid in batch_ids:
                        if vid not in metadata_dict:
                            metadata_dict[vid] = {
                                "error": "Metadata fetch failed after retries"
                            }
            except Exception as e:
                print(f"Unexpected error fetching metadata batch {i}: {e}")
                # Mark videos in this failed batch with an error
                for vid in batch_ids:
                    if vid not in metadata_dict:
                        metadata_dict[vid] = {
                            "error": f"Unexpected metadata fetch error: {e}"
                        }
                break  # Exit retry loop on unexpected error
        time.sleep(0.2)  # Delay between batches
    return metadata_dict


def get_transcript_for_video(video_id, languages):
    """Fetches the transcript for a single video ID."""
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        transcript = None
        found_lang = None

        # Prioritize preferred languages (manual first, then generated)
        try:
            transcript = transcript_list.find_manually_created_transcript(languages)
            found_lang = transcript.language
            print(f"Found manual transcript ({found_lang}) for {video_id}")
        except NoTranscriptFound:
            try:
                transcript = transcript_list.find_generated_transcript(languages)
                found_lang = transcript.language
                print(f"Found generated transcript ({found_lang}) for {video_id}")
            except NoTranscriptFound:
                print(
                    f"Transcript in {languages} not found for {video_id}. Trying any available language."
                )
                available_transcripts = list(transcript_list)
                if available_transcripts:
                    transcript = available_transcripts[0]
                    found_lang = transcript.language
                    transcript_type = (
                        "manual" if not transcript.is_generated else "generated"
                    )
                    print(
                        f"Found fallback {transcript_type} transcript in language: {found_lang} for {video_id}"
                    )
                else:
                    raise NoTranscriptFound(video_id, languages, transcript_list)

        # Fetch the actual transcript text
        fetched_transcript_list = transcript.fetch()
        if not fetched_transcript_list:
            print(
                f"Fetched transcript list is empty for {video_id} (Language: {found_lang})"
            )
            return None, found_lang

        # Join transcript parts, cleaning whitespace
        full_transcript_parts = []
        for item in fetched_transcript_list:
            try:
                text = item.text if hasattr(item, "text") else None
                if text and isinstance(text, str):
                    cleaned_text = " ".join(
                        text.split()
                    )  # Remove extra whitespace/newlines
                    full_transcript_parts.append(cleaned_text)
            except Exception as e:
                print(f"Warning: Error processing transcript item for {video_id}: {e}")

        if not full_transcript_parts:
            print(
                f"No valid transcript text extracted for {video_id} (Language: {found_lang})"
            )
            return None, found_lang

        return " ".join(full_transcript_parts), found_lang

    except TranscriptsDisabled:
        print(f"Transcripts are disabled for video: {video_id}")
        return None, "Transcripts Disabled"
    except NoTranscriptFound:
        print(f"No transcript found for video: {video_id}")
        return None, "No Transcript Found"
    except Exception as e:
        print(f"Could not retrieve or process transcript for {video_id}: {e}")
        return None, f"Error: {type(e).__name__}"


# --- End of Existing Functions ---


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()

    # --- Configuration Check ---
    # Check if API key was loaded from .env
    if not DEVELOPER_KEY:
        print("ERROR: YOUTUBE_API_KEY environment variable not set.")
        print("Please create a .env file in the script's directory with:")
        print("YOUTUBE_API_KEY='YOUR_ACTUAL_API_KEY'")
    elif not TARGET_CHANNEL_ID:
        print("ERROR: Please set the TARGET_CHANNEL_ID variable in the script.")
    else:
        print("Initializing YouTube service...")
        # Pass the key explicitly during service creation
        youtube_service = (
            get_youtube_service()
        )  # get_youtube_service now handles the None check

        if youtube_service:
            print(f"Fetching uploads playlist ID for channel: {TARGET_CHANNEL_ID}")
            upload_playlist_id, channel_title = get_channel_upload_playlist_id(
                youtube_service, TARGET_CHANNEL_ID
            )

            if upload_playlist_id:
                print(f"Found uploads playlist ID: {upload_playlist_id}")
                print(f"Channel Title: {channel_title}")
                print("Fetching all video IDs from the playlist...")
                # Consider reversing if you want "1st" to be the oldest video
                video_ids = get_all_video_ids_in_playlist(
                    youtube_service, upload_playlist_id
                )
                # Uncomment next line to process oldest videos first
                # video_ids.reverse()
                total_videos = len(video_ids)
                print(f"Found {total_videos} videos in the channel.")

                if total_videos > 0:
                    print("Fetching metadata for all videos (in batches)...")
                    all_metadata = get_videos_metadata(youtube_service, video_ids)
                    print(f"Fetched metadata for {len(all_metadata)} videos.")

                    print(
                        f"\nProcessing videos and writing to output files (limit ~{MAX_WORDS_PER_FILE:,} words/file)..."
                    )  # Formatted limit
                    successful_transcripts = 0
                    videos_with_issues = 0
                    output_filenames = []

                    # --- File Splitting Logic ---
                    current_word_count = 0
                    current_file_handle = None
                    current_filename_temp = (
                        None  # Temporary name while file is being written
                    )
                    file_start_video_number = 1  # 1-based index for naming
                    # Create output directory based on sanitized channel name
                    sanitized_channel_name = sanitize_filename(channel_title)
                    output_dir = os.path.join(
                        "transcripts_output", sanitized_channel_name
                    )  # Store in a sub-folder
                    os.makedirs(
                        output_dir, exist_ok=True
                    )  # Create the directory if it doesn't exist

                    try:  # Wrap the loop in try...finally to ensure the last file is closed/renamed
                        for i, video_id in enumerate(video_ids):
                            current_video_number = (
                                i + 1
                            )  # 1-based index for user display/naming
                            print(
                                f"Processing video {current_video_number}/{total_videos}: {video_id}"
                            )
                            metadata = all_metadata.get(video_id, {})
                            transcript_text = None
                            transcript_lang = "N/A"

                            if "error" in metadata:
                                print(
                                    f"  Skipping transcript fetch for {video_id} due to metadata fetch error: {metadata['error']}"
                                )
                                transcript_lang = f"Skipped ({metadata['error']})"
                                videos_with_issues += 1
                            else:
                                # Fetch transcript for this video
                                transcript_text, transcript_lang = (
                                    get_transcript_for_video(
                                        video_id, PREFERRED_LANGUAGES
                                    )
                                )
                                if transcript_text:  # Checks for non-empty string
                                    successful_transcripts += 1
                                    print(f"  Transcript fetched ({transcript_lang})")
                                elif (
                                    transcript_text == ""
                                ):  # Explicitly check for empty string
                                    print(
                                        f"  Transcript fetched ({transcript_lang}), but content is empty."
                                    )
                                    videos_with_issues += 1
                                else:  # transcript_text must be None
                                    # Reason already printed by get_transcript_for_video
                                    print(
                                        f"  Transcript fetch failed or unavailable ({transcript_lang})."
                                    )
                                    videos_with_issues += 1

                            # --- Prepare entry text and estimate word count ---
                            video_entry_text = format_video_entry(
                                current_video_number,
                                video_id,
                                metadata,
                                transcript_text,
                                transcript_lang,
                            )
                            # Simple word count estimation
                            entry_word_count = len(video_entry_text.split())

                            # --- Check if a new file needs to be started ---
                            # Start new file if it's the first video OR if adding the current video exceeds the limit
                            # (and ensure the current file isn't empty to avoid creating empty files)
                            needs_new_file = current_file_handle is None or (
                                current_word_count + entry_word_count
                                > MAX_WORDS_PER_FILE
                                and current_word_count > 0
                            )

                            if needs_new_file:
                                if current_file_handle is not None:
                                    # Close the previous file and rename it
                                    current_file_handle.close()
                                    last_video_number_in_file = current_video_number - 1
                                    final_filename_base = f"video-{get_ordinal(file_start_video_number)}-{get_ordinal(last_video_number_in_file)}-transcripts.txt"
                                    final_filepath = os.path.join(
                                        output_dir, final_filename_base
                                    )
                                    try:
                                        os.rename(current_filename_temp, final_filepath)
                                        print(f"Saved: {final_filepath}")
                                        output_filenames.append(final_filepath)
                                    except OSError as e:
                                        print(
                                            f"Error renaming {current_filename_temp} to {final_filepath}: {e}"
                                        )
                                        output_filenames.append(
                                            current_filename_temp + " (rename failed)"
                                        )
                                    current_file_handle = None

                                # --- Start a new file ---
                                file_start_video_number = current_video_number
                                # Use a temporary filename pattern within the output directory
                                temp_filename_base = f"temp_transcripts_part_{len(output_filenames) + 1}.txt"
                                current_filename_temp = os.path.join(
                                    output_dir, temp_filename_base
                                )

                                print(
                                    f"\nStarting new file: {current_filename_temp} (for videos {get_ordinal(file_start_video_number)} onwards)"
                                )
                                try:
                                    # Use 'with' statement for safer file handling
                                    current_file_handle = open(
                                        current_filename_temp, "w", encoding="utf-8"
                                    )
                                    # Write a header to the new file
                                    header = (
                                        f"--- YouTube Channel Transcript Export ---\n"
                                        f"Channel: {channel_title} (ID: {TARGET_CHANNEL_ID})\n"
                                        f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                                        f"Total Videos Found (in channel): {total_videos}\n"
                                        f"Preferred Languages: {', '.join(PREFERRED_LANGUAGES)}\n"
                                        f"--- Videos starting from {get_ordinal(file_start_video_number)} in this file ---\n"
                                        f"{'=' * 40}\n\n"
                                    )
                                    current_file_handle.write(header)
                                    current_word_count = len(
                                        header.split()
                                    )  # Reset word count for the new file
                                except IOError as e:
                                    print(
                                        f"\nFATAL ERROR: Could not open/write header to {current_filename_temp}: {e}"
                                    )
                                    current_file_handle = None  # Ensure it's None
                                    break  # Stop processing further videos

                            # --- Write the current video entry to the open file ---
                            if current_file_handle:
                                try:
                                    current_file_handle.write(
                                        video_entry_text + "\n"
                                    )  # Add extra newline for spacing
                                    current_word_count += entry_word_count
                                except IOError as e:
                                    print(
                                        f"Error writing video {current_video_number} to {current_filename_temp}: {e}"
                                    )
                                    # Attempt to close and finalize the file partially
                                    current_file_handle.close()
                                    last_video_number_in_file = (
                                        current_video_number - 1
                                    )  # Video that failed wasn't fully written
                                    partial_filename_base = f"video-{get_ordinal(file_start_video_number)}-{get_ordinal(last_video_number_in_file)}-transcripts_PARTIAL.txt"
                                    partial_filepath = os.path.join(
                                        output_dir, partial_filename_base
                                    )
                                    try:
                                        os.rename(
                                            current_filename_temp, partial_filepath
                                        )
                                        print(f"Saved partial file: {partial_filepath}")
                                        output_filenames.append(partial_filepath)
                                    except OSError as rename_e:
                                        print(
                                            f"Error renaming partial file {current_filename_temp}: {rename_e}"
                                        )
                                        output_filenames.append(
                                            current_filename_temp
                                            + " (partial, rename failed)"
                                        )
                                    current_file_handle = None
                                    break  # Stop processing

                            # Polite delay between transcript fetches/writes
                            time.sleep(0.3)  # Adjusted delay slightly

                    finally:
                        # --- Finalize the very last file after the loop ---
                        if current_file_handle is not None:
                            current_file_handle.close()
                            # Determine the last video number correctly
                            # If the loop finished normally, it's total_videos
                            # If it broke early, it should be the last successfully processed video number
                            # We'll assume normal completion here for simplicity, but a more robust solution
                            # might track the actual last successfully written video number.
                            last_video_number_in_file = (
                                current_video_number  # Use the last processed number
                            )
                            final_filename_base = f"video-{get_ordinal(file_start_video_number)}-{get_ordinal(last_video_number_in_file)}-transcripts.txt"
                            final_filepath = os.path.join(
                                output_dir, final_filename_base
                            )
                            try:
                                os.rename(current_filename_temp, final_filepath)
                                print(f"Saved: {final_filepath}")
                                output_filenames.append(final_filepath)
                            except OSError as e:
                                print(
                                    f"Error renaming final file {current_filename_temp} to {final_filepath}: {e}"
                                )
                                output_filenames.append(
                                    current_filename_temp + " (rename failed)"
                                )

                    print("\n--- Processing Complete ---")
                    print(
                        f"Successfully fetched transcripts for: {successful_transcripts} videos."
                    )
                    print(
                        f"Videos with metadata/transcript issues: {videos_with_issues}"
                    )
                    if output_filenames:
                        print(f"Output files created in directory: '{output_dir}'")
                        for fname in output_filenames:
                            print(
                                f"- {os.path.basename(fname)}"
                            )  # Print only filename for clarity
                    else:
                        print("No output files were generated.")

                else:
                    print("No videos found in the playlist to process.")
            else:
                print(
                    f"Could not find the uploads playlist for channel {TARGET_CHANNEL_ID}. Cannot proceed."
                )
        else:
            # Error message already printed by get_youtube_service or the initial check
            print("Script aborted due to YouTube service initialization failure.")

    end_time = time.time()
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")
