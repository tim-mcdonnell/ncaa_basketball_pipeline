import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

# --- Configuration ---
BASE_URL = "http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball"
OUTPUT_DIR = Path("sample_responses")
STATE_FILE = Path("discovery_state.json")
REQUEST_DELAY_SECONDS = 0.25  # Delay between API requests
MAX_RETRIES = 3  # Max retries for a failed request
MAX_ITERATIONS = 20  # Max discovery iterations to prevent infinite loops

# Initial Sample IDs - these are protected and prioritized
# Values should be strings as they appear in URLs
INITIAL_HARDCODED_IDS = {
    "season_id": "2024",  # Post-season is active, good data
    "type_id": "3",  # Postseason
    "week_id": "2",  # A specific week in postseason
}

# --- Utility Functions ---


def slugify(text):
    """Convert text to a filesystem-safe slug."""
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9_/-]+", "-", text)
    text = re.sub(r"[-_]+", "-", text)
    text = text.strip("-")
    text = text.replace("/", "_")
    if not text:
        text = "endpoint"
    text = text.strip("-_")
    return text if text else "endpoint"


def ensure_absolute_url(ref_url, current_base_url_for_relative_refs):
    """Ensures a URL is absolute."""
    if "://" in ref_url:
        return ref_url
    return urljoin(current_base_url_for_relative_refs, ref_url)


def get_json_from_url(url, params=None):
    """Fetches JSON from a URL, handles errors, respects delay, and retries."""
    retries = 0
    last_error = None
    while retries <= MAX_RETRIES:
        time.sleep(REQUEST_DELAY_SECONDS)  # Delay before each attempt
        if retries > 0:
            print(f"  Retrying ({retries}/{MAX_RETRIES})...")

        print(f"Fetching: {url} with params: {params}")
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json(), None
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error for {url}: {e.response.status_code} {e.response.reason}")
            last_error = f"HTTP {e.response.status_code} {e.response.reason}"
            if e.response.status_code == 404:
                print(f"Resource not found: {url}. No more retries for 404.")
                return None, last_error
            print(f"Response content sample: {e.response.text[:200]}...")
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            last_error = f"RequestException: {e}"
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response from {url}")
            response_text = getattr(response, "text", "N/A")
            print(f"Response content sample: {response_text[:200]}...")
            last_error = f"JSONDecodeError: {e}"
            return None, last_error  # Don't retry JSON errors

        retries += 1

    print(f"Max retries reached for {url}. Recording as failed.")
    return None, last_error


def save_json_response(data, pattern_slug, url_being_fetched):
    """Saves JSON data to a file named after the pattern slug."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_pattern_slug = re.sub(r'[<>:"|?*]', "", pattern_slug)
    if not safe_pattern_slug or safe_pattern_slug == "_":
        safe_pattern_slug = "base_league_details"

    # Use pattern slug for filename - ensures only one example per pattern is saved
    # (overwriting previous examples for the same pattern if run multiple times, which is fine)
    filename = f"{safe_pattern_slug}_example.json"
    filepath = OUTPUT_DIR / filename
    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)
        print(
            f"Saved example response for pattern '{pattern_slug}' to: {filepath} (from URL: {url_being_fetched})"
        )
    except Exception as e:
        print(f"Error saving JSON to {filepath}: {e}")


def extract_refs_recursive(data, current_url):
    """Recursively finds all '$ref' values in JSON data."""
    refs = set()
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "$ref" and isinstance(value, str):
                if value.startswith("http://") or value.startswith("https://"):
                    refs.add(ensure_absolute_url(value, current_url))
            else:
                refs.update(extract_refs_recursive(value, current_url))
    elif isinstance(data, list):
        for item in data:
            refs.update(extract_refs_recursive(item, current_url))
    return refs


def url_to_pattern_and_ids(url_string, api_base_url):
    """Converts URL to pattern and extracts IDs using simple rule."""
    if not url_string.startswith(api_base_url):
        return None, None

    parsed_url = urlparse(url_string)
    path_without_queries = parsed_url.path
    api_base_path = urlparse(api_base_url).path

    if path_without_queries.startswith(api_base_path):
        specific_path = path_without_queries[len(api_base_path) :]
    else:
        print(
            f"Warning: URL path {path_without_queries} doesn't align with API base path {api_base_path}."
        )
        specific_path = path_without_queries

    if not specific_path or specific_path == "/":
        return "/", {}

    segments = specific_path.strip("/").split("/")
    pattern_segments = []
    extracted_ids = {}

    for i, segment in enumerate(segments):
        if segment.isdigit():
            placeholder_name_root = f"id_{i + 1}"  # Default generic name
            placeholder_name_full = f"{{{placeholder_name_root}}}"
            id_key_for_dict = placeholder_name_root  # Use generic key

            if i > 0:
                prev_segment_cleaned = segments[i - 1].lower()
                temp_root = (
                    prev_segment_cleaned[:-1]
                    if prev_segment_cleaned.endswith("s")
                    else prev_segment_cleaned
                )
                if temp_root:  # Ensure we got a valid root
                    placeholder_name_root = temp_root
                    placeholder_name_full = f"{{{placeholder_name_root}_id}}"
                    id_key_for_dict = placeholder_name_root + "_id"  # Use specific key

            pattern_segments.append(placeholder_name_full)
            extracted_ids[id_key_for_dict] = segment
        else:
            pattern_segments.append(segment)

    final_pattern = "/" + "/".join(pattern_segments)
    return final_pattern, extracted_ids


def load_state():
    """Loads discovery state from STATE_FILE."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state_data = json.load(f)
                state_data["discovered_patterns"] = set(state_data.get("discovered_patterns", []))
                state_data["patterns_fetched_example"] = set(
                    state_data.get("patterns_fetched_example", [])
                )  # Load fetched patterns set
                state_data["fetched_urls"] = set(state_data.get("fetched_urls", []))
                state_data["to_fetch_queue"] = state_data.get("to_fetch_queue", [])
                state_data["sample_ids"] = state_data.get(
                    "sample_ids", INITIAL_HARDCODED_IDS.copy()
                )
                state_data["pattern_examples"] = state_data.get("pattern_examples", {})
                state_data["failed_urls"] = state_data.get("failed_urls", {})
                print(f"Loaded state from {STATE_FILE}")
                return state_data
        except Exception as e:
            print(f"Could not load state from {STATE_FILE} due to {e}. Starting fresh.")

    print("No existing state file found or error in loading. Starting with initial state.")
    return {
        "discovered_patterns": set(),
        "patterns_fetched_example": set(),  # Initialize fetched patterns set
        "pattern_examples": {},
        "fetched_urls": set(),
        "to_fetch_queue": [BASE_URL],
        "sample_ids": INITIAL_HARDCODED_IDS.copy(),
        "failed_urls": {},
    }


def save_state(state_data):
    """Saves discovery state to STATE_FILE."""
    try:
        serializable_state = state_data.copy()
        serializable_state["discovered_patterns"] = sorted(list(state_data["discovered_patterns"]))
        serializable_state["patterns_fetched_example"] = sorted(
            list(state_data["patterns_fetched_example"])
        )  # Save fetched patterns set
        serializable_state["fetched_urls"] = sorted(list(state_data["fetched_urls"]))
        with open(STATE_FILE, "w") as f:
            json.dump(serializable_state, f, indent=4)
        print(f"Saved state to {STATE_FILE}")
    except Exception as e:
        print(f"Error saving state to {STATE_FILE}: {e}")


def update_sample_ids(global_sample_ids, new_ids, hardcoded_keys):
    """Updates the global sample_ids, protecting hardcoded ones."""
    updated = False
    for key, value in new_ids.items():
        is_generic_id = key.startswith("id_") and key[3:].isdigit()
        if not key.endswith("_id") and not is_generic_id:
            continue

        if key not in global_sample_ids or key not in hardcoded_keys:
            if key not in global_sample_ids:
                print(f"  New sample ID type discovered: {key} = {value}")
            global_sample_ids[key] = value
            updated = True
        elif key in hardcoded_keys and global_sample_ids.get(key) != value:
            pass  # Don't overwrite hardcoded
    return updated


# --- Main Discovery Logic ---
def run_discovery():
    state = load_state()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    iteration = 0
    while iteration < MAX_ITERATIONS:
        iteration += 1
        print(f"\n--- Starting Discovery Iteration {iteration}/{MAX_ITERATIONS} ---")

        # Get current state components
        discovered_patterns = state["discovered_patterns"]
        patterns_fetched_example = state["patterns_fetched_example"]
        pattern_examples = state["pattern_examples"]
        fetched_urls = state[
            "fetched_urls"
        ]  # Tracks specific URLs successfully fetched (for their refs)
        to_fetch_queue = state["to_fetch_queue"]
        sample_ids = state["sample_ids"]
        failed_urls = state["failed_urls"]

        if not to_fetch_queue:
            print("Fetch queue is empty. Checking local files...")
            if not any(OUTPUT_DIR.glob("*_example.json")):  # Check if examples exist
                print("No local example files found either. Discovery likely complete.")
                break
            # If local files exist, Phase 1 might still add to queue

        # Stats for this iteration
        newly_discovered_patterns_this_iteration = set()
        urls_processed_this_iteration = 0  # Renamed from fetched, as we might skip fetching
        urls_actually_fetched_this_iteration = 0
        new_refs_found_this_iteration = set()
        urls_failed_this_iteration = {}

        # --- Phase 1: Process local files (optional, mainly for seeding/restarting) ---
        # This phase primarily helps discover patterns/IDs if starting with existing files
        # but doesn't fetch based on them. Fetching happens from the queue in Phase 2.
        print(
            f"\n--- Iteration {iteration}: Phase 1: Processing local sample files (for pattern/ID discovery) ---"
        )
        processed_local_files = 0
        for filepath in OUTPUT_DIR.glob("*_example.json"):  # Only process example files
            try:
                with open(filepath) as f:
                    data = json.load(f)
                refs_from_file = extract_refs_recursive(data, BASE_URL)  # Use BASE_URL as context
                for ref_url in refs_from_file:
                    ref_url_normalized = ref_url.rstrip("/")
                    # Discover patterns/IDs but DON'T add to fetch queue here
                    pattern, ids = url_to_pattern_and_ids(ref_url_normalized, BASE_URL)
                    if pattern and pattern not in discovered_patterns:
                        discovered_patterns.add(pattern)
                        newly_discovered_patterns_this_iteration.add(pattern)
                        pattern_examples[pattern] = ids
                        print(
                            f"  Discovered new pattern from local file ref: {pattern} with IDs: {ids}"
                        )
                    if pattern and ids:
                        update_sample_ids(sample_ids, ids, INITIAL_HARDCODED_IDS.keys())
                processed_local_files += 1
            except Exception as e:
                print(f"Error processing local file {filepath}: {e}")
        print(f"Processed {processed_local_files} local example files.")

        # --- Phase 2: Process URLs from the queue ---
        print(
            f"\n--- Iteration {iteration}: Phase 2: Processing URLs from queue ({len(to_fetch_queue)} URLs) ---"
        )

        queue_for_this_iteration = list(to_fetch_queue)
        to_fetch_queue.clear()  # New refs go to the *next* iteration's queue
        discovered_during_fetch = []  # Refs found from successful fetches in *this* iteration

        if (
            not queue_for_this_iteration
            and not newly_discovered_patterns_this_iteration
            and iteration > 1
        ):
            print(
                "Queue is empty and no new patterns found in previous iteration. Discovery likely complete."
            )
            break

        for current_url_to_fetch in queue_for_this_iteration:
            urls_processed_this_iteration += 1
            current_url_normalized = current_url_to_fetch.rstrip("/")

            # Skip if permanently failed
            if current_url_normalized in failed_urls and failed_urls[
                current_url_normalized
            ].startswith("HTTP 404"):
                continue

            # Determine pattern and IDs *before* deciding to fetch
            pattern, ids = url_to_pattern_and_ids(current_url_normalized, BASE_URL)

            # Always try to discover pattern and update sample IDs from the URL structure
            if pattern and pattern not in discovered_patterns:
                discovered_patterns.add(pattern)
                newly_discovered_patterns_this_iteration.add(pattern)
                if pattern not in pattern_examples:
                    pattern_examples[pattern] = ids  # Store first IDs seen for pattern
                print(f"  Discovered new pattern (from URL structure): {pattern}")
            if pattern and ids:
                update_sample_ids(sample_ids, ids, INITIAL_HARDCODED_IDS.keys())

            # --- Decide whether to fetch an example for this pattern ---
            should_fetch_example = False
            if pattern and pattern not in patterns_fetched_example:
                # Check if this specific URL already failed non-permanently
                if current_url_normalized not in failed_urls:
                    should_fetch_example = True
                else:
                    print(
                        f"  Skipping fetch for pattern '{pattern}' using URL {current_url_normalized} (URL failed previously)"
                    )

            elif not pattern:  # External or unparseable URL
                # print(f"  Skipping fetch for non-API or unparseable URL: {current_url_normalized}")
                pass  # Don't fetch

            # --- Fetching (if required) ---
            if should_fetch_example:
                print(
                    f"  Fetching example for new pattern '{pattern}' from URL: {current_url_normalized}"
                )
                data, error_message = get_json_from_url(current_url_normalized)
                urls_actually_fetched_this_iteration += 1

                if data:
                    # Success: Mark pattern as fetched, save, extract refs
                    patterns_fetched_example.add(pattern)  # Mark pattern example as fetched
                    fetched_urls.add(
                        current_url_normalized
                    )  # Mark this specific URL as successfully fetched
                    if (
                        pattern not in pattern_examples or not pattern_examples[pattern]
                    ):  # Store IDs if missing
                        pattern_examples[pattern] = ids
                    pattern_slug = slugify(pattern)
                    save_json_response(
                        data, pattern_slug, current_url_normalized
                    )  # Saves as pattern_slug_example.json

                    refs_from_data = extract_refs_recursive(data, current_url_normalized)
                    for ref in refs_from_data:
                        ref_normalized = ref.rstrip("/")
                        if (
                            ref_normalized not in fetched_urls
                            and ref_normalized not in queue_for_this_iteration
                            and ref_normalized not in to_fetch_queue
                            and ref_normalized not in discovered_during_fetch
                            and ref_normalized not in failed_urls
                        ):
                            discovered_during_fetch.append(ref_normalized)
                            new_refs_found_this_iteration.add(ref_normalized)

                    if current_url_normalized in failed_urls:
                        del failed_urls[current_url_normalized]

                elif error_message:
                    # Failure: Record failure, don't mark pattern as fetched
                    print(
                        f"  Failed to fetch example for pattern '{pattern}'. Error: {error_message}"
                    )
                    failed_urls[current_url_normalized] = error_message
                    urls_failed_this_iteration[current_url_normalized] = error_message
                    # Don't add to fetched_urls if it failed

            else:  # We are skipping the fetch for this URL (pattern example already exists or URL failed before)
                # Reduce noise: comment this out unless debugging
                # if pattern and pattern in patterns_fetched_example:
                #     print(f"  Skipping fetch for URL (pattern '{pattern}' already fetched): {current_url_normalized}")
                pass

        # --- End of Processing Queue for this Iteration ---

        # Add newly discovered URLs to the main queue for the *next* iteration
        for new_url in discovered_during_fetch:
            if (
                new_url not in fetched_urls
                and new_url not in to_fetch_queue
                and new_url not in failed_urls
            ):
                to_fetch_queue.append(new_url)
                # Discover pattern/IDs immediately for state consistency
                pattern, ids = url_to_pattern_and_ids(new_url, BASE_URL)
                if pattern and pattern not in discovered_patterns:
                    discovered_patterns.add(pattern)
                    newly_discovered_patterns_this_iteration.add(pattern)
                    if pattern not in pattern_examples:
                        pattern_examples[pattern] = ids
                    print(f"  Discovered new pattern from fetched ref: {pattern}")
                if pattern and ids:
                    update_sample_ids(sample_ids, ids, INITIAL_HARDCODED_IDS.keys())

        # --- Iteration Summary ---
        print(f"\n--- Iteration {iteration} Summary ---")
        print(
            f"Patterns discovered this iteration: {len(newly_discovered_patterns_this_iteration)}"
        )
        for p in sorted(list(newly_discovered_patterns_this_iteration)):
            print(f"  - {p}")
        print(f"Total unique patterns known: {len(discovered_patterns)}")
        print(f"Total patterns with fetched examples: {len(patterns_fetched_example)}")
        print(f"URLs processed this iteration: {urls_processed_this_iteration}")
        print(f"URLs actually fetched this iteration: {urls_actually_fetched_this_iteration}")
        print(
            f"URLs failed permanently this iteration (incl. retries): {len(urls_failed_this_iteration)}"
        )
        print(f"Total unique URLs fetched successfully (for refs): {len(fetched_urls)}")
        print(f"Total unique URLs failed permanently so far: {len(failed_urls)}")
        print(f"New $ref links added to queue for next iteration: {len(to_fetch_queue)}")
        print(f"Current Sample IDs being used: {json.dumps(sample_ids, indent=2)}")

        # --- Save State After Iteration ---
        state["discovered_patterns"] = discovered_patterns
        state["patterns_fetched_example"] = patterns_fetched_example
        state["pattern_examples"] = pattern_examples
        state["fetched_urls"] = fetched_urls
        state["to_fetch_queue"] = to_fetch_queue
        state["sample_ids"] = sample_ids
        state["failed_urls"] = failed_urls
        save_state(state)

        # --- Check for Completion ---
        if not to_fetch_queue and not discovered_during_fetch:
            print(
                f"\nDiscovery complete after iteration {iteration}: Fetch queue for next iteration is empty and no new URLs were discovered from fetches."
            )
            break

    # --- End of Main Loop ---
    if iteration >= MAX_ITERATIONS:
        print(f"\nReached maximum iterations ({MAX_ITERATIONS}). Stopping discovery.")
    print("\n--- Final Discovery State ---")
    print(f"Total unique patterns discovered: {len(state['discovered_patterns'])}")
    print(f"Total patterns with fetched examples: {len(state['patterns_fetched_example'])}")
    print(f"Total unique URLs fetched successfully (for refs): {len(state['fetched_urls'])}")
    print(f"Total unique URLs failed permanently: {len(state['failed_urls'])}")
    if state["failed_urls"]:
        print("Failed URLs:")
        for url, err in state["failed_urls"].items():
            print(f"  - {url}: {err}")
    print(f"Final Sample IDs: {json.dumps(state['sample_ids'], indent=2)}")
    print(f"See {STATE_FILE} for full details.")
    print(f"Discovered Patterns & Example IDs:\n{json.dumps(state['pattern_examples'], indent=2)}")


if __name__ == "__main__":
    run_discovery()
