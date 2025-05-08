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
    "season_id": "2021",
    "type_id": "2",
    "week_id": "1",
    # Add any other IDs known to lead to rich data
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

    filename = f"{safe_pattern_slug}_example.json"
    filepath = OUTPUT_DIR / filename
    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)
        print(
            f"Saved example response for pattern '{pattern_slug}' to: {filepath}\n"
            f"(from URL: {url_being_fetched})"
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
            f"Warning: URL path {path_without_queries} doesn't align with API base "
            f"path {api_base_path}."
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
                )
                state_data["fetched_urls"] = set(
                    state_data.get("fetched_urls", [])
                )  # Tracks constructed URLs fetched
                state_data["to_fetch_queue"] = state_data.get(
                    "to_fetch_queue", []
                )  # Contains original $ref URLs
                state_data["sample_ids"] = state_data.get(
                    "sample_ids", INITIAL_HARDCODED_IDS.copy()
                )
                state_data["pattern_examples"] = state_data.get(
                    "pattern_examples", {}
                )  # Stores IDs from first time pattern seen
                state_data["failed_urls"] = state_data.get(
                    "failed_urls", {}
                )  # Tracks constructed URLs that failed
                print(f"Loaded state from {STATE_FILE}")
                return state_data
        except Exception as e:
            print(f"Could not load state from {STATE_FILE} due to {e}. Starting fresh.")

    print("No existing state file found or error in loading. Starting with initial state.")
    return {
        "discovered_patterns": set(),
        "patterns_fetched_example": set(),
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
        serializable_state["discovered_patterns"] = sorted(state_data["discovered_patterns"])
        serializable_state["patterns_fetched_example"] = sorted(
            state_data["patterns_fetched_example"]
        )
        serializable_state["fetched_urls"] = sorted(state_data["fetched_urls"])
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


def construct_url_from_pattern(pattern, global_sample_ids, base_api_url):
    """Constructs a concrete URL by filling pattern placeholders with global IDs."""
    try:
        if pattern == "/":  # Base URL itself
            return base_api_url, None

        constructed_url = base_api_url.rstrip("/") + pattern
        missing_ids = set()
        required_ids = set(re.findall(r"{(.*?)}", pattern))

        for placeholder in required_ids:
            id_key_to_try = placeholder  # e.g., {season_id} -> season_id
            if not id_key_to_try.endswith("_id"):  # for generic {id_1}, {id_2}
                id_key_to_try = placeholder

            # Try specific ID (e.g., season_id) then generic (e.g., id_1)
            actual_id_value = global_sample_ids.get(id_key_to_try)

            if actual_id_value is None:
                # Fallback for patterns like /events/{id_1}/competitions/{id_2}
                # if id_1 or id_2 not in global_sample_ids directly
                # This logic might need refinement if generic IDs clash or are ambiguous
                generic_key_match = re.match(r"id_(\d+)", placeholder)
                if generic_key_match:
                    # This part is tricky. If pattern is /type/{id_1}/group/{id_2}
                    # And global_sample_ids has "type_id" and "group_id", they won't be used.
                    # This assumes the key in global_sample_ids *is* "id_1", "id_2", etc.
                    # This might be okay if url_to_pattern_and_ids produces these generic keys
                    # when a more specific one (like event_id from /events/{event_id}) isn't found.
                    pass  # Keep missing_ids.add(placeholder)

                missing_ids.add(placeholder)
            else:
                constructed_url = constructed_url.replace(
                    f"{{{placeholder}}}", str(actual_id_value)
                )

        if missing_ids:
            # print(f"  Cannot construct URL for '{pattern}'. Missing IDs: {missing_ids}")
            return None, f"Missing sample IDs: {missing_ids}"

        if "{" in constructed_url or "}" in constructed_url:
            # This means some placeholders were not replaced, possibly due to missing IDs
            # print(f"  Failed to sub all placeholders for '{pattern}'. URL: {constructed_url}")
            return None, "Unresolved placeholders in URL"

        return constructed_url, None

    except Exception as e:
        print(f"  Error constructing URL for pattern '{pattern}': {e}")
        return None, f"Construction error: {e}"


# --- Main Discovery Logic ---
def run_discovery():
    state = load_state()
    discovered_patterns = state["discovered_patterns"]
    patterns_fetched_example = state["patterns_fetched_example"]
    pattern_examples = state["pattern_examples"]  # URLs that led to pattern discovery with IDs
    to_fetch_queue = state["to_fetch_queue"]  # Raw $ref URLs
    sample_ids = state["sample_ids"]  # Global pool of example IDs
    fetched_urls = state["fetched_urls"]  # Constructed URLs that have been fetched
    failed_urls = state["failed_urls"]  # Constructed URLs that resulted in 404 or other errors

    # print(f"Initial sample IDs: {sample_ids}")
    # print(f"Initial fetch queue: {to_fetch_queue}")

    processed_local_example_files = False  # To run local file scan only once

    for iteration in range(1, MAX_ITERATIONS + 1):
        print(f"--- Iteration {iteration} ---")
        newly_discovered_patterns_this_iteration = set()
        newly_added_to_queue_this_iteration = set()
        urls_processed_this_iteration = 0
        urls_actually_fetched_this_iteration = 0  # For constructing pattern examples

        # --- Phase 1: Process local files (for seeding patterns/IDs) ---
        # This is now run only once at the beginning if not done before.
        if not processed_local_example_files:
            print("\n--- Processing local sample files (run once) ---")
            # processed_local_files = 0 # Unused variable removed
            local_files_path = Path(OUTPUT_DIR)
            if local_files_path.exists():
                for file_path in local_files_path.glob("*_example.json"):
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                        # We need a representative URL for this local file for patterns/IDs.
                        # Chicken-and-egg: best guess or placeholder.
                        # Assume filename hints at structure or use BASE_URL.
                        # Better: store source URL with example.
                        # For this example, infer from filename or use base.
                        # This is more about seeding from *existing* examples than true discovery.

                        # Local file processing for ID/pattern extraction needs refinement.
                        # Primary goal of loading local files: avoid re-fetching.
                        # For now, focus on extracting $refs from them.
                        refs_from_local = extract_refs_recursive(
                            data, BASE_URL
                        )  # Use BASE_URL as context
                        for ref in refs_from_local:
                            if ref not in fetched_urls and ref not in to_fetch_queue:
                                to_fetch_queue.append(ref)
                                newly_added_to_queue_this_iteration.add(ref)
                        # processed_local_files += 1
                    except Exception as e:
                        print(f"Error processing local file {file_path}: {e}")
            # print(f"Processed {processed_local_files} local example files.")
            processed_local_example_files = True

        # --- Phase 2: Process URLs from the queue ---
        print(
            f"\n--- Iteration {iteration}: Phase 2: Processing URLs from queue "
            f"({len(to_fetch_queue)} URLs) ---"
        )
        queue_for_this_iteration = list(to_fetch_queue)  # Copy for safe iteration
        to_fetch_queue.clear()  # Clear original for items to be processed next iteration

        discovered_during_fetch = []

        if not queue_for_this_iteration and iteration > 1 and not to_fetch_queue:
            # If queue was empty AND the previous iteration didn't add anything new
            # Check if discovered_during_fetch from *previous* run (not stored in state) was empty.
            # We can approximate by checking if the queue *is still empty* after Phase 1.
            # Combined the nested if statements
            print(
                "Queue is empty and no new URLs added from local files. Discovery likely complete."
            )
            break

        for url_to_process in queue_for_this_iteration:
            urls_processed_this_iteration += 1
            # print(f"Processing URL from queue: {url_to_process}")

            # 1. Convert current URL to pattern and extract IDs
            current_pattern, ids_from_current_url = url_to_pattern_and_ids(url_to_process, BASE_URL)
            if current_pattern:
                if current_pattern not in discovered_patterns:
                    # print(f"  New pattern discovered from URL structure: {current_pattern}")
                    discovered_patterns.add(current_pattern)
                    newly_discovered_patterns_this_iteration.add(current_pattern)
                    if current_pattern not in pattern_examples and ids_from_current_url:
                        pattern_examples[current_pattern] = ids_from_current_url
                        # print(f"  Stored example IDs for {current_pattern}: "
                        #       f"{ids_from_current_url}")

                if ids_from_current_url:
                    # print(f"  Extracted IDs from current URL: {ids_from_current_url}")
                    update_sample_ids(
                        sample_ids, ids_from_current_url, INITIAL_HARDCODED_IDS.keys()
                    )
            else:
                # print(f"  Could not determine pattern for {url_to_process}")
                pass

            # 2. If this URL hasn't been fetched for $ref extraction before
            if url_to_process not in fetched_urls and url_to_process not in failed_urls:
                # print(f"  Fetching $refs from: {url_to_process}")
                data, fetch_error = get_json_from_url(url_to_process)
                fetched_urls.add(url_to_process)  # Mark as processed (attempted)

                if data:
                    # 3. Extract $refs from this fetched data
                    refs = extract_refs_recursive(data, url_to_process)
                    # print(f"    Found {len(refs)} $refs.")
                    for ref_url in refs:
                        cleaned_ref_url = (
                            urlparse(ref_url)._replace(query="").geturl()
                        )  # Remove query params for queue
                        if (
                            cleaned_ref_url.startswith(BASE_URL)
                            and cleaned_ref_url not in fetched_urls
                            and cleaned_ref_url not in to_fetch_queue
                            and cleaned_ref_url not in (r for r in queue_for_this_iteration)
                        ):  # Avoid adding if already in current batch
                            # print(f"      Adding to fetch queue: {cleaned_ref_url}")
                            to_fetch_queue.append(cleaned_ref_url)
                            newly_added_to_queue_this_iteration.add(cleaned_ref_url)
                            discovered_during_fetch.append(cleaned_ref_url)
                        # else:
                        # print(f"      Skipping $ref (not new, not base, or already processed): "
                        #       f"{cleaned_ref_url}")
                else:
                    # print(f"  Failed to fetch {url_to_process}. Error: {fetch_error}")
                    failed_urls[url_to_process] = (
                        fetch_error  # Record failure of the $ref URL itself
                    )
            # else:
            # print(f"  Skipping $ref fetch for {url_to_process} (already fetched or failed).")

        # --- Phase 3: Attempt to fetch examples for NEWLY discovered patterns ---
        # print(f"\n--- Iteration {iteration}: Phase 3: Fetching examples for "
        #       f"newly discovered patterns ---")
        patterns_to_try_fetching = (
            newly_discovered_patterns_this_iteration - patterns_fetched_example
        )
        # print(f"Found {len(patterns_to_try_fetching)} new patterns to attempt "
        #       f"fetching examples for.")

        for pattern in patterns_to_try_fetching:
            if pattern == "/":  # Already handled by initial queue if needed
                patterns_fetched_example.add(pattern)  # Mark as "fetched"
                continue

            # 4. Construct a URL for this pattern using global sample IDs
            # print(f"Attempting to construct URL for pattern: {pattern}")
            # print(f"  Using sample_ids: {sample_ids}")
            constructed_url, error_msg = construct_url_from_pattern(pattern, sample_ids, BASE_URL)

            if not constructed_url:
                # print(f"  Could not construct URL for pattern '{pattern}'. Reason: {error_msg}")
                # Might "blacklist" this pattern if it repeatedly fails construction.
                continue

            # 5. Check if this *constructed* URL has failed before (e.g. 404'd)
            if constructed_url in failed_urls and failed_urls[constructed_url].startswith(
                "HTTP 404"
            ):  # Be more specific for 404s
                # This checks if the *exact* constructed URL previously 404'd.
                # If sample IDs change, a *different* URL might be constructed later.
                print(
                    f"  Skipping fetch for pattern '{pattern}'. Constructed URL "
                    f"{constructed_url} previously 404'd."
                )
                # We might want to mark the pattern as "attempted but failed with current IDs"
                # Shortened: Skip now; may retry if IDs change and pattern is re-queued.
                continue

            if constructed_url in fetched_urls and pattern in patterns_fetched_example:
                # print(f"  Skipping fetch for pattern '{pattern}'. "
                #       f"Already have example from {constructed_url}")
                continue

            # 6. Fetch the *constructed* URL
            print(
                f"  Attempting to fetch example for pattern '{pattern}' using "
                f"constructed URL: {constructed_url}"
            )
            data, fetch_error_message = get_json_from_url(constructed_url)
            urls_actually_fetched_this_iteration += 1
            fetched_urls.add(
                constructed_url
            )  # Add to global fetched_urls to avoid re-fetching this specific URL

            if data:
                # Success
                patterns_fetched_example.add(pattern)
                save_json_response(data, slugify(pattern), constructed_url)

                # Discover IDs from this newly fetched data too
                _, ids_from_data = url_to_pattern_and_ids(
                    constructed_url, BASE_URL
                )  # Use the *fetched* URL
                if ids_from_data:
                    # print(f"    Extracted IDs from successful fetch of {pattern}: "
                    #       f"{ids_from_data}")
                    update_sample_ids(sample_ids, ids_from_data, INITIAL_HARDCODED_IDS.keys())

                # Extract $refs from this new data as well
                new_refs = extract_refs_recursive(data, constructed_url)
                # print(f"    Found {len(new_refs)} $refs from new data for pattern {pattern}.")
                for ref_url in new_refs:
                    cleaned_ref_url = urlparse(ref_url)._replace(query="").geturl()
                    if (
                        cleaned_ref_url.startswith(BASE_URL)
                        and cleaned_ref_url not in fetched_urls
                        and cleaned_ref_url not in to_fetch_queue
                        and cleaned_ref_url not in (r for r in queue_for_this_iteration)
                        and cleaned_ref_url not in (r for r in newly_added_to_queue_this_iteration)
                    ):
                        # print(f"      Adding to fetch queue from pattern example: "
                        #       f"{cleaned_ref_url}")
                        to_fetch_queue.append(cleaned_ref_url)
                        newly_added_to_queue_this_iteration.add(cleaned_ref_url)
                        discovered_during_fetch.append(
                            cleaned_ref_url
                        )  # Track for loop break condition

            else:
                # Failure
                print(
                    f"  Failed to fetch example for pattern '{pattern}' using "
                    f"{constructed_url}. Error: {fetch_error_message}"
                )
                failed_urls[constructed_url] = fetch_error_message
                # Potentially try to re-queue the *pattern* later if IDs change?
                # For now, we just record the specific URL failure.
                # If sample_ids updates, a *new* constructed_url might be tried for this pattern.

        # Save state at the end of each iteration
        save_state(
            {
                "discovered_patterns": discovered_patterns,
                "patterns_fetched_example": patterns_fetched_example,
                "pattern_examples": pattern_examples,
                "fetched_urls": fetched_urls,
                "to_fetch_queue": list(set(to_fetch_queue)),  # Deduplicate before saving
                "sample_ids": sample_ids,
                "failed_urls": failed_urls,
            }
        )

        # Summarize iteration
        print(f"\n--- Iteration {iteration} Summary ---")
        print(f"New URLs added to queue this iteration: {len(newly_added_to_queue_this_iteration)}")
        # for url in sorted(list(newly_added_to_queue_this_iteration)): print(f"  - {url}")

        # Check for new URLs discovered from actual fetches
        new_urls_from_fetches_this_iter = [
            url
            for url in discovered_during_fetch
            if url not in (q for q in queue_for_this_iteration)  # Exclude what was already in queue
        ]

        # Discover patterns from these newly found $ref URLs (if any)
        for new_url in new_urls_from_fetches_this_iter:
            pattern, ids = url_to_pattern_and_ids(new_url, BASE_URL)
            if pattern and pattern not in discovered_patterns:
                # is_new_pattern = True # Flag potentially redundant but safe - Unused var removed
                discovered_patterns.add(pattern)
                newly_discovered_patterns_this_iteration.add(pattern)
                if pattern not in pattern_examples and ids:
                    pattern_examples[pattern] = ids

        print(
            f"Patterns discovered this iteration: {len(newly_discovered_patterns_this_iteration)}"
        )
        # for p in sorted(list(newly_discovered_patterns_this_iteration)): print(f"  - {p}")
        # Above line can be verbose.
        print(f"Total unique patterns known: {len(discovered_patterns)}")
        print(f"Total patterns with fetched examples: {len(patterns_fetched_example)}")
        print(f"URLs processed (from queue) this iteration: {urls_processed_this_iteration}")
        print(
            f"URLs actually fetched (for pattern examples) this iteration: "
            f"{urls_actually_fetched_this_iteration}"
        )
        print(f"Size of fetch queue for next iteration: {len(to_fetch_queue)}")
        print(f"Current sample ID pool size: {len(sample_ids)}")
        # print(f"Sample IDs: {sample_ids}")

        # Break conditions
        if (
            not to_fetch_queue and not newly_discovered_patterns_this_iteration and iteration > 1
        ):  # Give at least one full cycle for initial BASE_URL processing
            print(
                f"\n--- Iteration {iteration}: Fetch queue is empty and no new patterns "
                "were discovered this iteration. Discovery complete. ---"
            )
            break
        if not to_fetch_queue and not new_urls_from_fetches_this_iter and iteration > 1:
            print(
                f"\n--- Iteration {iteration}: Fetch queue for next iteration is empty and "
                "no new URLs were discovered from fetches."
            )
            print(
                "This might indicate discovery is complete or stuck if patterns remain "
                "without examples."
            )
            # Check if there are discovered patterns still needing examples
            patterns_without_examples = discovered_patterns - patterns_fetched_example
            if patterns_without_examples:
                print(
                    f"There are still {len(patterns_without_examples)} patterns "
                    "without fetched examples."
                )
                # for p_no_ex in sorted(list(patterns_without_examples)): print(f"  - {p_no_ex}")
            else:
                print("All discovered patterns have fetched examples. Discovery complete.")
                break

    print("\n--- Discovery Process Finished ---")
    final_state = load_state()  # Load the very latest state
    print(f"Total unique patterns discovered: {len(final_state['discovered_patterns'])}")
    print(f"Total patterns with fetched examples: {len(final_state['patterns_fetched_example'])}")
    print(f"Total unique $ref URLs fetched: {len(final_state['fetched_urls'])}")
    print(f"Total constructed URLs that failed: {len(final_state['failed_urls'])}")
    print(f"See {STATE_FILE} for full details.")
    print(
        f"Discovered Patterns & Example IDs (from first encounter):\n"
        f"{json.dumps(final_state['pattern_examples'], indent=2)}"
    )
    # print(f"Final Sample IDs: {json.dumps(final_state['sample_ids'], indent=2)}")


if __name__ == "__main__":
    run_discovery()
