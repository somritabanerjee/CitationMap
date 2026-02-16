#!/usr/bin/env python3
"""
Modified citation map generator with incremental caching and automatic retry.

Features:
- Saves affiliation data after EVERY author to avoid data loss when Google Scholar blocks you
- Tracks failed author requests during processing
- After reaching the last author, automatically analyzes which authors are missing (0-N)
- Retries all missing authors up to 3 times
- Resumes from where it left off if interrupted
"""

import os
import pickle
import time
import random
from tqdm import tqdm
from scholarly import scholarly

# Import constants from citation_map
import sys
sys.path.insert(0, 'citation_map')
from scholarly_support import NO_AUTHOR_FOUND_STR, get_organization_name

def load_cache(fpath):
    """Load cached data."""
    with open(fpath, 'rb') as fd:
        return pickle.load(fd)

def save_cache(data, fpath):
    """Save data to cache."""
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    with open(fpath, "wb") as fd:
        pickle.dump(data, fd)

def affiliations_from_authors_aggressive(citing_author_paper_info):
    """Get affiliation using aggressive approach (self-reported affiliation)."""
    citing_author_id, citing_paper_title, cited_paper_title = citing_author_paper_info

    if citing_author_id == NO_AUTHOR_FOUND_STR:
        return (NO_AUTHOR_FOUND_STR, citing_paper_title, cited_paper_title, NO_AUTHOR_FOUND_STR)

    time.sleep(random.uniform(1, 5))  # Random delay to reduce risk of being blocked

    try:
        citing_author = scholarly.search_author_id(citing_author_id)
        if 'affiliation' in citing_author:
            return (citing_author['name'], citing_paper_title, cited_paper_title, citing_author['affiliation'])
    except Exception as e:
        print(f"\n[Error] Failed to get affiliation for author {citing_author_id}: {str(e)}")
        return None

    return None

def affiliations_from_authors_conservative(citing_author_paper_info):
    """Get affiliation using conservative approach (verified organization only)."""
    citing_author_id, citing_paper_title, cited_paper_title = citing_author_paper_info

    if citing_author_id == NO_AUTHOR_FOUND_STR:
        return (NO_AUTHOR_FOUND_STR, citing_paper_title, cited_paper_title, NO_AUTHOR_FOUND_STR)

    time.sleep(random.uniform(1, 5))  # Random delay to reduce risk of being blocked

    try:
        citing_author = scholarly.search_author_id(citing_author_id)
        if 'organization' in citing_author:
            author_organization = get_organization_name(citing_author['organization'])
            return (citing_author['name'], citing_paper_title, cited_paper_title, author_organization)
    except Exception as e:
        print(f"\n[Error] Failed to get affiliation for author {citing_author_id}: {str(e)}")
        return None

    return None

def find_affiliations_with_incremental_cache(
    all_citing_author_paper_tuple_list,
    scholar_id,
    cache_folder='cache',
    affiliation_conservative=False,
    save_interval=1,
    max_retry_passes=3
):
    """
    Find affiliations with incremental caching to avoid data loss.

    Parameters:
    -----------
    save_interval : int
        Save progress to cache every N authors (default: 1)
    max_retry_passes : int
        Maximum number of retry passes for failed authors (default: 3)
    """

    cache_file = os.path.join(cache_folder, scholar_id, 'author_paper_affiliation_tuple_list.pkl')
    progress_file = os.path.join(cache_folder, scholar_id, 'affiliation_progress.pkl')

    # Check if we have existing progress
    if os.path.exists(cache_file):
        print(f"Found existing affiliation cache at {cache_file}")
        return load_cache(cache_file)

    # Load any existing progress
    completed_affiliations = []
    successful_indices = set()  # Track which author indices were successfully processed
    failed_authors = []
    start_idx = 0

    if os.path.exists(progress_file):
        progress_data = load_cache(progress_file)
        completed_affiliations = progress_data.get('affiliations', [])
        successful_indices = progress_data.get('successful_indices', set())
        failed_authors = progress_data.get('failed_authors', [])
        start_idx = progress_data.get('last_index', -1) + 1
        print(f"\nResuming from author {start_idx}/{len(all_citing_author_paper_tuple_list)}")
        print(f"Already collected {len(completed_affiliations)} affiliation records")
        print(f"Successfully processed {len(successful_indices)} author indices")
        if failed_authors:
            print(f"Previously failed (from current run): {len(failed_authors)} authors")

    # Choose affiliation method
    if affiliation_conservative:
        get_affiliation = affiliations_from_authors_conservative
        approach = "conservative"
    else:
        get_affiliation = affiliations_from_authors_aggressive
        approach = "aggressive"

    print(f"\nIdentifying affiliations using the {approach} approach.")
    print(f"Progress will be saved after every author to avoid data loss.\n")

    # Build a mapping from (citing, cited) -> list of all matching indices
    # This handles cases where the same paper combination appears multiple times (co-authors, multiple citations by same author)
    from collections import defaultdict
    paper_to_indices = defaultdict(list)
    for idx, (author_id, citing_p, cited_p) in enumerate(all_citing_author_paper_tuple_list):
        paper_to_indices[(citing_p, cited_p)].append(idx)

    # Process remaining authors
    remaining = all_citing_author_paper_tuple_list[start_idx:]

    for idx, author_and_paper in enumerate(tqdm(remaining,
                                                  desc=f'Finding citing affiliations (Pass 1)',
                                                  initial=start_idx,
                                                  total=len(all_citing_author_paper_tuple_list))):
        actual_idx = start_idx + idx

        # Get affiliation
        result = get_affiliation(author_and_paper)
        if result:
            completed_affiliations.append(result)

            # Mark ALL matching indices as successful (not just current one)
            _, citing_paper, cited_paper = author_and_paper
            key = (citing_paper, cited_paper)
            for matching_idx in paper_to_indices[key]:
                successful_indices.add(matching_idx)
        else:
            # Track failed author for retry
            failed_authors.append((actual_idx, author_and_paper))

        # Save progress periodically
        if (idx + 1) % save_interval == 0:
            progress_data = {
                'affiliations': completed_affiliations,
                'successful_indices': successful_indices,
                'failed_authors': failed_authors,
                'last_index': actual_idx
            }
            save_cache(progress_data, progress_file)
            # Only print every 10 saves to avoid console spam
            if (idx + 1) % 10 == 0:
                print(f"\n[Progress saved: {actual_idx + 1}/{len(all_citing_author_paper_tuple_list)} authors processed, {len(failed_authors)} failed]")

    # First pass complete
    print(f"\n\nFirst pass completed!")

    # Deduplicate affiliations before analyzing
    original_count = len(completed_affiliations)
    completed_affiliations = list(set(completed_affiliations))
    duplicates_removed = original_count - len(completed_affiliations)

    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed} duplicate affiliation records")

    print(f"  Collected: {len(completed_affiliations)} unique affiliation records")
    print(f"  Failed during processing: {len(failed_authors)} authors")

    # Analyze which authors are missing from the full list
    print(f"\n{'='*80}")
    print("ANALYZING MISSING AUTHORS...")
    print('='*80)

    # Find missing authors by checking which indices (0 to N-1) are not in successful_indices
    missing_authors = []
    for idx, author_tuple in enumerate(all_citing_author_paper_tuple_list):
        if idx not in successful_indices:
            # Check if already in failed_authors to avoid duplicates
            already_tracked = any(fail_idx == idx for fail_idx, _ in failed_authors)
            if not already_tracked:
                missing_authors.append((idx, author_tuple))

    if missing_authors:
        print(f"\nFound {len(missing_authors)} missing authors from indices 0-{len(all_citing_author_paper_tuple_list)-1}")
        print(f"Adding them to retry queue...")
        failed_authors.extend(missing_authors)

    print(f"\nTotal authors to retry: {len(failed_authors)}")
    print('='*80)

    # Retry failed authors
    retry_pass = 1
    while failed_authors and retry_pass <= max_retry_passes:
        print(f"\n{'='*80}")
        print(f"RETRY PASS {retry_pass}/{max_retry_passes}")
        print(f"Retrying {len(failed_authors)} failed authors...")
        print(f"{'='*80}\n")

        current_failures = failed_authors.copy()
        failed_authors = []

        for orig_idx, author_and_paper in tqdm(current_failures,
                                                desc=f'Retrying failed authors (Pass {retry_pass + 1})'):
            result = get_affiliation(author_and_paper)
            if result:
                # Only add if not already in completed_affiliations (prevent duplicates)
                if result not in completed_affiliations:
                    completed_affiliations.append(result)

                # Mark ALL matching indices as successful (not just this one)
                _, citing_paper, cited_paper = author_and_paper
                key = (citing_paper, cited_paper)
                for matching_idx in paper_to_indices[key]:
                    successful_indices.add(matching_idx)
            else:
                # Still failed, add back to failed list
                failed_authors.append((orig_idx, author_and_paper))

            # Save progress after each retry too
            progress_data = {
                'affiliations': completed_affiliations,
                'successful_indices': successful_indices,
                'failed_authors': failed_authors,
                'last_index': len(all_citing_author_paper_tuple_list) - 1,
                'retry_pass': retry_pass
            }
            save_cache(progress_data, progress_file)

        print(f"\nRetry pass {retry_pass} complete:")
        print(f"  Recovered: {len(current_failures) - len(failed_authors)} authors")
        print(f"  Still failing: {len(failed_authors)} authors")

        retry_pass += 1

    # Final summary
    print(f"\n{'='*80}")
    print(f"ALL PASSES COMPLETED!")
    print(f"{'='*80}")

    # Final deduplication
    original_count = len(completed_affiliations)
    completed_affiliations = list(set(completed_affiliations))
    duplicates_removed = original_count - len(completed_affiliations)

    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed} final duplicate records")

    print(f"  Total affiliation records collected: {len(completed_affiliations)}")
    print(f"  Total authors processed: {len(all_citing_author_paper_tuple_list)}")
    print(f"  Permanently failed authors: {len(failed_authors)}")

    if failed_authors:
        print(f"\nAuthors that could not be retrieved after {max_retry_passes} retry passes:")
        for orig_idx, (author_id, _, _) in failed_authors[:10]:
            print(f"  [{orig_idx}] Author ID: {author_id}")
        if len(failed_authors) > 10:
            print(f"  ... and {len(failed_authors) - 10} more")

    # Save final cache and remove progress file
    save_cache(completed_affiliations, cache_file)
    print(f"\nFinal cache saved to: {cache_file}")

    if os.path.exists(progress_file):
        os.remove(progress_file)
        print(f"Removed temporary progress file.")

    return completed_affiliations

if __name__ == '__main__':
    scholar_id = 'HNw5OdcAAAAJ'
    cache_folder = 'cache'

    # Load the cached author list
    author_cache_path = os.path.join(cache_folder, scholar_id, 'all_citing_author_paper_tuple_list.pkl')

    if not os.path.exists(author_cache_path):
        print("Error: Author cache not found. Run map_generate.py first to collect citing authors.")
        exit(1)

    print("Loading citing authors from cache...")
    all_citing_author_paper_tuple_list = load_cache(author_cache_path)
    print(f"Loaded {len(all_citing_author_paper_tuple_list)} citing authors.\n")

    # Collect affiliations with incremental caching
    try:
        affiliations = find_affiliations_with_incremental_cache(
            all_citing_author_paper_tuple_list,
            scholar_id=scholar_id,
            cache_folder=cache_folder,
            affiliation_conservative=False,  # Set to True for conservative approach
            save_interval=1,  # Save after every author
            max_retry_passes=3  # Retry failed authors up to 3 times
        )

        print(f"\n{'='*80}")
        print(f"SUCCESS! Collected {len(affiliations)} total affiliation records.")
        print(f"Cache saved to: {cache_folder}/{scholar_id}/author_paper_affiliation_tuple_list.pkl")
        print(f"{'='*80}")
        print("\nYou can now run map_generate.py or analyze_affiliations.py")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Progress has been saved.")
        print("Run this script again to resume from where you left off.")
    except Exception as e:
        print(f"\n\nError: {e}")
        print("Progress has been saved. Run this script again to resume.")
