import pickle
from collections import Counter

# Load the original author list
with open('cache/HNw5OdcAAAAJ/all_citing_author_paper_tuple_list.pkl', 'rb') as f:
    all_authors = pickle.load(f)

print(f"Total author entries: {len(all_authors)}")

# Check uniqueness of author IDs
author_ids = [author_id for author_id, _, _ in all_authors]
unique_author_ids = set(author_ids)
print(f"Unique author IDs: {len(unique_author_ids)}")
print(f"Duplicate author IDs: {len(author_ids) - len(unique_author_ids)}")

# Check uniqueness of (citing_paper, cited_paper) combinations
paper_combos = [(citing, cited) for _, citing, cited in all_authors]
unique_paper_combos = set(paper_combos)
print(f"\nUnique (citing_paper, cited_paper) combinations: {len(unique_paper_combos)}")
print(f"Duplicate paper combinations: {len(paper_combos) - len(unique_paper_combos)}")

# Show some examples of duplicates
if len(paper_combos) != len(unique_paper_combos):
    counter = Counter(paper_combos)
    duplicates = [(combo, count) for combo, count in counter.items() if count > 1]
    print(f"\nExample duplicate paper combinations (first 3):")
    for (citing, cited), count in duplicates[:3]:
        print(f"\n  {count}x citations:")
        print(f"    Citing: {citing[:60]}...")
        print(f"    Your paper: {cited[:60]}...")
        # Find the author IDs for this combo
        matching_authors = [aid for aid, citing_p, cited_p in all_authors if citing_p == citing and cited_p == cited]
        print(f"    Author IDs: {matching_authors}")

# Check uniqueness of (author_id, citing_paper, cited_paper) - the full tuple
full_tuples = [(aid, citing, cited) for aid, citing, cited in all_authors]
unique_full = set(full_tuples)
print(f"\n\nUnique (author_id, citing_paper, cited_paper) tuples: {len(unique_full)}")
print(f"This should equal total entries: {len(all_authors)}")
print(f"Are all entries unique? {len(unique_full) == len(all_authors)}")
