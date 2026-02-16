import pickle

# Load both caches
with open('cache/HNw5OdcAAAAJ/all_citing_author_paper_tuple_list.pkl', 'rb') as f:
    all_authors = pickle.load(f)

with open('cache/HNw5OdcAAAAJ/affiliation_progress.pkl', 'rb') as f:
    progress = pickle.load(f)

affiliations = progress['affiliations']

print("Original author list format:")
print(f"  (author_id, citing_paper, cited_paper)")
print(f"  Example: {all_authors[0]}")

print("\n\nAffiliation list format:")
print(f"  (author_name, citing_paper, cited_paper, affiliation)")
print(f"  Example: {affiliations[0]}")

print("\n\nThe problem: We lost the author_id in the affiliation records!")
print("We only have author_name, which we got from the scholarly API.")

print("\n\nSolution: Build a mapping from (citing, cited) -> author_id")
print("Then for each affiliation, look up its author_id(s)")

# Build mapping
from collections import defaultdict
paper_to_author_ids = defaultdict(list)
for idx, (author_id, citing, cited) in enumerate(all_authors):
    paper_to_author_ids[(citing, cited)].append((idx, author_id))

# Now try to match affiliations
matched = 0
unmatched = 0
multi_match = 0

for author_name, citing, cited, affiliation in affiliations[:10]:
    key = (citing, cited)
    if key in paper_to_author_ids:
        matches = paper_to_author_ids[key]
        if len(matches) == 1:
            matched += 1
            idx, author_id = matches[0]
            print(f"\n✓ Unique match: {author_name} -> index {idx}, ID {author_id}")
        else:
            multi_match += 1
            print(f"\n⚠ Multiple matches for {author_name}: {len(matches)} authors")
            print(f"  Indices: {[idx for idx, _ in matches]}")
    else:
        unmatched += 1
        print(f"\n✗ No match for {author_name}")

print(f"\n\nSummary (first 10):")
print(f"  Unique matches: {matched}")
print(f"  Multiple matches: {multi_match}")
print(f"  Unmatched: {unmatched}")
