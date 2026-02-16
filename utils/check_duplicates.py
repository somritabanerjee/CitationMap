import pickle
from collections import Counter

# Check for duplicates in the progress cache
with open('cache/HNw5OdcAAAAJ/affiliation_progress.pkl', 'rb') as f:
    progress = pickle.load(f)

affiliations = progress['affiliations']
print(f"Total affiliation records: {len(affiliations)}")

# Check for duplicates
unique_affiliations = set(affiliations)
print(f"Unique affiliation records: {len(unique_affiliations)}")
print(f"Duplicates: {len(affiliations) - len(unique_affiliations)}")

# Show some duplicate examples
if len(affiliations) != len(unique_affiliations):
    counter = Counter(affiliations)
    duplicates = [(item, count) for item, count in counter.items() if count > 1]
    print(f"\nExample duplicates (showing first 5):")
    for item, count in duplicates[:5]:
        author, citing, cited, affil = item
        print(f"  {count}x: {author} @ {affil}")
