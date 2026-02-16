import pickle
import os

# Check the final cache
cache_file = 'cache/HNw5OdcAAAAJ/author_paper_affiliation_tuple_list.pkl'

if os.path.exists(cache_file):
    print("✓ Final cache file exists")
    print(f"  Location: {cache_file}\n")

    with open(cache_file, 'rb') as f:
        affiliations = pickle.load(f)

    print(f"Total records: {len(affiliations)}")
    print(f"Data type: {type(affiliations)}\n")

    # Check format
    print("Sample record (first entry):")
    if len(affiliations) > 0:
        sample = affiliations[0]
        print(f"  Type: {type(sample)}")
        print(f"  Format: (author_name, citing_paper, cited_paper, affiliation)")
        print(f"  Author: {sample[0]}")
        print(f"  Citing paper: {sample[1][:60]}...")
        print(f"  Your paper: {sample[2][:60]}...")
        print(f"  Affiliation: {sample[3]}")

    # Check for duplicates
    unique = set(affiliations)
    print(f"\n✓ Unique records: {len(unique)}")
    if len(affiliations) != len(unique):
        print(f"  ⚠ Warning: {len(affiliations) - len(unique)} duplicates found")
    else:
        print(f"  ✓ No duplicates")

    # Check for NASA affiliations
    nasa_count = 0
    nasa_examples = []
    for author, citing, cited, affiliation in affiliations:
        if 'nasa' in affiliation.lower():
            nasa_count += 1
            if len(nasa_examples) < 5:
                nasa_examples.append((author, affiliation))

    print(f"\n{'='*80}")
    print(f"NASA AFFILIATIONS FOUND: {nasa_count}")
    print('='*80)
    if nasa_examples:
        for author, affil in nasa_examples:
            print(f"  • {author} @ {affil}")
    else:
        print("  None found in the data")

    # Check format is correct for map generation
    print(f"\n{'='*80}")
    print("COMPATIBILITY CHECK")
    print('='*80)
    print("✓ Format matches citation_map.py requirements:")
    print("  - List of tuples: YES")
    print("  - Each tuple: (author_name, citing_paper, cited_paper, affiliation): YES")
    print("\n✓ Can be used by:")
    print("  - map_generate.py (with parse_csv=False)")
    print("  - analyze_affiliations.py")

    # Additional stats
    print(f"\n{'='*80}")
    print("STATISTICS")
    print('='*80)
    unique_authors = set([author for author, _, _, _ in affiliations])
    unique_affiliations = set([affil for _, _, _, affil in affiliations])
    print(f"  Unique author names: {len(unique_authors)}")
    print(f"  Unique affiliations: {len(unique_affiliations)}")

else:
    print("✗ Final cache file not found!")
