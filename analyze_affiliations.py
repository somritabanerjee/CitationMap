#!/usr/bin/env python3
"""
Analyze cached citation affiliation data and create a CSV report.
This script reads the cached affiliation data and groups authors by affiliation.
"""

import pickle
import pandas as pd
import os
from collections import defaultdict

def load_affiliation_cache(scholar_id):
    """Load the cached affiliation data."""
    cache_path = f'cache/{scholar_id}/author_paper_affiliation_tuple_list.pkl'

    if not os.path.exists(cache_path):
        print(f"Error: Cache file not found at {cache_path}")
        print("\nThe affiliation cache hasn't been created yet.")
        print("Run map_generate.py first and wait for it to complete the affiliation gathering step.")
        return None

    with open(cache_path, 'rb') as f:
        data = pickle.load(f)

    print(f"Loaded {len(data)} affiliation records from cache.")
    return data

def analyze_affiliations(affiliation_data):
    """
    Analyze affiliation data and group by affiliation.

    Each entry in affiliation_data is a tuple:
    (author_name, citing_paper_title, cited_paper_title, affiliation_name)
    """
    # Dictionary to store: affiliation -> set of unique author names
    affiliation_authors = defaultdict(set)

    # Also track which papers each affiliation cited
    affiliation_papers = defaultdict(list)

    for author_name, citing_paper_title, cited_paper_title, affiliation_name in affiliation_data:
        if author_name == 'No_author_found':
            continue

        affiliation_authors[affiliation_name].add(author_name)
        affiliation_papers[affiliation_name].append({
            'author': author_name,
            'citing_paper': citing_paper_title,
            'cited_paper': cited_paper_title
        })

    return affiliation_authors, affiliation_papers

def create_affiliation_summary_csv(affiliation_authors, output_file='results/affiliation_summary.csv'):
    """Create a CSV with affiliations and author counts."""

    # Create list of (affiliation, count, authors)
    summary_data = []
    for affiliation, authors in affiliation_authors.items():
        summary_data.append({
            'Affiliation': affiliation,
            'Author Count': len(authors),
            'Authors': '; '.join(sorted(authors))
        })

    # Sort by author count (descending)
    summary_data.sort(key=lambda x: x['Author Count'], reverse=True)

    # Create DataFrame and save
    df = pd.DataFrame(summary_data)

    # Ensure results directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)

    print(f"\nSummary saved to: {output_file}")
    print(f"Total unique affiliations: {len(summary_data)}")
    print(f"Total unique authors: {sum(d['Author Count'] for d in summary_data)}")

    return df

def create_nasa_specific_csv(affiliation_authors, affiliation_papers, output_file='results/nasa_affiliations.csv'):
    """Create a detailed CSV for NASA affiliations."""

    nasa_data = []

    for affiliation, authors in affiliation_authors.items():
        # Case-insensitive search for NASA
        if 'nasa' in affiliation.lower():
            for paper_info in affiliation_papers[affiliation]:
                nasa_data.append({
                    'Affiliation': affiliation,
                    'Author': paper_info['author'],
                    'Citing Paper': paper_info['citing_paper'],
                    'Your Paper (Cited)': paper_info['cited_paper']
                })

    if nasa_data:
        df = pd.DataFrame(nasa_data)
        df = df.sort_values(['Affiliation', 'Author'])

        # Ensure results directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)

        print(f"\nNASA-specific data saved to: {output_file}")
        print(f"Found {len(df)} citation records from NASA affiliations")
        print(f"Unique NASA affiliations found:")
        for affiliation in df['Affiliation'].unique():
            count = len(df[df['Affiliation'] == affiliation]['Author'].unique())
            print(f"  - {affiliation}: {count} authors")
    else:
        print("\nNo NASA affiliations found in the data.")

    return nasa_data

def print_top_affiliations(df, top_n=20):
    """Print the top N affiliations by author count."""
    print(f"\n{'='*80}")
    print(f"TOP {top_n} AFFILIATIONS BY AUTHOR COUNT")
    print('='*80)

    for idx, row in df.head(top_n).iterrows():
        print(f"\n{idx+1}. {row['Affiliation']}")
        print(f"   Authors ({row['Author Count']}): {row['Authors'][:200]}{'...' if len(row['Authors']) > 200 else ''}")

if __name__ == '__main__':
    # Your Google Scholar ID
    scholar_id = 'HNw5OdcAAAAJ'

    print("Citation Affiliation Analyzer")
    print("="*80)

    # Load cached data
    affiliation_data = load_affiliation_cache(scholar_id)

    if affiliation_data is None:
        print("\nPlease run map_generate.py first and wait for it to complete.")
        exit(1)

    # Analyze the data
    print("\nAnalyzing affiliations...")
    affiliation_authors, affiliation_papers = analyze_affiliations(affiliation_data)

    # Create summary CSV
    df = create_affiliation_summary_csv(affiliation_authors)

    # Create NASA-specific CSV
    create_nasa_specific_csv(affiliation_authors, affiliation_papers)

    # Print top affiliations
    print_top_affiliations(df)

    print(f"\n{'='*80}")
    print("Analysis complete!")
    print("="*80)
