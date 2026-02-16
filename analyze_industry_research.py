#!/usr/bin/env python3
"""
Analyze citations from industry research centers.
Creates a table with institutions, # of citing papers, and # of citing researchers.
"""

import pickle
import pandas as pd
from collections import defaultdict
import os


def categorize_affiliation(affiliation_str):
    """
    Returns institution category or None if no match.
    """
    affiliation_lower = affiliation_str.lower()

    # Toyota Research Institute
    if 'toyota research institute' in affiliation_lower:
        return 'Toyota Research Institute'

    # Google Deepmind or Google
    if 'deepmind' in affiliation_lower:
        return 'Google Deepmind or Google'
    if 'google' in affiliation_lower:
        return 'Google Deepmind or Google'

    # Amazon Prime Air
    if 'amazon prime air' in affiliation_lower or 'prime air' in affiliation_lower:
        return 'Amazon Prime Air'

    # NVIDIA
    if 'nvidia' in affiliation_lower:
        return 'NVIDIA'

    # OpenAI
    if 'openai' in affiliation_lower or 'open ai' in affiliation_lower:
        return 'OpenAI'

    # SpaceX
    if 'spacex' in affiliation_lower or 'space x' in affiliation_lower:
        return 'SpaceX'

    # Tesla
    if 'tesla' in affiliation_lower:
        return 'Tesla'

    # Plus AI
    if 'plus ai' in affiliation_lower or 'plus.ai' in affiliation_lower or 'plusai' in affiliation_lower:
        return 'Plus AI'

    # Bosch
    if 'bosch' in affiliation_lower:
        return 'Bosch'

    # Honda Research Institute or Honda
    if 'honda research institute' in affiliation_lower or 'honda' in affiliation_lower:
        return 'Honda Research Institute'

    # Tyvak
    if 'tyvak' in affiliation_lower:
        return 'Tyvak'

    # Argotec
    if 'argotec' in affiliation_lower:
        return 'Argotec, Italy'

    # Tencent
    if 'tencent' in affiliation_lower:
        return 'Tencent, China'

    return None


def create_industry_research_table(affiliation_data):
    """
    Process affiliation data and create industry research centers summary.

    Returns:
        DataFrame with Institution, # of citing papers, # of citing researchers
    """
    # Define all institutions we're tracking
    ALL_INSTITUTIONS = [
        'Toyota Research Institute',
        'Google Deepmind or Google',
        'Amazon Prime Air',
        'NVIDIA',
        'OpenAI',
        'SpaceX',
        'Tesla',
        'Plus AI',
        'Bosch',
        'Honda Research Institute',
        'Tyvak',
        'Argotec, Italy',
        'Tencent, China'
    ]

    # Data structure to collect results
    industry_citations = defaultdict(lambda: {
        'citing_papers': set(),
        'citing_researchers': set(),
        'raw_affiliations': set()
    })

    # Process each citation record
    for author, citing_paper, cited_paper, affiliation in affiliation_data:
        # Skip "No_author_found" entries
        if author == 'No_author_found':
            continue

        # Categorize affiliation
        institution = categorize_affiliation(affiliation)

        if institution:
            industry_citations[institution]['citing_papers'].add(citing_paper)
            industry_citations[institution]['citing_researchers'].add(author)
            industry_citations[institution]['raw_affiliations'].add(affiliation)

    # Generate table with ALL institutions (including zeros)
    results = []
    for institution in ALL_INSTITUTIONS:
        if institution in industry_citations:
            results.append({
                'Institution': institution,
                '# of citing papers': len(industry_citations[institution]['citing_papers']),
                '# of citing researchers': len(industry_citations[institution]['citing_researchers'])
            })
        else:
            # Institution not found in data - add with zeros
            results.append({
                'Institution': institution,
                '# of citing papers': 0,
                '# of citing researchers': 0
            })

    # Create DataFrame (keep in fixed order - no sorting)
    df = pd.DataFrame(results)
    df = df.reset_index(drop=True)

    return df, industry_citations


def create_detailed_report(industry_citations, output_file='results/industry_research_centers_detailed.csv'):
    """
    Create detailed report with researcher names and raw affiliation strings.
    """
    detailed_data = []

    for institution, data in sorted(industry_citations.items()):
        detailed_data.append({
            'Institution': institution,
            '# of citing papers': len(data['citing_papers']),
            '# of citing researchers': len(data['citing_researchers']),
            'Researchers': '; '.join(sorted(data['citing_researchers'])),
            'Raw Affiliations': ' | '.join(sorted(data['raw_affiliations']))
        })

    if detailed_data:
        df = pd.DataFrame(detailed_data)
        df = df.sort_values(by='# of citing researchers', ascending=False)

        # Ensure results directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)

        print(f"\nDetailed report saved to: {output_file}")


def main():
    print("Industry Research Centers Citation Analysis")
    print("=" * 80)

    # Load data
    cache_file = 'cache/HNw5OdcAAAAJ/author_paper_affiliation_tuple_list.pkl'

    if not os.path.exists(cache_file):
        print(f"Error: Cache file not found at {cache_file}")
        print("Please run incremental_scrape.py first to generate the affiliation cache.")
        return

    with open(cache_file, 'rb') as f:
        affiliation_data = pickle.load(f)

    print(f"Loaded {len(affiliation_data)} affiliation records\n")

    # Process
    df, industry_citations = create_industry_research_table(affiliation_data)

    # Export primary table
    output_file = 'results/industry_research_centers.csv'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)

    print(f"Industry research centers table saved to: {output_file}\n")

    # Display summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(df.to_string(index=False))
    print("\n")

    # Total statistics
    total_papers = df['# of citing papers'].sum()
    total_researchers = df['# of citing researchers'].sum()
    institutions_with_citations = len(df[df['# of citing researchers'] > 0])

    print(f"Total industry institutions tracked: {len(df)}")
    print(f"Institutions with citations: {institutions_with_citations}")
    print(f"Total citing papers: {total_papers}")
    print(f"Total citing researchers: {total_researchers}")

    # Create detailed report
    create_detailed_report(industry_citations)

    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("=" * 80)


if __name__ == '__main__':
    main()
