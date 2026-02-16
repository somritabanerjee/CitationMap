#!/usr/bin/env python3
"""
Analyze citations from government-funded research centers.
Creates a table with institutions, # of citing papers, and # of citing researchers.
"""

import pickle
import pandas as pd
from collections import defaultdict
import os


def categorize_affiliation(affiliation_str):
    """
    Returns institution category or None if no match.
    Order matters for NASA to avoid overlap (JPL and Ames before generic NASA).
    """
    affiliation_lower = affiliation_str.lower()

    # NASA - hierarchical matching (specific to general)
    if 'jpl' in affiliation_lower or 'jet propulsion lab' in affiliation_lower:
        return 'NASA Jet Propulsion Lab'

    if 'ames' in affiliation_lower:
        return 'NASA Ames Research Center'

    if 'nasa' in affiliation_lower:
        return 'Other NASA'

    # European Space Agency
    if 'european space agency' in affiliation_lower or 'esa' in affiliation_lower:
        return 'European Space Agency (ESA), Europe'

    # German Aerospace Center
    if 'german aerospace center' in affiliation_lower or 'dlr' in affiliation_lower:
        return 'German Aerospace Center (DLR), Germany'

    # MIT Lincoln Lab
    if 'mit lincoln lab' in affiliation_lower:
        return 'MIT Lincoln Lab'

    # National Robotics Engineering Center
    if 'national robotics engineering center' in affiliation_lower:
        return 'National Robotics Engineering Center'

    # INRIA
    if 'inria' in affiliation_lower:
        return 'INRIA, France'

    # KAIST
    if 'kaist' in affiliation_lower or \
       'korea advanced institute of science and technology' in affiliation_lower:
        return 'Korea Advanced Institute of Science and Technology (KAIST), South Korea'

    # Technology Innovation Institute
    if 'technology innovation institute' in affiliation_lower or 'tii' in affiliation_lower:
        return 'Technology Innovation Institute (TII), UAE'

    # CNR-IEIIT
    if 'cnr-ieiit' in affiliation_lower or 'cnr ieiit' in affiliation_lower:
        return 'CNR-IEIIT, Italy'

    # UK Atomic Energy Authority
    if 'uk atomic energy authority' in affiliation_lower or \
       'atomic energy authority' in affiliation_lower:
        return 'UK Atomic Energy Authority, UK'

    return None


def create_government_centers_table(affiliation_data):
    """
    Process affiliation data and create government research centers summary.

    Returns:
        DataFrame with Institution, # of citing papers, # of citing researchers
    """
    # Define all institutions we're tracking (excluding "Other NASA")
    ALL_INSTITUTIONS = [
        'NASA Jet Propulsion Lab',
        'NASA Ames Research Center',
        'European Space Agency (ESA), Europe',
        'German Aerospace Center (DLR ), Germany',
        'MIT Lincoln Lab',
        'National Robotics Engineering Center',
        'INRIA, France',
        'Korea Advanced Institute of Science and Technology (KAIST), South Korea',
        'Technology Innovation Institute (TII), UAE',
        'CNR-IEIIT, Italy',
        'UK Atomic Energy Authority , UK'
    ]

    # Data structure to collect results
    gov_citations = defaultdict(lambda: {
        'citing_papers': set(),
        'citing_researchers': set(),
        'raw_affiliations': set()
    })

    # Track "Other NASA" separately for reporting
    other_nasa_count = 0
    other_nasa_affiliations = []

    # Process each citation record
    for author, citing_paper, cited_paper, affiliation in affiliation_data:
        # Skip "No_author_found" entries
        if author == 'No_author_found':
            continue

        # Categorize affiliation
        institution = categorize_affiliation(affiliation)

        if institution == 'Other NASA':
            # Track but don't include in table
            other_nasa_count += 1
            other_nasa_affiliations.append(affiliation)
        elif institution:
            gov_citations[institution]['citing_papers'].add(citing_paper)
            gov_citations[institution]['citing_researchers'].add(author)
            gov_citations[institution]['raw_affiliations'].add(affiliation)

    # Print message if "Other NASA" affiliations found
    if other_nasa_count > 0:
        print(f"\nℹ️  Found {other_nasa_count} citation(s) from 'Other NASA' affiliations (not JPL or Ames):")
        for affil in set(other_nasa_affiliations):
            print(f"   - {affil}")
        print()

    # Generate table with ALL institutions (including zeros)
    results = []
    for institution in ALL_INSTITUTIONS:
        if institution in gov_citations:
            results.append({
                'Institution': institution,
                '# of citing papers': len(gov_citations[institution]['citing_papers']),
                '# of citing researchers': len(gov_citations[institution]['citing_researchers'])
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

    return df, gov_citations


def create_detailed_report(gov_citations, output_file='results/government_research_centers_detailed.csv'):
    """
    Create detailed report with researcher names and raw affiliation strings.
    """
    detailed_data = []

    for institution, data in sorted(gov_citations.items()):
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
    print("Government-Funded Research Centers Citation Analysis")
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
    df, gov_citations = create_government_centers_table(affiliation_data)

    # Export primary table
    output_file = 'results/government_research_centers.csv'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)

    print(f"Government research centers table saved to: {output_file}\n")

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

    print(f"Total government institutions tracked: {len(df)}")
    print(f"Institutions with citations: {institutions_with_citations}")
    print(f"Total citing papers: {total_papers}")
    print(f"Total citing researchers: {total_researchers}")

    # Create detailed report
    create_detailed_report(gov_citations)

    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("=" * 80)


if __name__ == '__main__':
    main()
