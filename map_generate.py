# remember to run `pip install citation-map` before executing this script

from citation_map import generate_citation_map

if __name__ == '__main__':
    scholar_id = 'HNw5OdcAAAAJ'  # This is my Google Scholar ID. Replace this with your ID.

    # IMPORTANT: Geocoding takes 15-30 minutes on first run
    # After first successful run, the geocodes are saved in results/citation_info.csv
    # Set parse_csv=True to skip geocoding and reuse cached geocodes from CSV (instant)

    generate_citation_map(
        scholar_id,
        output_path='results/citation_map.html',
        csv_output_path='results/citation_info.csv',
        num_processes=1,        # Reduced from default 16 to avoid getting blocked
        parse_csv=True,         # Set to True after first run to skip geocoding
        # use_proxy=True causes compatibility error with httpx
    )