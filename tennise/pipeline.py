import dlt
import json
import os
import duckdb
from typing import Dict, List, Generator
from pathlib import Path
from contextlib import closing

def load_json_file(file_path: str) -> Dict:
    """Load and return JSON data from a file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def get_season_info(season_dir: str) -> Dict:
    """Extract season information from directory name."""
    dir_name = os.path.basename(season_dir)
    parts = dir_name.split('_')
    
    # Extract year from the season directory name
    year = parts[-1]
    
    # Determine if it's ATP or WTA
    tour = "ATP" if "ATP" in dir_name else "WTA"
    
    return {
        'tour': tour,
        'year': year,
        'tournament': 'Roland Garros'
    }

def count_seasons(data_dir: str) -> int:
    """Count total number of seasons across ATP and WTA Roland Garros."""
    count = 0
    for tour in ['Roland Garros(ATP)', 'Roland Garros(WTA)']:
        tour_path = os.path.join(data_dir, tour)
        count += sum(1 for d in os.listdir(tour_path) if d.startswith('season'))
    return count

def extract_matches(data_dir: str, verbose: bool = True) -> Generator[Dict, None, None]:
    """Extract match list data from all seasons."""
    match_count = 0
    for tour in ['Roland Garros(ATP)', 'Roland Garros(WTA)']:
        tour_path = os.path.join(data_dir, tour)
        for season_dir in sorted(os.listdir(tour_path)):
            # Skip the seasons_list.json file and any non-season directories
            if not season_dir.startswith('season_'):  # Changed from 'season' to 'season_'
                continue
                
            if verbose:
                print(f"Processing {season_dir}...")
                
            season_path = os.path.join(tour_path, season_dir)
            match_list_path = os.path.join(season_path, 'match_list.json')
            
            if not os.path.exists(match_list_path):
                if verbose:
                    print(f"Warning: Match file not found: {match_list_path}")
                continue
                
            season_info = get_season_info(season_dir)
            matches = load_json_file(match_list_path)
            
            if verbose:
                print(f"Found {len(matches)} matches in {season_dir}")
            
            match_count += len(matches)
            for match in matches:
                match.update(season_info)
                yield match
    
    if verbose:
        print(f"\nProcessed {match_count} matches\n")

def extract_match_statistics(data_dir: str) -> Generator[Dict, None, None]:
    """Extract match statistics from all seasons."""
    for tour in ['Roland Garros(ATP)', 'Roland Garros(WTA)']:
        tour_path = os.path.join(data_dir, tour)
        for season_dir in os.listdir(tour_path):
            if not season_dir.startswith('season_'):  # Changed from 'season' to 'season_'
                continue
                
            season_path = os.path.join(tour_path, season_dir)
            stats_path = os.path.join(season_path, 'match_statistics')
            
            if not os.path.exists(stats_path):
                continue
                
            season_info = get_season_info(season_dir)
            
            for stats_file in os.listdir(stats_path):
                if not stats_file.endswith('.json'):
                    continue
                    
                match_id = stats_file.replace('.json', '')
                stats = load_json_file(os.path.join(stats_path, stats_file))
                stats.update(season_info)
                stats['match_id'] = match_id
                yield stats

def create_tables(conn: duckdb.DuckDBPyConnection):
    """Create necessary tables if they don't exist"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id BIGINT,
            tournament VARCHAR,
            tour VARCHAR,
            year VARCHAR,
            status__description VARCHAR,
            home_team__name VARCHAR,
            home_team__id BIGINT,
            away_team__name VARCHAR,
            away_team__id BIGINT,
            home_score__current BIGINT,
            away_score__current BIGINT,
            winner_code BIGINT,
            start_timestamp BIGINT,
            ground_type VARCHAR
        )
    """)

def create_views(conn: duckdb.DuckDBPyConnection):
    """Create analytics views"""
    # Season summary view
    try:
        conn.execute("""
            CREATE OR REPLACE VIEW season_summary AS
            SELECT 
                tour,
                year,
                COUNT(DISTINCT id) as total_matches,
                COUNT(DISTINCT home_team__id) + COUNT(DISTINCT away_team__id) as total_players
            FROM matches
            GROUP BY tour, year
            ORDER BY tour, year DESC
        """)
    except Exception as e:
        print(f"Warning: Could not create season_summary view: {e}")

    # Player performance view
    try:
        conn.execute("""
            CREATE OR REPLACE VIEW player_performance AS
            WITH player_matches AS (
                -- Home matches
                SELECT 
                    home_team__name as player_name,
                    tour,
                    year,
                    CASE WHEN winner_code = 1 THEN 1 ELSE 0 END as is_winner
                FROM matches
                WHERE home_team__name != 'Unknown'
                UNION ALL
                -- Away matches
                SELECT 
                    away_team__name as player_name,
                    tour,
                    year,
                    CASE WHEN winner_code = 2 THEN 1 ELSE 0 END as is_winner
                FROM matches
                WHERE away_team__name != 'Unknown'
            )
            SELECT 
                player_name,
                tour,
                year,
                COUNT(*) as matches_played,
                SUM(is_winner) as wins,
                COUNT(*) - SUM(is_winner) as losses
            FROM player_matches
            GROUP BY player_name, tour, year
            ORDER BY tour, year DESC, wins DESC
        """)
    except Exception as e:
        print(f"Warning: Could not create player_performance view: {e}")

    # Add a view for surface statistics
    try:
        conn.execute("""
            CREATE OR REPLACE VIEW surface_statistics AS
            SELECT 
                tour,
                year,
                ground_type,
                COUNT(*) as matches_played,
                COUNT(DISTINCT home_team__name) + COUNT(DISTINCT away_team__name) as unique_players
            FROM matches
            WHERE ground_type IS NOT NULL
            GROUP BY tour, year, ground_type
            ORDER BY tour, year DESC, matches_played DESC
        """)
    except Exception as e:
        print(f"Warning: Could not create surface_statistics view: {e}")

def show_database_summary(conn):
    """Display summary information about the database contents."""
    try:
        print("\nFinal Database Contents:")
        
        # Get table counts
        tables = conn.execute("""
            SELECT 
                table_name,
                COUNT(*) as row_count
            FROM (
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                AND table_type = 'BASE TABLE'
            ) t
            CROSS JOIN matches
            GROUP BY table_name
        """).fetchall()
        
        print("\nTable Counts:")
        for table, count in tables:
            print(f"{table}: {count:,} rows")

        # Sample matches data
        print("\nSample Matches Data:")
        matches = conn.execute("""
            SELECT 
                tour,
                year,
                home_team__name || ' vs ' || away_team__name as matchup,
                home_score__current || '-' || away_score__current as score,
                ground_type
            FROM matches 
            LIMIT 3
        """).fetchall()
        print_table(matches, ["Tour", "Year", "Matchup", "Score", "Surface"])

        # Tour summary
        print("\nTour Summary:")
        summary = conn.execute("""
            SELECT 
                tour,
                COUNT(DISTINCT year) as years,
                COUNT(*) as total_matches,
                COUNT(DISTINCT home_team__name) + COUNT(DISTINCT away_team__name) as unique_players
            FROM matches
            GROUP BY tour
            ORDER BY total_matches DESC
        """).fetchall()
        print_table(summary, ["Tour", "Years", "Matches", "Players"])

        # Surface distribution
        print("\nSurface Distribution:")
        surfaces = conn.execute("""
            SELECT 
                ground_type,
                COUNT(*) as matches,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM matches
            WHERE ground_type IS NOT NULL
            GROUP BY ground_type
            ORDER BY matches DESC
        """).fetchall()
        print_table(surfaces, ["Surface", "Matches", "Percentage"])

    except Exception as e:
        print(f"Error showing database summary: {e}")

def verify_database():
    """Verify database contents and structure"""
    with closing(duckdb.connect('tennis_stats.duckdb')) as conn:
        print("\nDatabase Verification:")
        
        # Check data quality
        print("\nData Quality Check:")
        quality = conn.execute("""
            SELECT 
                COUNT(*) as total_matches,
                COUNT(CASE WHEN home_score__current IS NULL THEN 1 END) as missing_scores,
                COUNT(CASE WHEN home_team__name = 'Unknown' OR away_team__name = 'Unknown' THEN 1 END) as unknown_players,
                COUNT(DISTINCT CONCAT(home_team__name, away_team__name)) as unique_matchups
            FROM matches
        """).fetchall()
        print_table(quality, ["Total Matches", "Missing Scores", "Unknown Players", "Unique Matchups"])
        
        # Database file size
        db_size = os.path.getsize('tennis_stats.duckdb') / (1024 * 1024)
        print(f"\nDatabase file size: {db_size:.2f} MB")

def print_table(data, headers):
    widths = [max(len(str(row[i])) for row in data + [headers]) for i in range(len(headers))]
    print(' | '.join(f'{h:<{w}}' for h, w in zip(headers, widths)))
    print('-' * sum(widths + [len(headers) * 3 - 2]))
    for row in data:
        print(' | '.join(f'{str(cell):<{w}}' for cell, w in zip(row, widths)))

def main():
    db_path = "tennis_stats.duckdb"
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        print("Removing existing database...")
        os.remove(db_path)
        if os.path.exists(f"{db_path}.wal"):
            os.remove(f"{db_path}.wal")
    
    # Count total seasons
    total_seasons = count_seasons('data')
    print(f"\nFound {total_seasons} seasons to process\n")
    
    pipeline = None
    conn = None
    try:
        # Initialize DLT pipeline
        pipeline = dlt.pipeline(pipeline_name='tennis_stats', destination='duckdb', dataset_name='tennis')

        print("Loading matches data...")
        matches_data = extract_matches('data', verbose=True)
        matches_info = pipeline.run(matches_data, table_name='matches', write_disposition='replace')
        print(f"Matches loaded: {matches_info}")

        print("\nLoading match statistics...")
        match_stats_data = extract_match_statistics('data')
        match_stats_info = pipeline.run(match_stats_data, table_name='match_statistics', write_disposition='replace')
        print(f"Match statistics loaded: {match_stats_info}")

        # Let's examine the table structure before creating views
        conn = duckdb.connect(db_path)
        conn.execute("USE tennis;")
        
        # Print table schemas to debug
        print("\nTable Schemas:")
        for table in ['matches', 'match_statistics']:
            try:
                schema = conn.execute(f"DESCRIBE {table};").fetchall()
                print(f"\n{table} schema:")
                for col in schema:
                    print(f"  {col[0]}: {col[1]}")
            except Exception as e:
                print(f"Error getting schema for {table}: {e}")
        
        print("\nCreating database views...")
        create_views(conn)
        
        # Show database summary
        show_database_summary(conn)

        print("\nTo connect in DBeaver:")
        print(f"1. Database location: {os.path.abspath(db_path)}")
        print("2. Driver: DuckDB")
        print("3. Schema name: tennis")

    except Exception as e:
        print(f"Error during pipeline execution: {e}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    main()
