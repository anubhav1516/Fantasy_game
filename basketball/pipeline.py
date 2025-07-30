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
    
    if 'NBA' in dir_name and 'WNBA' not in dir_name:
        if 'Cup' in dir_name or 'Tournament' in dir_name:
            return {
                'league': 'NBA',
                'season': dir_name.replace('season_', ''),
                'type': 'tournament'
            }
        season = dir_name.replace('season_NBA ', '')
        year = f"20{season.split('-')[0]}-20{season.split('-')[1]}"
        return {'league': 'NBA', 'season': year, 'type': 'regular'}
    else:
        parts = dir_name.split('_')
        return {'league': 'WNBA', 'season': parts[-1], 'type': 'regular'}

def count_seasons(data_dir: str) -> int:
    """Count total number of seasons across NBA and WNBA."""
    count = 0
    for league in ['nba', 'wnba']:
        league_path = os.path.join(data_dir, league)
        count += sum(1 for d in os.listdir(league_path) if d.startswith('season'))
    return count

def extract_matches(data_dir: str, verbose: bool = True) -> Generator[Dict, None, None]:
    """Extract match list data from all seasons."""
    match_count = 0
    for league in ['nba', 'wnba']:
        league_path = os.path.join(data_dir, league)
        for season_dir in sorted(os.listdir(league_path)):
            if not season_dir.startswith('season'):
                continue
                
            if verbose:
                print(f"Processing {season_dir}...")
                
            season_path = os.path.join(league_path, season_dir)
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
    for league in ['nba', 'wnba']:
        league_path = os.path.join(data_dir, league)
        for season_dir in os.listdir(league_path):
            if not season_dir.startswith('season'):
                continue
                
            season_path = os.path.join(league_path, season_dir)
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

def extract_player_statistics(data_dir: str) -> Generator[Dict, None, None]:
    """Extract player statistics from all seasons."""
    for league in ['nba', 'wnba']:
        league_path = os.path.join(data_dir, league)
        for season_dir in os.listdir(league_path):
            if not season_dir.startswith('season'):
                continue
                
            season_path = os.path.join(league_path, season_dir)
            stats_path = os.path.join(season_path, 'player_statistics')
            
            if not os.path.exists(stats_path):
                continue
                
            season_info = get_season_info(season_dir)
            
            for stats_file in os.listdir(stats_path):
                if not stats_file.endswith('.json'):
                    continue
                    
                match_id = stats_file.replace('.json', '')
                stats = load_json_file(os.path.join(stats_path, stats_file))
                
                # Process home team players
                for player in stats.get('home', {}).get('players', []):
                    player_data = {
                        'match_id': match_id,
                        'team_id': player.get('teamId'),
                        'is_home': True,
                        'player': player.get('player'),
                        'statistics': player.get('statistics'),
                        **season_info
                    }
                    yield player_data
                
                # Process away team players
                for player in stats.get('away', {}).get('players', []):
                    player_data = {
                        'match_id': match_id,
                        'team_id': player.get('teamId'),
                        'is_home': False,
                        'player': player.get('player'),
                        'statistics': player.get('statistics'),
                        **season_info
                    }
                    yield player_data

def create_tables(conn: duckdb.DuckDBPyConnection):
    """Create necessary tables if they don't exist"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id BIGINT,
            tournament__name VARCHAR,
            tournament__unique_tournament__name VARCHAR,
            season VARCHAR,
            league VARCHAR,
            type VARCHAR,
            status__description VARCHAR,
            home_team__name VARCHAR,
            home_team__id BIGINT,
            away_team__name VARCHAR,
            away_team__id BIGINT,
            home_score__current BIGINT,
            away_score__current BIGINT,
            home_score__period1 BIGINT,
            home_score__period2 BIGINT,
            home_score__period3 BIGINT,
            home_score__period4 BIGINT,
            away_score__period1 BIGINT,
            away_score__period2 BIGINT,
            away_score__period3 BIGINT,
            away_score__period4 BIGINT,
            winner_code BIGINT,
            start_timestamp BIGINT
        )
    """)

def create_views(conn: duckdb.DuckDBPyConnection):
    """Create analytics views"""
    # Season summary view
    try:
        conn.execute("""
            CREATE OR REPLACE VIEW season_summary AS
            SELECT 
                league,
                season,
                type,
                COUNT(DISTINCT id) as total_matches,
                COUNT(DISTINCT home_team__id) + COUNT(DISTINCT away_team__id) as total_teams,
                AVG(home_score__current + away_score__current) as avg_total_points
            FROM matches
            GROUP BY league, season, type
            ORDER BY league, season DESC
        """)
    except Exception as e:
        print(f"Warning: Could not create season_summary view: {e}")

    # Team performance view
    try:
        conn.execute("""
            CREATE OR REPLACE VIEW team_performance AS
            WITH team_games AS (
                -- Home games
                SELECT 
                    home_team__name as team_name,
                    league,
                    season,
                    CASE WHEN winner_code = 1 THEN 1 ELSE 0 END as is_winner,
                    home_score__current as points_scored,
                    away_score__current as points_conceded
                FROM matches
                WHERE home_team__name != 'Unknown'
                UNION ALL
                -- Away games
                SELECT 
                    away_team__name as team_name,
                    league,
                    season,
                    CASE WHEN winner_code = 2 THEN 1 ELSE 0 END as is_winner,
                    away_score__current as points_scored,
                    home_score__current as points_conceded
                FROM matches
                WHERE away_team__name != 'Unknown'
            )
            SELECT 
                team_name,
                league,
                season,
                COUNT(*) as games_played,
                SUM(is_winner) as wins,
                COUNT(*) - SUM(is_winner) as losses,
                ROUND(AVG(points_scored), 2) as avg_points_scored,
                ROUND(AVG(points_conceded), 2) as avg_points_conceded
            FROM team_games
            GROUP BY team_name, league, season
            ORDER BY league, season DESC, wins DESC
        """)
    except Exception as e:
        print(f"Warning: Could not create team_performance view: {e}")

    # Quarter scoring analysis view
    try:
        conn.execute("""
            CREATE OR REPLACE VIEW quarter_analysis AS
            SELECT 
                league,
                season,
                'Q1' as quarter,
                AVG(home_score__period1 + away_score__period1) as avg_points
            FROM matches
            GROUP BY league, season
            UNION ALL
            SELECT 
                league,
                season,
                'Q2' as quarter,
                AVG(home_score__period2 + away_score__period2) as avg_points
            FROM matches
            GROUP BY league, season
            UNION ALL
            SELECT 
                league,
                season,
                'Q3' as quarter,
                AVG(home_score__period3 + away_score__period3) as avg_points
            FROM matches
            GROUP BY league, season
            UNION ALL
            SELECT 
                league,
                season,
                'Q4' as quarter,
                AVG(home_score__period4 + away_score__period4) as avg_points
            FROM matches
            GROUP BY league, season
            ORDER BY league, season DESC, quarter
        """)
    except Exception as e:
        print(f"Warning: Could not create quarter_analysis view: {e}")

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
                league,
                season,
                home_team__name || ' vs ' || away_team__name as matchup,
                home_score__current || '-' || away_score__current as score,
                tournament__name as tournament
            FROM matches 
            LIMIT 3
        """).fetchall()
        print_table(matches, ["League", "Season", "Matchup", "Score", "Tournament"])

        # League summary
        print("\nLeague Summary:")
        summary = conn.execute("""
            SELECT 
                league,
                COUNT(DISTINCT season) as seasons,
                COUNT(*) as total_matches,
                ROUND(AVG(home_score__current + away_score__current), 2) as avg_total_points
            FROM matches
            GROUP BY league
            ORDER BY total_matches DESC
        """).fetchall()
        print_table(summary, ["League", "Seasons", "Matches", "Avg Points"])

        # Tournament summary
        print("\nTournament Summary:")
        tournaments = conn.execute("""
            SELECT 
                tournament__name,
                COUNT(*) as matches,
                COUNT(DISTINCT season) as seasons
            FROM matches
            GROUP BY tournament__name
            ORDER BY matches DESC
            LIMIT 5
        """).fetchall()
        print_table(tournaments, ["Tournament", "Matches", "Seasons"])

    except Exception as e:
        print(f"Error showing database summary: {e}")

def verify_database():
    """Verify database contents and structure"""
    with closing(duckdb.connect('basketball_stats.duckdb')) as conn:
        print("\nDatabase Verification:")
        
        # Check data quality
        print("\nData Quality Check:")
        quality = conn.execute("""
            SELECT 
                COUNT(*) as total_matches,
                COUNT(CASE WHEN home_score__current IS NULL THEN 1 END) as missing_scores,
                COUNT(CASE WHEN home_team__name = 'Unknown' OR away_team__name = 'Unknown' THEN 1 END) as unknown_teams,
                COUNT(DISTINCT tournament__name) as tournaments,
                COUNT(DISTINCT CONCAT(home_team__name, away_team__name)) as unique_matchups
            FROM matches
        """).fetchall()
        print_table(quality, ["Total Matches", "Missing Scores", "Unknown Teams", "Tournaments", "Unique Matchups"])
        
        # Check score distribution
        print("\nScore Distribution:")
        scores = conn.execute("""
            SELECT 
                league,
                ROUND(AVG(home_score__current + away_score__current), 2) as avg_total_points,
                MIN(home_score__current + away_score__current) as min_total_points,
                MAX(home_score__current + away_score__current) as max_total_points
            FROM matches
            GROUP BY league
        """).fetchall()
        print_table(scores, ["League", "Avg Points", "Min Points", "Max Points"])
        
        # Database file size
        db_size = os.path.getsize('basketball_stats.duckdb') / (1024 * 1024)
        print(f"\nDatabase file size: {db_size:.2f} MB")

def print_table(data, headers):
    widths = [max(len(str(row[i])) for row in data + [headers]) for i in range(len(headers))]
    print(' | '.join(f'{h:<{w}}' for h, w in zip(headers, widths)))
    print('-' * sum(widths + [len(headers) * 3 - 2]))
    for row in data:
        print(' | '.join(f'{str(cell):<{w}}' for cell, w in zip(row, widths)))

def main():
    db_path = "basketball_stats.duckdb"
    
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
        pipeline = dlt.pipeline(pipeline_name='basketball_stats', destination='duckdb', dataset_name='basketball')

        print("Loading matches data...")
        matches_data = extract_matches('data', verbose=True)
        matches_info = pipeline.run(matches_data, table_name='matches', write_disposition='replace')
        print(f"Matches loaded: {matches_info}")

        print("\nLoading match statistics...")
        match_stats_data = extract_match_statistics('data')
        match_stats_info = pipeline.run(match_stats_data, table_name='match_statistics', write_disposition='replace')
        print(f"Match statistics loaded: {match_stats_info}")

        print("\nLoading player statistics...")
        player_stats_data = extract_player_statistics('data')
        player_stats_info = pipeline.run(player_stats_data, table_name='player_statistics', write_disposition='replace')
        print(f"Player statistics loaded: {player_stats_info}")

        # Let's examine the table structure before creating views
        conn = duckdb.connect(db_path)
        conn.execute("USE basketball;")
        
        # Print table schemas to debug
        print("\nTable Schemas:")
        for table in ['matches', 'match_statistics', 'player_statistics']:
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
        print("3. Schema name: basketball")

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
