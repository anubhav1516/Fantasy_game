import duckdb
import json
import os
from typing import Dict, List, Generator, Optional
from contextlib import closing

# Function to read season data
def read_seasons() -> Dict:
    with open('data/seasons_list.json', 'r') as f:
        return json.load(f)

def safe_get_score(score_dict: Dict, key: str, default: int = 0) -> int:
    """Safely get score value with fallback to default"""
    if not isinstance(score_dict, dict):
        return default
    return score_dict.get(key, default)

def transform_match(match: Dict) -> Optional[Dict]:
    """Transform raw match data into structured format with error handling"""
    try:
        # Default score dictionary structure
        default_score = {
            'current': 0,
            'period1': 0,
            'period2': 0,
            'period3': 0,
            'period4': 0
        }

        # Get score dictionaries with defaults
        home_score = match.get('homeScore', default_score)
        away_score = match.get('awayScore', default_score)

        return {
            'match_id': match.get('id', 0),
            'season': match.get('season', ''),
            'start_timestamp': match.get('startTimestamp', 0),
            'status': match.get('status', {}).get('description', 'Unknown'),
            'home_team_id': match.get('homeTeam', {}).get('id', 0),
            'home_team_name': match.get('homeTeam', {}).get('name', 'Unknown'),
            'home_team_code': match.get('homeTeam', {}).get('nameCode', 'UNK'),
            'away_team_id': match.get('awayTeam', {}).get('id', 0),
            'away_team_name': match.get('awayTeam', {}).get('name', 'Unknown'),
            'away_team_code': match.get('awayTeam', {}).get('nameCode', 'UNK'),
            'home_score': safe_get_score(home_score, 'current'),
            'away_score': safe_get_score(away_score, 'current'),
            'home_period1': safe_get_score(home_score, 'period1'),
            'home_period2': safe_get_score(home_score, 'period2'),
            'home_period3': safe_get_score(home_score, 'period3'),
            'home_period4': safe_get_score(home_score, 'period4'),
            'away_period1': safe_get_score(away_score, 'period1'),
            'away_period2': safe_get_score(away_score, 'period2'),
            'away_period3': safe_get_score(away_score, 'period3'),
            'away_period4': safe_get_score(away_score, 'period4'),
            'winner_code': match.get('winnerCode', None)
        }
    except Exception as e:
        print(f"Error transforming match: {str(e)}")
        return None

def create_tables(conn: duckdb.DuckDBPyConnection):
    """Create necessary tables if they don't exist"""
    # Create seasons table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seasons (
            id INTEGER,
            name VARCHAR,
            year VARCHAR,
            editor BOOLEAN
        )
    """)
    
    # Create matches table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER,
            season VARCHAR,
            start_timestamp INTEGER,
            status VARCHAR,
            home_team_id INTEGER,
            home_team_name VARCHAR,
            home_team_code VARCHAR,
            away_team_id INTEGER,
            away_team_name VARCHAR,
            away_team_code VARCHAR,
            home_score INTEGER,
            away_score INTEGER,
            home_period1 INTEGER,
            home_period2 INTEGER,
            home_period3 INTEGER,
            home_period4 INTEGER,
            away_period1 INTEGER,
            away_period2 INTEGER,
            away_period3 INTEGER,
            away_period4 INTEGER,
            winner_code INTEGER
        )
    """)

def create_views(conn: duckdb.DuckDBPyConnection):
    """Create analytics views"""
    # Create season summary view
    conn.execute("""
        CREATE OR REPLACE VIEW season_summary AS
        SELECT 
            m.season,
            COUNT(DISTINCT m.match_id) as total_matches,
            COUNT(DISTINCT m.home_team_id) + COUNT(DISTINCT m.away_team_id) as total_teams,
            AVG(m.home_score + m.away_score) as avg_total_points
        FROM matches m
        GROUP BY m.season
        ORDER BY m.season DESC
    """)
    
    # Create team performance view
    conn.execute("""
        CREATE OR REPLACE VIEW team_performance AS
        WITH team_games AS (
            -- Home games
            SELECT 
                home_team_name as team_name,
                season,
                CASE WHEN winner_code = 1 THEN 1 ELSE 0 END as is_winner,
                home_score as points_scored,
                away_score as points_conceded
            FROM matches
            WHERE home_team_name != 'Error'
            UNION ALL
            -- Away games
            SELECT 
                away_team_name as team_name,
                season,
                CASE WHEN winner_code = 2 THEN 1 ELSE 0 END as is_winner,
                away_score as points_scored,
                home_score as points_conceded
            FROM matches
            WHERE away_team_name != 'Error'
        )
        SELECT 
            team_name,
            season,
            COUNT(*) as games_played,
            SUM(is_winner) as wins,
            COUNT(*) - SUM(is_winner) as losses,
            ROUND(AVG(points_scored), 2) as avg_points_scored,
            ROUND(AVG(points_conceded), 2) as avg_points_conceded
        FROM team_games
        GROUP BY team_name, season
        ORDER BY season DESC, wins DESC
    """)

def verify_data_loading(conn: duckdb.DuckDBPyConnection):
    """Verify data was loaded correctly"""
    print("\nVerifying data loading:")
    
    # Check seasons
    season_count = conn.execute("SELECT COUNT(*) FROM seasons").fetchone()[0]
    print(f"Seasons loaded: {season_count}")
    
    # Check matches
    match_count = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    print(f"Matches loaded: {match_count}")

def process_nfl_data():
    """Main function to process NFL data"""
    # Use with closing to ensure connection is properly closed
    with closing(duckdb.connect('nfl_analytics.duckdb')) as conn:
        try:
            # Create tables
            create_tables(conn)
            
            # Get seasons data
            seasons_data = read_seasons()
            season_count = len(seasons_data['seasons'])
            print(f"\nFound {season_count} seasons to process")
            
            if season_count == 0:
                print("Error: No seasons found in seasons_list.json")
                return
            
            # Load seasons directly using DuckDB
            print("\nLoading seasons data...")
            conn.execute("DELETE FROM seasons")  # Clear existing data
            for season in seasons_data['seasons']:
                conn.execute("""
                    INSERT INTO seasons (id, name, year, editor)
                    VALUES (?, ?, ?, ?)
                """, [season['id'], season['name'], season['year'], season['editor']])
            
            # Verify seasons were loaded
            actual_seasons = conn.execute("SELECT COUNT(*) FROM seasons").fetchone()[0]
            print(f"Loaded {actual_seasons} seasons into database")
            
            # Load matches for each season
            print("\nLoading matches data...")
            conn.execute("DELETE FROM matches")  # Clear existing data
            
            total_matches = 0
            for season in seasons_data['seasons']:
                season_folder = f"season_{season['year'].replace('/', '-')}"
                print(f"Processing {season_folder}...")
                
                match_file = os.path.join('data', season_folder, 'match_list.json')
                if not os.path.exists(match_file):
                    print(f"Warning: Match file not found: {match_file}")
                    continue
                    
                with open(match_file, 'r') as f:
                    try:
                        matches = json.load(f)
                        print(f"Found {len(matches)} matches in {season_folder}")
                        
                        for match in matches:
                            match['season'] = season_folder.replace('season_', '').replace('-', '/')
                            transformed_match = transform_match(match)
                            if transformed_match:
                                total_matches += 1
                                # Insert match data directly
                                conn.execute("""
                                    INSERT INTO matches (
                                        match_id, season, start_timestamp, status,
                                        home_team_id, home_team_name, home_team_code,
                                        away_team_id, away_team_name, away_team_code,
                                        home_score, away_score,
                                        home_period1, home_period2, home_period3, home_period4,
                                        away_period1, away_period2, away_period3, away_period4,
                                        winner_code
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, [
                                    transformed_match['match_id'], transformed_match['season'],
                                    transformed_match['start_timestamp'], transformed_match['status'],
                                    transformed_match['home_team_id'], transformed_match['home_team_name'],
                                    transformed_match['home_team_code'], transformed_match['away_team_id'],
                                    transformed_match['away_team_name'], transformed_match['away_team_code'],
                                    transformed_match['home_score'], transformed_match['away_score'],
                                    transformed_match['home_period1'], transformed_match['home_period2'],
                                    transformed_match['home_period3'], transformed_match['home_period4'],
                                    transformed_match['away_period1'], transformed_match['away_period2'],
                                    transformed_match['away_period3'], transformed_match['away_period4'],
                                    transformed_match['winner_code']
                                ])
                    except json.JSONDecodeError as e:
                        print(f"Error reading {match_file}: {str(e)}")
                        continue
                    except Exception as e:
                        print(f"Error processing {match_file}: {str(e)}")
                        continue

            print(f"\nProcessed {total_matches} matches")

            # Verify final data counts
            final_seasons = conn.execute("SELECT COUNT(*) FROM seasons").fetchone()[0]
            final_matches = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
            
            print("\nFinal Database Contents:")
            print(f"Seasons: {final_seasons}")
            print(f"Matches: {final_matches}")

            if final_seasons == 0 or final_matches == 0:
                print("\nWARNING: No data was loaded into the database!")
                return

            # Create views after data is loaded
            print("\nCreating views...")
            create_views(conn)
            
            # Show sample data
            print("\nSample Season Data:")
            seasons = conn.execute("""
                SELECT * FROM seasons 
                ORDER BY year DESC 
                LIMIT 3
            """).fetchall()
            print_table(seasons, ["ID", "Name", "Year", "Editor"])
            
            print("\nSample Match Data:")
            matches = conn.execute("""
                SELECT 
                    season,
                    home_team_name || ' vs ' || away_team_name as matchup,
                    home_score || '-' || away_score as score
                FROM matches 
                LIMIT 3
            """).fetchall()
            print_table(matches, ["Season", "Matchup", "Score"])
            
            print("\nPipeline completed successfully!")
            
        except Exception as e:
            print(f"\nError in pipeline: {str(e)}")
            print(f"Error type: {type(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            raise

def print_table(data, headers):
    """Helper function to print formatted tables"""
    if not data:
        print("No data found")
        return
    
    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in data:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    
    # Print headers
    header = " | ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
    print(header)
    print("-" * len(header))
    
    # Print data
    for row in data:
        print(" | ".join(f"{str(cell):<{w}}" for cell, w in zip(row, widths)))

def query_database():
    """Function to safely query the database"""
    with closing(duckdb.connect('nfl_analytics.duckdb')) as conn:
        try:
            # Verify data is present
            seasons_count = conn.execute("SELECT COUNT(*) FROM seasons").fetchone()[0]
            matches_count = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
            
            print(f"\nDatabase contains:")
            print(f"- {seasons_count} seasons")
            print(f"- {matches_count} matches")
            
            if seasons_count == 0 or matches_count == 0:
                print("\nWARNING: No data found in the database!")
                return
            
            # 1. Season Summary with more details
            print("\n1. Detailed Season Summary:")
            result = conn.execute("""
                SELECT 
                    season,
                    total_matches,
                    total_teams,
                    ROUND(avg_total_points, 2) as avg_points_per_game,
                    ROUND(AVG(total_matches * 1.0 / total_teams), 2) as avg_games_per_team
                FROM season_summary 
                GROUP BY season, total_matches, total_teams, avg_total_points
                ORDER BY season DESC
                LIMIT 5
            """).fetchall()
            print_table(result, ["Season", "Matches", "Teams", "Avg Points/Game", "Avg Games/Team"])
            
            # 2. Highest Scoring Games
            print("\n2. Top 5 Highest Scoring Games:")
            result = conn.execute("""
                SELECT 
                    season,
                    home_team_name || ' vs ' || away_team_name as matchup,
                    home_score || '-' || away_score as score,
                    (home_score + away_score) as total_points
                FROM matches 
                ORDER BY total_points DESC
                LIMIT 5
            """).fetchall()
            print_table(result, ["Season", "Matchup", "Score", "Total Points"])
            
            # 3. Best Performing Teams (All Time)
            print("\n3. Best Performing Teams (All Time):")
            result = conn.execute("""
                SELECT 
                    team_name,
                    SUM(games_played) as total_games,
                    SUM(wins) as total_wins,
                    ROUND(SUM(wins) * 100.0 / SUM(games_played), 2) as win_percentage,
                    ROUND(AVG(avg_points_scored), 2) as avg_points_per_game
                FROM team_performance
                GROUP BY team_name
                HAVING total_games >= 50  -- Only teams with significant number of games
                ORDER BY win_percentage DESC
                LIMIT 5
            """).fetchall()
            print_table(result, ["Team", "Games", "Wins", "Win %", "Avg Points"])
            
            # 4. Most Competitive Rivalries
            print("\n4. Most Competitive Rivalries:")
            result = conn.execute("""
                WITH rivalries AS (
                    SELECT 
                        CASE 
                            WHEN home_team_name < away_team_name 
                            THEN home_team_name || ' vs ' || away_team_name
                            ELSE away_team_name || ' vs ' || home_team_name
                        END as rivalry,
                        COUNT(*) as meetings,
                        AVG(ABS(home_score - away_score)) as avg_point_diff
                    FROM matches
                    GROUP BY 
                        CASE 
                            WHEN home_team_name < away_team_name 
                            THEN home_team_name || ' vs ' || away_team_name
                            ELSE away_team_name || ' vs ' || home_team_name
                        END
                    HAVING meetings >= 10  -- Only rivalries with significant meetings
                )
                SELECT 
                    rivalry,
                    meetings,
                    ROUND(avg_point_diff, 2) as avg_point_difference
                FROM rivalries
                ORDER BY avg_point_diff ASC
                LIMIT 5
            """).fetchall()
            print_table(result, ["Rivalry", "Meetings", "Avg Point Diff"])
            
            # 5. Quarter Analysis
            print("\n5. Highest Scoring Quarters (Average):")
            result = conn.execute("""
                SELECT 
                    'Q1' as quarter,
                    ROUND(AVG(home_period1 + away_period1), 2) as avg_points
                FROM matches
                UNION ALL
                SELECT 
                    'Q2',
                    ROUND(AVG(home_period2 + away_period2), 2)
                FROM matches
                UNION ALL
                SELECT 
                    'Q3',
                    ROUND(AVG(home_period3 + away_period3), 2)
                FROM matches
                UNION ALL
                SELECT 
                    'Q4',
                    ROUND(AVG(home_period4 + away_period4), 2)
                FROM matches
                ORDER BY avg_points DESC
            """).fetchall()
            print_table(result, ["Quarter", "Avg Points"])
            
            # 6. Home vs Away Advantage Analysis
            print("\n6. Home vs Away Advantage Analysis:")
            result = conn.execute("""
                WITH game_results AS (
                    SELECT 
                        season,
                        CASE WHEN home_score > away_score THEN 1 ELSE 0 END as home_win
                    FROM matches
                    WHERE home_score != away_score  -- Exclude ties
                )
                SELECT 
                    COUNT(*) as total_games,
                    SUM(home_win) as home_wins,
                    COUNT(*) - SUM(home_win) as away_wins,
                    ROUND(SUM(home_win) * 100.0 / COUNT(*), 2) as home_win_percentage
                FROM game_results
            """).fetchall()
            print_table(result, ["Total Games", "Home Wins", "Away Wins", "Home Win %"])
            
        except Exception as e:
            print(f"Error querying database: {str(e)}")
            print("Full error:", str(e.__class__), str(e))

def verify_database():
    """Verify database contents and structure"""
    with closing(duckdb.connect('nfl_analytics.duckdb')) as conn:
        print("\nDatabase Verification:")
        
        # 1. Check tables and views
        print("\nAvailable Tables and Views:")
        tables = conn.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()
        print_table(tables, ["Name", "Type"])
        
        # 2. Check data counts
        print("\nRow Counts:")
        counts = conn.execute("""
            SELECT 
                'Seasons' as table_name, 
                COUNT(*) as count, 
                MIN(year) as earliest,
                MAX(year) as latest
            FROM seasons
            UNION ALL
            SELECT 
                'Matches',
                COUNT(*),
                MIN(season),
                MAX(season)
            FROM matches
        """).fetchall()
        print_table(counts, ["Table", "Count", "Earliest", "Latest"])
        
        # 3. Sample data from each table
        print("\nSample Seasons Data:")
        seasons = conn.execute("""
            SELECT * FROM seasons 
            ORDER BY year DESC 
            LIMIT 3
        """).fetchall()
        print_table(seasons, ["ID", "Name", "Year", "Editor"])
        
        print("\nSample Matches Data:")
        matches = conn.execute("""
            SELECT 
                season,
                home_team_name || ' vs ' || away_team_name as matchup,
                home_score || '-' || away_score as score,
                status
            FROM matches 
            ORDER BY start_timestamp DESC 
            LIMIT 3
        """).fetchall()
        print_table(matches, ["Season", "Matchup", "Score", "Status"])
        
        # 4. Check database file
        db_size = os.path.getsize('nfl_analytics.duckdb') / (1024 * 1024)  # Convert to MB
        print(f"\nDatabase file size: {db_size:.2f} MB")

def quick_db_check():
    """Quick check of database contents"""
    if not os.path.exists('nfl_analytics.duckdb'):
        print("Database file not found!")
        return
        
    with duckdb.connect('nfl_analytics.duckdb') as conn:
        # Get table counts
        seasons = conn.execute("SELECT COUNT(*) FROM seasons").fetchone()[0]
        matches = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        
        print(f"Database contains:")
        print(f"- {seasons} seasons")
        print(f"- {matches} matches")
        
        # Get file size
        size_mb = os.path.getsize('nfl_analytics.duckdb') / (1024 * 1024)
        print(f"Database size: {size_mb:.2f} MB")

def verify_and_print_results():
    """Print database contents to verify data loading"""
    with closing(duckdb.connect('nfl_analytics.duckdb')) as conn:
        try:
            # Print table names
            print("\nAvailable Tables:")
            tables = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            for table in tables:
                print(f"- {table[0]}")

            # Print row counts
            print("\nData Counts:")
            seasons_count = conn.execute("SELECT COUNT(*) FROM seasons").fetchone()[0]
            matches_count = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
            print(f"Seasons: {seasons_count}")
            print(f"Matches: {matches_count}")

            # Print sample data
            if seasons_count > 0:
                print("\nSample Seasons Data:")
                seasons = conn.execute("SELECT * FROM seasons LIMIT 3").fetchall()
                for season in seasons:
                    print(season)

            if matches_count > 0:
                print("\nSample Matches Data:")
                matches = conn.execute("""
                    SELECT season, home_team_name, away_team_name, home_score, away_score 
                    FROM matches LIMIT 3
                """).fetchall()
                for match in matches:
                    print(match)

        except Exception as e:
            print(f"Error verifying database: {str(e)}")

if __name__ == "__main__":
    # Delete existing database file to start fresh
    if os.path.exists('nfl_analytics.duckdb'):
        print("Removing existing database...")
        os.remove('nfl_analytics.duckdb')
    
    # Process the data
    process_nfl_data()
    
    # Verify and print results
    verify_and_print_results()
    
    print("\nTo connect in DBeaver:")
    print(f"1. Database location: {os.path.abspath('nfl_analytics.duckdb')}")
    print("2. Driver: DuckDB")
    print("3. Make sure the database file exists and has data")
