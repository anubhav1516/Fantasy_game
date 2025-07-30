from google.cloud import bigquery
import json
from pathlib import Path
import logging
from typing import List, Dict, Any
from datetime import datetime
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
import queue

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BasketballBigQueryLoader:
    def __init__(self, project_id: str, dataset_id: str, league: str, credentials_path: str = None):
        try:
            if credentials_path:
                # Use explicit credentials if provided
                self.client = bigquery.Client.from_service_account_json(
                    credentials_path,
                    project=project_id
                )
            else:
                # Try to create client with explicit project
                self.client = bigquery.Client(project=project_id)
        except Exception as e:
            logger.warning(f"Failed to create BigQuery client with explicit project: {e}")
            try:
                # Fallback to default credentials
                self.client = bigquery.Client()
            except Exception as e:
                logger.error("Failed to create BigQuery client. Please ensure you have set up authentication correctly.")
                logger.error("You can either:")
                logger.error("1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your service account key file")
                logger.error("2. Run 'gcloud auth application-default login'")
                raise

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = f"{project_id}.{dataset_id}"
        self.batch_queue = queue.Queue()
        self.MAX_BATCH_SIZE = 500
        self.RATE_LIMIT_DELAY = 2
        self.MAX_RETRIES = 5
        self.league = league.lower()
        self.stats_mapping = {
            'points': ['points', 'totalPoints', 'score'],
            'rebounds': ['rebounds', 'totalRebounds', 'rebs'],
            'assists': ['assists', 'totalAssists', 'asts'],
            'steals': ['steals', 'totalSteals', 'stls'],
            'blocks': ['blocks', 'totalBlocks', 'blks'],
            'turnovers': ['turnovers', 'totalTurnovers', 'tos'],
            'fouls': ['fouls', 'personalFouls', 'pf']
        }

    def _load_batch_to_bigquery(self, table_id: str, rows: List[Dict], retries: int = 5) -> None:
        """Load a batch of data with retry logic"""
        if not rows:
            return

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            for row in rows:
                json.dump(row, temp_file)
                temp_file.write('\n')
            temp_file_path = temp_file.name

        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                ignore_unknown_values=True
            )

            attempt = 0
            while attempt < retries:
                try:
                    with open(temp_file_path, "rb") as source_file:
                        job = self.client.load_table_from_file(
                            source_file,
                            table_id,
                            job_config=job_config,
                            timeout=300
                        )
                        job.result(timeout=600)
                    break
                except Exception as e:
                    attempt += 1
                    if attempt == retries:
                        logger.error(f"Failed to load batch after {retries} attempts: {e}")
                        raise
                    
                    wait_time = min(300, (2 ** attempt) * self.RATE_LIMIT_DELAY)
                    
                    if "429" in str(e) or "rate limit" in str(e).lower():
                        logger.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                    elif "timeout" in str(e).lower():
                        logger.warning(f"Timeout occurred, waiting {wait_time} seconds before retry...")
                    else:
                        logger.warning(f"Error occurred: {e}, waiting {wait_time} seconds before retry...")
                    
                    time.sleep(wait_time)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def create_dataset(self) -> None:
        """Create the BigQuery dataset if it doesn't exist"""
        dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
        dataset.location = "US"
        
        try:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {self.dataset_id} created successfully")
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            raise

    def create_tables(self) -> None:
        """Create all necessary tables in BigQuery"""
        schemas = {
            'matches': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("season_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("season_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("home_team", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("away_team", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("home_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("away_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("match_date", "DATETIME", mode="NULLABLE"),
                bigquery.SchemaField("tournament", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'match_statistics': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("is_home_team", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("total_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("period1_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("period2_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("period3_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("period4_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("overtime_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'player_stats': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("player_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("is_home_team", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("position", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("minutes_played", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("points", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("field_goals_made", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("field_goals_attempted", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("three_points_made", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("three_points_attempted", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("free_throws_made", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("free_throws_attempted", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("rebounds", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("offensive_rebounds", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("defensive_rebounds", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("assists", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("steals", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("blocks", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("turnovers", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("fouls", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("plus_minus", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'team_stats': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("is_home_team", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("points", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("rebounds", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("assists", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("steals", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("blocks", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'team_streak': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("is_home_team", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("wins", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("losses", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'play_by_play': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("play_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quarter", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("play_type", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("scoring_play", "BOOLEAN", mode="NULLABLE"),
                bigquery.SchemaField("home_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("away_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("commentary_type", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ]
        }

        for table_name, schema in schemas.items():
            table_id = f"{self.dataset_ref}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)
            
            try:
                self.client.delete_table(table_id, not_found_ok=True)
                table = self.client.create_table(table)
                logger.info(f"Table {table_name} created successfully")
            except Exception as e:
                logger.error(f"Error creating table {table_name}: {e}")
                raise

    def _extract_team_stats(self, match_id: int, team_data: dict, match_data: dict, is_home_team: bool) -> list:
        """Extract team statistics from match data."""
        team_stats = []
        
        team_id = team_data.get('id', 0)
        team_name = team_data.get('name', '')
        
        stats_to_extract = {
            'total_score': match_data.get('homeScore' if is_home_team else 'awayScore', {}).get('current', 0),
            'field_goals_made': team_data.get('fieldGoalsMade', 0),
            'field_goals_attempted': team_data.get('fieldGoalsAttempted', 0),
            'three_pointers_made': team_data.get('threePointersMade', 0),
            'three_pointers_attempted': team_data.get('threePointersAttempted', 0),
            'free_throws_made': team_data.get('freeThrowsMade', 0),
            'free_throws_attempted': team_data.get('freeThrowsAttempted', 0),
            'rebounds': team_data.get('rebounds', 0),
            'assists': team_data.get('assists', 0),
            'steals': team_data.get('steals', 0),
            'blocks': team_data.get('blocks', 0),
            'turnovers': team_data.get('turnovers', 0),
            'fouls': team_data.get('fouls', 0)
        }
        
        for stat_name, stat_value in stats_to_extract.items():
            stat = {
                'match_id': match_id,
                'team_id': team_id,
                'team_name': team_name,
                'stat_name': stat_name,
                'stat_value': float(stat_value) if stat_value is not None else 0.0,
                'is_home_team': is_home_team
            }
            team_stats.append(stat)
        
        return team_stats

    def _safe_get_nested(self, data: dict, *keys, default=None):
        """Safely get nested dictionary values."""
        current = data
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key, default)
            if current is None:
                return default
        return current

    def _extract_match_data(self, match: dict) -> dict:
        """Extract match data from match list."""
        return {
            'match_id': match['id'],
            'season_id': match.get('season', {}).get('id'),
            'season_name': match.get('season', {}).get('name', ''),
            'home_team': match.get('homeTeam', {}).get('name', ''),
            'away_team': match.get('awayTeam', {}).get('name', ''),
            'home_score': match.get('homeScore', {}).get('current'),
            'away_score': match.get('awayScore', {}).get('current'),
            'match_date': datetime.fromtimestamp(match.get('startTimestamp', 0)).isoformat() if match.get('startTimestamp') else None,
            'tournament': match.get('tournament', {}).get('name', ''),
            'raw_data': json.dumps(match)
        }

    def _extract_match_statistics(self, match_id: int, data: dict) -> list:
        """Extract match statistics."""
        stats = []
        event_data = data.get('event', {})
        
        # Process home team stats
        home_team = event_data.get('homeTeam', {})
        home_score = event_data.get('homeScore', {})
        if home_team and home_score:
            stats.append({
                'match_id': match_id,
                'team_id': home_team.get('id', 0),
                'team_name': home_team.get('name', ''),
                'is_home_team': True,
                'total_score': home_score.get('current', 0),
                'period1_score': home_score.get('period1', 0),
                'period2_score': home_score.get('period2', 0),
                'period3_score': home_score.get('period3', 0),
                'period4_score': home_score.get('period4', 0),
                'overtime_score': home_score.get('overtime', 0),
                'raw_data': json.dumps(home_score)
            })

        # Process away team stats
        away_team = event_data.get('awayTeam', {})
        away_score = event_data.get('awayScore', {})
        if away_team and away_score:
            stats.append({
                'match_id': match_id,
                'team_id': away_team.get('id', 0),
                'team_name': away_team.get('name', ''),
                'is_home_team': False,
                'total_score': away_score.get('current', 0),
                'period1_score': away_score.get('period1', 0),
                'period2_score': away_score.get('period2', 0),
                'period3_score': away_score.get('period3', 0),
                'period4_score': away_score.get('period4', 0),
                'overtime_score': away_score.get('overtime', 0),
                'raw_data': json.dumps(away_score)
            })
        
        return stats

    def _extract_player_stats(self, match_id: int, player_data: dict, team_data: dict, is_home_team: bool) -> dict:
        """Extract player statistics."""
        try:
            player = player_data.get('player', {})
            stats = player_data.get('statistics', {})
            seconds_played = stats.get('secondsPlayed', 0)
            
            return {
                'match_id': match_id,
                'team_id': team_data.get('id', 0),  # Use passed team_data instead
                'team_name': team_data.get('name', ''),  # Use passed team_data instead
                'player_id': player.get('id', 0),
                'player_name': player.get('name', ''),
                'is_home_team': is_home_team,
                'position': player_data.get('position'),
                'minutes_played': round(seconds_played / 60.0, 2) if seconds_played else 0,
                'points': stats.get('points', 0),
                'field_goals_made': stats.get('fieldGoalsMade', 0),
                'field_goals_attempted': stats.get('fieldGoalAttempts', 0),
                'three_points_made': stats.get('threePointsMade', 0),
                'three_points_attempted': stats.get('threePointAttempts', 0),
                'free_throws_made': stats.get('freeThrowsMade', 0),
                'free_throws_attempted': stats.get('freeThrowAttempts', 0),
                'rebounds': stats.get('rebounds', 0),
                'offensive_rebounds': stats.get('offensiveRebounds', 0),
                'defensive_rebounds': stats.get('defensiveRebounds', 0),
                'assists': stats.get('assists', 0),
                'steals': stats.get('steals', 0),
                'blocks': stats.get('blocks', 0),
                'turnovers': stats.get('turnovers', 0),
                'fouls': stats.get('personalFouls', 0),
                'plus_minus': stats.get('plusMinus', 0),
                'raw_data': json.dumps(player_data)
            }
        except Exception as e:
            logger.error(f"Error extracting player stats: {e}")
            return None

    def _extract_team_streak(self, match_id: int, data: dict) -> list:
        """Extract team streak information."""
        streaks = []
        event_data = data.get('event', {})
        
        # Process home team streak
        home_team = event_data.get('homeTeam', {})
        home_form = event_data.get('homeTeamSeasonHistoricalForm', {})
        if home_team:
            streaks.append({
                'match_id': match_id,
                'team_id': home_team.get('id', 0),
                'team_name': home_team.get('name', ''),
                'is_home_team': True,
                'wins': home_form.get('wins', 0),
                'losses': home_form.get('losses', 0),
                'raw_data': json.dumps(home_form)
            })
        
        # Process away team streak
        away_team = event_data.get('awayTeam', {})
        away_form = event_data.get('awayTeamSeasonHistoricalForm', {})
        if away_team:
            streaks.append({
                'match_id': match_id,
                'team_id': away_team.get('id', 0),
                'team_name': away_team.get('name', ''),
                'is_home_team': False,
                'wins': away_form.get('wins', 0),
                'losses': away_form.get('losses', 0),
                'raw_data': json.dumps(away_form)
            })
        
        return streaks

    def _parse_period(self, period_value) -> int:
        """Parse period value to integer, handling different formats."""
        if period_value is None:
            return None
        
        if isinstance(period_value, int):
            return period_value
            
        if isinstance(period_value, str):
            digits = ''.join(filter(str.isdigit, period_value))
            return int(digits) if digits else None
        
        return None

    def _process_team_streak(self, match_id: int, data: dict, data_type: str, *args) -> list:
        """Process team streak data."""
        streaks = []
        for streak in data.get('streaks', []):
            try:
                streak_data = self._extract_team_streak(match_id, streak)
                if streak_data:
                    streaks.append(streak_data)
            except Exception as e:
                logger.warning(f"Error processing streak in {data_type}: {e}")
                continue
        return streaks

    def _process_quarter_data(self, match_id: int, data: dict, data_type: str, *args) -> list:
        """Process quarter data for team statistics."""
        try:
            match_data = data.get('event', {})
            
            # Extract home team stats
            home_stats = self._extract_team_stats(
                match_id, 
                match_data.get('homeTeam', {}), 
                match_data,
                is_home_team=True
            )
            
            # Extract away team stats
            away_stats = self._extract_team_stats(
                match_id, 
                match_data.get('awayTeam', {}), 
                match_data,
                is_home_team=False
            )
            
            return home_stats + away_stats
        except Exception as e:
            logger.warning(f"Error processing quarter data: {e}")
            return []

    def _process_match_statistics(self, match_id: int, data: dict, data_type: str, *args) -> list:
        """Process match statistics data."""
        stats = []
        try:
            # Process home team stats
            home_stats = data.get('homeTeam', {})
            if home_stats:
                stats.append({
                    'match_id': match_id,
                    'team_id': home_stats.get('id', 0),
                    'team_name': home_stats.get('name', ''),
                    'is_home_team': True,
                    'possession': home_stats.get('possession'),
                    'shots_on_target': home_stats.get('shotsOnTarget'),
                    'shots_off_target': home_stats.get('shotsOffTarget'),
                    'total_shots': home_stats.get('totalShots'),
                    'blocked_shots': home_stats.get('blockedShots'),
                    'free_throws': home_stats.get('freeThrows'),
                    'free_throws_attempted': home_stats.get('freeThrowsAttempted'),
                    'rebounds': home_stats.get('rebounds'),
                    'offensive_rebounds': home_stats.get('offensiveRebounds'),
                    'defensive_rebounds': home_stats.get('defensiveRebounds'),
                    'assists': home_stats.get('assists'),
                    'turnovers': home_stats.get('turnovers'),
                    'steals': home_stats.get('steals'),
                    'blocks': home_stats.get('blocks'),
                    'personal_fouls': home_stats.get('personalFouls'),
                    'raw_data': json.dumps(home_stats)
                })

            # Process away team stats
            away_stats = data.get('awayTeam', {})
            if away_stats:
                stats.append({
                    'match_id': match_id,
                    'team_id': away_stats.get('id', 0),
                    'team_name': away_stats.get('name', ''),
                    'is_home_team': False,
                    'possession': away_stats.get('possession'),
                    'shots_on_target': away_stats.get('shotsOnTarget'),
                    'shots_off_target': away_stats.get('shotsOffTarget'),
                    'total_shots': away_stats.get('totalShots'),
                    'blocked_shots': away_stats.get('blockedShots'),
                    'free_throws': away_stats.get('freeThrows'),
                    'free_throws_attempted': away_stats.get('freeThrowsAttempted'),
                    'rebounds': away_stats.get('rebounds'),
                    'offensive_rebounds': away_stats.get('offensiveRebounds'),
                    'defensive_rebounds': away_stats.get('defensiveRebounds'),
                    'assists': away_stats.get('assists'),
                    'turnovers': away_stats.get('turnovers'),
                    'steals': away_stats.get('steals'),
                    'blocks': away_stats.get('blocks'),
                    'personal_fouls': away_stats.get('personalFouls'),
                    'raw_data': json.dumps(away_stats)
                })
        except Exception as e:
            logger.error(f"Error processing match statistics: {e}")
        return stats

    def process_season(self, season_path: Path) -> None:
        """Process a single season with improved error handling and data validation."""
        try:
            match_list_file = season_path / 'match_list.json'
            if not match_list_file.exists():
                logger.warning(f"No match_list.json found in {season_path}, skipping...")
                return

            with open(match_list_file, 'r') as f:
                matches = json.load(f)
                if not isinstance(matches, list):
                    logger.error(f"Invalid match list format in {season_path}, expected list but got {type(matches)}")
                    return

            # Extract season name from directory name
            season_name = season_path.name
            if '_WNBA_' in season_name:
                # For WNBA format: season_2096_WNBA_2009 -> WNBA 2009
                season_year = season_name.split('_')[-1]
                season_name = f"WNBA {season_year}"
            else:
                # For NBA format: season_NBA 09-10 -> NBA 09-10
                season_name = season_name.replace('season_', '')
            
            # Initialize data lists for the entire season
            all_matches = []
            all_match_stats = []
            all_player_stats = []
            all_team_stats = []
            all_plays = []
            all_team_streaks = []
            
            # Process each match
            for match in matches:
                if not isinstance(match, dict):
                    logger.warning(f"Invalid match data format in {season_path}, skipping match")
                    continue

                match_id = match.get('id')
                if not match_id:
                    logger.warning(f"Match missing ID in {season_path}, skipping match")
                    continue

                # Get season info
                season_info = match.get('season', {})
                if isinstance(season_info, dict):
                    season_id = season_info.get('id')
                else:
                    season_id = None
                
                # Process match data
                match_data = {
                    'match_id': match_id,
                    'season_id': season_id,
                    'season_name': season_name,
                    'home_team': match.get('homeTeam', {}).get('name') if isinstance(match.get('homeTeam'), dict) else None,
                    'away_team': match.get('awayTeam', {}).get('name') if isinstance(match.get('awayTeam'), dict) else None,
                    'home_score': match.get('homeScore', {}).get('current') if isinstance(match.get('homeScore'), dict) else None,
                    'away_score': match.get('awayScore', {}).get('current') if isinstance(match.get('awayScore'), dict) else None,
                    'match_date': datetime.fromtimestamp(match.get('startTimestamp', 0)).isoformat() if match.get('startTimestamp') else None,
                    'tournament': match.get('tournament', {}).get('name') if isinstance(match.get('tournament'), dict) else None,
                    'raw_data': json.dumps(match)
                }
                all_matches.append(match_data)

                # Process match statistics
                home_team = match.get('homeTeam', {})
                away_team = match.get('awayTeam', {})
                home_score = match.get('homeScore', {})
                away_score = match.get('awayScore', {})
                
                if isinstance(home_team, dict) and isinstance(home_score, dict):
                    all_match_stats.append({
                        'match_id': match_id,
                        'team_id': home_team.get('id', 0),
                        'team_name': home_team.get('name', ''),
                        'is_home_team': True,
                        'total_score': home_score.get('current', 0),
                        'period1_score': home_score.get('period1', 0),
                        'period2_score': home_score.get('period2', 0),
                        'period3_score': home_score.get('period3', 0),
                        'period4_score': home_score.get('period4', 0),
                        'overtime_score': home_score.get('overtime', 0),
                        'raw_data': json.dumps(home_score)
                    })
                
                if isinstance(away_team, dict) and isinstance(away_score, dict):
                    all_match_stats.append({
                        'match_id': match_id,
                        'team_id': away_team.get('id', 0),
                        'team_name': away_team.get('name', ''),
                        'is_home_team': False,
                        'total_score': away_score.get('current', 0),
                        'period1_score': away_score.get('period1', 0),
                        'period2_score': away_score.get('period2', 0),
                        'period3_score': away_score.get('period3', 0),
                        'period4_score': away_score.get('period4', 0),
                        'overtime_score': away_score.get('overtime', 0),
                        'raw_data': json.dumps(away_score)
                    })

                # Process player statistics
                player_stats_file = season_path / 'player_statistics' / f"{match_id}.json"
                if player_stats_file.exists():
                    try:
                        with open(player_stats_file, 'r') as f:
                            player_stats_data = json.load(f)
                            
                            # Process home team players
                            home_team_stats = player_stats_data.get('home', {})
                            if isinstance(home_team_stats, dict):
                                for player in home_team_stats.get('players', []):
                                    if isinstance(player, dict):
                                        player_stat = self._extract_player_stats(match_id, player, home_team_stats, True)
                                        if player_stat:
                                            all_player_stats.append(player_stat)
                            
                            # Process away team players
                            away_team_stats = player_stats_data.get('away', {})
                            if isinstance(away_team_stats, dict):
                                for player in away_team_stats.get('players', []):
                                    if isinstance(player, dict):
                                        player_stat = self._extract_player_stats(match_id, player, away_team_stats, False)
                                        if player_stat:
                                            all_player_stats.append(player_stat)
                    except Exception as e:
                        logger.error(f"Error processing player statistics for match {match_id}: {e}")

                # Process team stats from quarter data
                quarter_file = season_path / 'quarter' / f"{match_id}.json"
                if quarter_file.exists():
                    try:
                        with open(quarter_file, 'r') as f:
                            quarter_data = json.load(f)
                            event_data = quarter_data.get('event', {})
                            
                            if isinstance(event_data, dict):
                                home_team_stats = event_data.get('homeTeam', {})
                                away_team_stats = event_data.get('awayTeam', {})
                                
                                if isinstance(home_team_stats, dict):
                                    all_team_stats.append({
                                        'match_id': match_id,
                                        'team_id': home_team.get('id', 0),
                                        'team_name': home_team.get('name', ''),
                                        'is_home_team': True,
                                        'points': home_score.get('current', 0),
                                        'rebounds': home_team_stats.get('rebounds', 0),
                                        'assists': home_team_stats.get('assists', 0),
                                        'steals': home_team_stats.get('steals', 0),
                                        'blocks': home_team_stats.get('blocks', 0),
                                        'raw_data': json.dumps(home_team_stats)
                                    })
                                
                                if isinstance(away_team_stats, dict):
                                    all_team_stats.append({
                                        'match_id': match_id,
                                        'team_id': away_team.get('id', 0),
                                        'team_name': away_team.get('name', ''),
                                        'is_home_team': False,
                                        'points': away_score.get('current', 0),
                                        'rebounds': away_team_stats.get('rebounds', 0),
                                        'assists': away_team_stats.get('assists', 0),
                                        'steals': away_team_stats.get('steals', 0),
                                        'blocks': away_team_stats.get('blocks', 0),
                                        'raw_data': json.dumps(away_team_stats)
                                    })
                    except Exception as e:
                        logger.error(f"Error processing quarter data for match {match_id}: {e}")

                # Process team streaks
                home_form = match.get('homeTeamSeasonHistoricalForm', {})
                away_form = match.get('awayTeamSeasonHistoricalForm', {})
                
                if isinstance(home_form, dict):
                    all_team_streaks.append({
                        'match_id': match_id,
                        'team_id': home_team.get('id', 0),
                        'team_name': home_team.get('name', ''),
                        'is_home_team': True,
                        'wins': home_form.get('wins', 0),
                        'losses': home_form.get('losses', 0),
                        'raw_data': json.dumps(home_form)
                    })
                
                if isinstance(away_form, dict):
                    all_team_streaks.append({
                        'match_id': match_id,
                        'team_id': away_team.get('id', 0),
                        'team_name': away_team.get('name', ''),
                        'is_home_team': False,
                        'wins': away_form.get('wins', 0),
                        'losses': away_form.get('losses', 0),
                        'raw_data': json.dumps(away_form)
                    })

                # Process play-by-play data
                for commentary_type in ['score_commentary', 'text_commentary']:
                    commentary_file = season_path / commentary_type / f"{match_id}.json"
                    if commentary_file.exists():
                        try:
                            with open(commentary_file, 'r') as f:
                                commentary_data = json.load(f)
                                for incident in commentary_data.get('incidents', []):
                                    if isinstance(incident, dict):
                                        all_plays.append({
                                            'match_id': match_id,
                                            'play_id': incident.get('id', 0),
                                            'time': incident.get('time'),
                                            'quarter': self._parse_period(incident.get('period')),
                                            'description': incident.get('text', incident.get('description', '')),
                                            'play_type': incident.get('incidentType'),
                                            'scoring_play': incident.get('isScoring', False),
                                            'home_score': incident.get('homeScore'),
                                            'away_score': incident.get('awayScore'),
                                            'commentary_type': commentary_type,
                                            'raw_data': json.dumps(incident)
                                        })
                        except Exception as e:
                            logger.error(f"Error processing {commentary_type} for match {match_id}: {e}")

            # Load data in batches
            data_tables = [
                (all_matches, 'matches'),
                (all_match_stats, 'match_statistics'),
                (all_player_stats, 'player_stats'),
                (all_team_stats, 'team_stats'),
                (all_plays, 'play_by_play'),
                (all_team_streaks, 'team_streak')
            ]

            for data_list, table_name in data_tables:
                if data_list:  # Only process if we have data
                    try:
                        for i in range(0, len(data_list), self.MAX_BATCH_SIZE):
                            batch = data_list[i:i + self.MAX_BATCH_SIZE]
                            if batch:  # Double check we have data to load
                                self._load_batch_to_bigquery(f"{self.dataset_ref}.{table_name}", batch)
                                time.sleep(self.RATE_LIMIT_DELAY)
                                logger.info(f"Loaded {len(batch)} {table_name} records from season {season_name}")
                    except Exception as e:
                        logger.error(f"Error loading {table_name} data: {e}")
                        continue

            logger.info(f"Successfully processed entire season {season_name}")

        except Exception as e:
            logger.error(f"Error processing season {season_path}: {e}")
            raise

    def _process_commentary(self, match_id: int, data: dict, commentary_type: str) -> list:
        """Process commentary data with type tracking."""
        plays = []
        for incident in data.get('incidents', []):
            try:
                play = {
                    'match_id': match_id,
                    'play_id': incident.get('id') or 0,
                    'time': incident.get('time'),
                    'quarter': self._parse_period(incident.get('period')),
                    'description': incident.get('description', incident.get('text', '')),
                    'play_type': incident.get('incidentType'),
                    'scoring_play': incident.get('isScoring', False),
                    'home_score': incident.get('homeScore'),
                    'away_score': incident.get('awayScore'),
                    'commentary_type': commentary_type,
                    'raw_data': json.dumps(incident)
                }
                plays.append(play)
            except Exception as e:
                logger.warning(f"Error processing incident in {commentary_type}: {e}")
                continue
        return plays

    def _process_player_stats(self, match_id: int, data: dict) -> list:
        """Process player statistics data."""
        player_stats = []
        for player in data.get('players', []):
            player_stat = self._extract_player_stats(match_id, player)
            player_stats.append(player_stat)
        return player_stats

    def process_league_data(self, league_dir: Path) -> None:
        """Process all data for a specific league"""
        try:
            if not league_dir.exists():
                logger.error(f"League directory {league_dir} does not exist")
                return

            # For WNBA, look for directories containing WNBA in the name
            season_dirs = [
                d for d in league_dir.iterdir() 
                if d.is_dir() and 'WNBA' in d.name
            ]
            
            if not season_dirs:
                logger.warning(f"No season directories found in {league_dir}")
                return

            # Sort by the year at the end of the directory name
            season_dirs = sorted(
                season_dirs,
                key=lambda x: x.name.split('_')[-1]  # Get the year at the end
            )

            for season_dir in season_dirs:
                logger.info(f"Processing {self.league.upper()} season: {season_dir.name}")
                self.process_season(season_dir)

        except Exception as e:
            logger.error(f"Error processing league data: {e}")
            raise

def main():
    project_id = "helium-4-ai"
    dataset_id = "basketball"
    credentials_path = "helium-4-ai-1af28ad29823.json"
    
    # Only process WNBA since NBA is already done
    league = 'wnba'
    
    try:
        logger.info(f"Starting to process {league.upper()} data")
        
        loader = BasketballBigQueryLoader(
            project_id=project_id,
            dataset_id=dataset_id,
            league=league,
            credentials_path=credentials_path
        )

        league_dir = Path("data") / league
        loader.process_league_data(league_dir)
        
        logger.info(f"Finished processing {league.upper()} data")

    except Exception as e:
        logger.error(f"Error processing {league.upper()} data: {e}")

if __name__ == "__main__":
    main()