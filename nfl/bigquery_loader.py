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

class NFLBigQueryLoader:
    def __init__(self, project_id: str, dataset_id: str):
        try:
            # First try to create client with explicit project
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
        # Reduce batch size to help with timeouts
        self.MAX_BATCH_SIZE = 500  # Reduced from 1000
        self.RATE_LIMIT_DELAY = 2  # Increased from 1
        self.MAX_RETRIES = 5  # Added max retries
        
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
                        # Set request timeout for the load job
                        job = self.client.load_table_from_file(
                            source_file,
                            table_id,
                            job_config=job_config,
                            # Add timeout for the initial request
                            timeout=300  # 5 minutes for initial request
                        )
                        # Wait for job completion with timeout
                        job.result(timeout=600)  # 10 minutes for job completion
                    break
                except Exception as e:
                    attempt += 1
                    if attempt == retries:
                        logger.error(f"Failed to load batch after {retries} attempts: {e}")
                        raise
                    
                    # Calculate backoff time
                    wait_time = min(300, (2 ** attempt) * self.RATE_LIMIT_DELAY)  # Cap at 5 minutes
                    
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
        dataset.location = "US"  # Specify the location
        
        try:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {self.dataset_id} created successfully")
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            raise

    def create_tables(self) -> None:
        """Create all necessary tables in BigQuery"""
        # Define table schemas
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
                bigquery.SchemaField("venue", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("tournament", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'play_by_play': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                # Changed play_id to NULLABLE
                bigquery.SchemaField("play_id", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quarter", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("play_type", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("scoring_play", "BOOLEAN", mode="NULLABLE"),
                bigquery.SchemaField("home_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("away_score", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'team_stats': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("team_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("stat_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("stat_value", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("is_home_team", "BOOLEAN", mode="NULLABLE")
            ]
        }

        # Create each table
        for table_name, schema in schemas.items():
            table_id = f"{self.dataset_ref}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)
            
            try:
                # Delete existing table if it exists
                self.client.delete_table(table_id, not_found_ok=True)
                # Create new table
                table = self.client.create_table(table)
                logger.info(f"Table {table_name} created successfully")
            except Exception as e:
                logger.error(f"Error creating table {table_name}: {e}")
                raise

    def _extract_team_stats(self, match_id: int, team_data: dict, match_data: dict, is_home_team: bool) -> list:
        """Extract team statistics from match data."""
        team_stats = []
        
        # Basic team info
        team_id = team_data.get('id', 0)
        team_name = team_data.get('name', '')
        
        # Get score data - scores are at the top level of match_data
        score_data = match_data.get('homeScore' if is_home_team else 'awayScore', {})
        
        if score_data:  # Only process if we have score data
            stats_to_extract = {
                'total_score': score_data.get('current', 0),
                'q1_score': score_data.get('period1', 0),
                'q2_score': score_data.get('period2', 0),
                'q3_score': score_data.get('period3', 0),
                'q4_score': score_data.get('period4', 0)
            }
            
            # Create individual stat records
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

    def process_season(self, season_path: Path) -> None:
        """Process a single season"""
        try:
            match_list_file = season_path / 'match_list.json'
            if not match_list_file.exists():
                logger.warning(f"No match_list.json found in {season_path}, skipping...")
                return

            with open(match_list_file, 'r') as f:
                matches = json.load(f)

            season_name = season_path.name.replace('season_', '').replace('-', '/')
            
            # Process in smaller chunks
            chunk_size = 50  # Process 50 matches at a time
            for i in range(0, len(matches), chunk_size):
                chunk_matches = matches[i:i + chunk_size]
                
                # Collect data for current chunk
                all_matches = []
                all_plays = []
                
                for match in chunk_matches:
                    match_id = match['id']
                    
                    # Prepare match data
                    match_data = {
                        'match_id': match_id,
                        'season_id': match.get('season', {}).get('id'),
                        'season_name': season_name,
                        'home_team': match.get('homeTeam', {}).get('name'),
                        'away_team': match.get('awayTeam', {}).get('name'),
                        'home_score': match.get('homeScore', {}).get('current'),
                        'away_score': match.get('awayScore', {}).get('current'),
                        'match_date': datetime.fromtimestamp(match.get('startTimestamp')).isoformat() if match.get('startTimestamp') else None,
                        'status': match.get('status', {}).get('description'),
                        'tournament': match.get('tournament', {}).get('name'),
                        'raw_data': json.dumps(match)
                    }
                    all_matches.append(match_data)

                    # Collect play-by-play data
                    pbp_file = season_path / 'score_commentary' / f"{match_id}.json"
                    if pbp_file.exists():
                        try:
                            with open(pbp_file, 'r') as f:
                                pbp_data = json.load(f)
                                for incident in pbp_data.get('incidents', []):
                                    play = {
                                        'match_id': match_id,
                                        'play_id': incident.get('id') or 0,
                                        'time': incident.get('time'),
                                        'quarter': incident.get('period'),
                                        'description': incident.get('description'),
                                        'play_type': incident.get('incidentType'),
                                        'scoring_play': incident.get('isScoring', False),
                                        'home_score': incident.get('homeScore'),
                                        'away_score': incident.get('awayScore'),
                                        'raw_data': json.dumps(incident)
                                    }
                                    all_plays.append(play)
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing play-by-play file for match {match_id}: {e}")
                            continue

                # Load matches in batches
                for i in range(0, len(all_matches), self.MAX_BATCH_SIZE):
                    batch = all_matches[i:i + self.MAX_BATCH_SIZE]
                    self._load_batch_to_bigquery(f"{self.dataset_ref}.matches", batch)
                    time.sleep(self.RATE_LIMIT_DELAY)
                    logger.info(f"Loaded {len(batch)} matches from season {season_name}")

                # Load plays in batches
                for i in range(0, len(all_plays), self.MAX_BATCH_SIZE):
                    batch = all_plays[i:i + self.MAX_BATCH_SIZE]
                    self._load_batch_to_bigquery(f"{self.dataset_ref}.play_by_play", batch)
                    time.sleep(self.RATE_LIMIT_DELAY)
                    logger.info(f"Loaded {len(batch)} plays from season {season_name}")

                # Add new code to process team stats
                all_team_stats = []
                for match in chunk_matches:
                    match_id = match['id']
                    
                    # Get the quarter data which contains detailed stats
                    quarter_file = season_path / 'quarter' / f"{match_id}.json"
                    if quarter_file.exists():
                        try:
                            with open(quarter_file, 'r') as f:
                                quarter_data = json.load(f)
                                match_data = quarter_data.get('event', {})  # Get the event data
                                
                                # Extract team stats
                                home_stats = self._extract_team_stats(
                                    match_id, 
                                    match_data.get('homeTeam', {}), 
                                    match_data,  # Pass the entire match_data
                                    is_home_team=True
                                )
                                away_stats = self._extract_team_stats(
                                    match_id, 
                                    match_data.get('awayTeam', {}), 
                                    match_data,  # Pass the entire match_data
                                    is_home_team=False
                                )
                                
                                all_team_stats.extend(home_stats)
                                all_team_stats.extend(away_stats)
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing quarter file for match {match_id}: {e}")
                            continue
                
                # Load team stats in batches
                for i in range(0, len(all_team_stats), self.MAX_BATCH_SIZE):
                    batch = all_team_stats[i:i + self.MAX_BATCH_SIZE]
                    self._load_batch_to_bigquery(f"{self.dataset_ref}.team_stats", batch)
                    time.sleep(self.RATE_LIMIT_DELAY)
                    logger.info(f"Loaded {len(batch)} team stats from season {season_name}")

                logger.info(f"Successfully processed chunk of {len(chunk_matches)} matches from season {season_name}")

            logger.info(f"Successfully processed entire season {season_name}")

        except Exception as e:
            logger.error(f"Error processing season {season_path}: {e}")
            raise

def main():
    # Make sure this matches your company's Google Cloud project ID
    project_id = "helium-4-ai"  # This is your intended project ID
    dataset_id = "football_nfl"  # Changed from football(nfl) to football_nfl
    data_dir = Path("data")

    try:
        loader = NFLBigQueryLoader(project_id, dataset_id)
        
        # Create dataset and tables
        loader.create_dataset()
        loader.create_tables()

        # Process seasons sequentially to avoid rate limits
        for season_dir in sorted(data_dir.glob("season_*")):
            if season_dir.is_dir():
                logger.info(f"Processing season: {season_dir.name}")
                loader.process_season(season_dir)

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()