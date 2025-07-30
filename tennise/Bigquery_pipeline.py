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

# Set the credentials file path
current_dir = Path(__file__).parent
credentials_path = str(current_dir / "helium-4-ai-1af28ad29823.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

class TennisBigQueryLoader:
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
        self.MAX_BATCH_SIZE = 500
        self.RATE_LIMIT_DELAY = 2
        self.MAX_RETRIES = 5
        
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
                bigquery.SchemaField("tournament_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("player1_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("player2_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("player1_country", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("player2_country", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("player1_seed", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("player2_seed", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("winner_code", "INTEGER", mode="NULLABLE"),  # 1 for player1, 2 for player2
                bigquery.SchemaField("score_info", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("match_date", "DATETIME", mode="NULLABLE"),
                bigquery.SchemaField("round_info", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("surface", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE")
            ],
            'match_statistics': [
                bigquery.SchemaField("match_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("player_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("stat_category", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("stat_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("stat_value", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("is_player1", "BOOLEAN", mode="REQUIRED")
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

    def _parse_stat_value(self, value: str) -> float:
        """Parse different formats of statistic values into float numbers"""
        if not value:
            return 0.0
        
        # If it's already a number, return it
        try:
            return float(value)
        except ValueError:
            pass
            
        # Handle percentage format "47/68 (69%)"
        if '(' in value and ')' in value and '%' in value:
            # Extract the percentage value
            percentage = value[value.find('(') + 1:value.find('%')]
            try:
                return float(percentage) / 100
            except ValueError:
                pass
                
        # Handle ratio format "47/68"
        if '/' in value:
            try:
                numerator, denominator = value.split('/')
                return float(numerator) / float(denominator)
            except (ValueError, ZeroDivisionError):
                pass
                
        # If we can't parse it, log a warning and return 0
        logger.warning(f"Could not parse stat value: {value}")
        return 0.0

    def _extract_match_statistics(self, match_id: int, stats_data: dict, player_data: dict, is_player1: bool) -> list:
        """Extract match statistics from the statistics file"""
        stats = []
        
        player_id = player_data.get('id', 0)
        player_name = player_data.get('name', '')
        
        if not stats_data or 'statistics' not in stats_data:
            return stats

        for period in stats_data['statistics']:
            if period.get('period') != 'ALL':  # Only process full match statistics
                continue
                
            for group in period.get('groups', []):
                category = group.get('groupName', '')
                for item in group.get('statisticsItems', []):
                    raw_value = item.get('home' if is_player1 else 'away', 0)
                    stat = {
                        'match_id': match_id,
                        'player_id': player_id,
                        'player_name': player_name,
                        'stat_category': category,
                        'stat_name': item.get('name', ''),
                        'stat_value': self._parse_stat_value(str(raw_value)),
                        'is_player1': is_player1
                    }
                    stats.append(stat)
        
        return stats

    def process_season(self, season_path: Path, tournament_type: str) -> None:
        """Process a single season of tennis matches"""
        try:
            match_list_file = season_path / 'match_list.json'
            if not match_list_file.exists():
                logger.warning(f"No match_list.json found in {season_path}, skipping...")
                return

            with open(match_list_file, 'r') as f:
                matches = json.load(f)

            season_name = season_path.name
            
            # Process in smaller chunks
            chunk_size = 50
            for i in range(0, len(matches), chunk_size):
                chunk_matches = matches[i:i + chunk_size]
                
                all_matches = []
                all_statistics = []
                
                for match in chunk_matches:
                    match_id = match['id']
                    
                    # Extract basic match data
                    match_data = {
                        'match_id': match_id,
                        'season_id': match.get('season', {}).get('id'),
                        'season_name': match.get('season', {}).get('name'),
                        'tournament_name': match.get('tournament', {}).get('name'),
                        'player1_name': match.get('homeTeam', {}).get('name'),
                        'player2_name': match.get('awayTeam', {}).get('name'),
                        'player1_country': match.get('homeTeam', {}).get('country', {}).get('name'),
                        'player2_country': match.get('awayTeam', {}).get('country', {}).get('name'),
                        'player1_seed': match.get('homeTeamSeed'),
                        'player2_seed': match.get('awayTeamSeed'),
                        'winner_code': match.get('winnerCode'),
                        'score_info': self._format_score(match.get('homeScore', {}), match.get('awayScore', {})),
                        'match_date': datetime.fromtimestamp(match.get('startTimestamp')).isoformat() if match.get('startTimestamp') else None,
                        'round_info': match.get('roundInfo', {}).get('name'),
                        'surface': match.get('groundType'),
                        'raw_data': json.dumps(match)
                    }
                    all_matches.append(match_data)

                    # Process match statistics
                    stats_file = season_path / 'match_statistics' / f"{match_id}.json.bak"
                    if stats_file.exists():
                        try:
                            with open(stats_file, 'r') as f:
                                stats_data = json.load(f)
                                
                                # Extract statistics for both players
                                player1_stats = self._extract_match_statistics(
                                    match_id,
                                    stats_data,
                                    match.get('homeTeam', {}),
                                    is_player1=True
                                )
                                player2_stats = self._extract_match_statistics(
                                    match_id,
                                    stats_data,
                                    match.get('awayTeam', {}),
                                    is_player1=False
                                )
                                
                                all_statistics.extend(player1_stats)
                                all_statistics.extend(player2_stats)
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing statistics file for match {match_id}: {e}")
                            continue

                # Load matches in batches
                for i in range(0, len(all_matches), self.MAX_BATCH_SIZE):
                    batch = all_matches[i:i + self.MAX_BATCH_SIZE]
                    self._load_batch_to_bigquery(f"{self.dataset_ref}.matches", batch)
                    time.sleep(self.RATE_LIMIT_DELAY)
                    logger.info(f"Loaded {len(batch)} matches from {tournament_type} season {season_name}")

                # Load statistics in batches
                for i in range(0, len(all_statistics), self.MAX_BATCH_SIZE):
                    batch = all_statistics[i:i + self.MAX_BATCH_SIZE]
                    self._load_batch_to_bigquery(f"{self.dataset_ref}.match_statistics", batch)
                    time.sleep(self.RATE_LIMIT_DELAY)
                    logger.info(f"Loaded {len(batch)} statistics from {tournament_type} season {season_name}")

                logger.info(f"Successfully processed chunk of {len(chunk_matches)} matches from {tournament_type} season {season_name}")

            logger.info(f"Successfully processed entire {tournament_type} season {season_name}")

        except Exception as e:
            logger.error(f"Error processing season {season_path}: {e}")
            raise

    def _format_score(self, home_score: Dict, away_score: Dict) -> str:
        """Format the tennis score in a readable string"""
        if not home_score or not away_score:
            return ""
            
        sets = []
        for i in range(1, 6):  # Tennis matches can have up to 5 sets
            period_key = f"period{i}"
            if period_key in home_score and period_key in away_score:
                home_set = home_score[period_key]
                away_set = away_score[period_key]
                
                # Check for tiebreak
                tiebreak_key = f"period{i}TieBreak"
                if tiebreak_key in home_score and tiebreak_key in away_score:
                    sets.append(f"{home_set}-{away_set}({home_score[tiebreak_key]}-{away_score[tiebreak_key]})")
                else:
                    sets.append(f"{home_set}-{away_set}")
            else:
                break
                
        return ", ".join(sets)

def main():
    project_id = "helium-4-ai"
    dataset_id = "tennis_roland_garros"
    data_dir = Path("data")

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Set the environment variable to point to the service account key file
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/helium-4-ai-1af28ad29823.json"

        loader = TennisBigQueryLoader(project_id, dataset_id)
        
        # Create dataset and tables
        loader.create_dataset()
        loader.create_tables()

        # Process ATP matches
        atp_dir = data_dir / "Roland Garros(ATP)"
        for season_dir in sorted(atp_dir.glob("season_*")):
            if season_dir.is_dir():
                logger.info(f"Processing ATP season: {season_dir.name}")
                loader.process_season(season_dir, "ATP")

        # Process WTA matches
        wta_dir = data_dir / "Roland Garros(WTA)"
        for season_dir in sorted(wta_dir.glob("season_*")):
            if season_dir.is_dir():
                logger.info(f"Processing WTA season: {season_dir.name}")
                loader.process_season(season_dir, "WTA")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()
