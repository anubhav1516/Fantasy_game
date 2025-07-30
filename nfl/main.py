from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from fake_useragent import UserAgent
import requests
import json
import time
import random
import re
from datetime import datetime, UTC
from pathlib import Path
import cloudscraper
import logging
from typing import Optional, Dict, List, Tuple, Any
import urllib3
import certifi
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import threading
from queue import Queue
import diskcache
import backoff

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants for batch processing
BATCH_SIZE = 20  # Increased from 10 to 20
MAX_WORKERS = 8  # Increased from 4 to 8
CACHE_DIR = Path('cache')

def create_season_directories(season_path: Path):
    """Create all required subdirectories for a season"""
    directories = [
        'team-streak',
        'quarter',
        'score_commentary'
    ]
    for directory in directories:
        (season_path / directory).mkdir(parents=True, exist_ok=True)

def format_season_folder_name(season_name: str) -> str:
    """Convert season name to folder format (e.g., 'NFL 24/25' to 'season_24-25')"""
    # Extract years from season name (e.g., '24/25' from 'NFL 24/25')
    year_part = season_name.split()[-1]
    years = year_part.replace('/', '-')
    return f"season_{years}"

def random_exponential():
    """Returns random value between 1 and 2"""
    return 1 + random.random()

def should_retry_request(e):
    if isinstance(e, requests.exceptions.HTTPError):
        # Only retry on 403 (rate limit) and 5xx (server errors)
        return e.response.status_code == 403 or 500 <= e.response.status_code < 600
    return isinstance(e, (requests.exceptions.RequestException, PlaywrightTimeout))

class BasketballScraper:
    def __init__(self):
        self.base_url = "https://www.sofascore.com"
        self.api_base_url = "https://api.sofascore.com/api/v1"
        self.session = requests.Session()
        self.session.verify = certifi.where()
        self.cache = diskcache.Cache(str(CACHE_DIR))
        self._lock = threading.Lock()
        self._cookie_refresh_lock = threading.Lock()
        self._browsers = {}
        self._browsers_lock = threading.Lock()
        
    def __enter__(self):
        self.setup_thread_browser()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup_thread_browser()
        self.cache.close()

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=2),
        exception=(PlaywrightTimeout, Exception),  # Handle network errors
        max_tries=5,
        jitter=backoff.full_jitter
    )
    def setup_thread_browser(self):
        """Initialize Playwright browser and context for current thread"""
        thread_id = threading.get_ident()
        with self._browsers_lock:
            if thread_id not in self._browsers:
                try:
                    playwright = sync_playwright().start()
                    browser = playwright.chromium.launch(
                        headless=True,
                        args=[
                            '--disable-blink-features=AutomationControlled',
                            '--disable-notifications',
                            '--disable-gpu',
                            '--disable-dev-shm-usage',
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                            '--disable-extensions',
                            '--disable-images',
                            '--ignore-certificate-errors',  # Added for connection issues
                            '--disable-web-security'  # Added for connection issues
                        ]
                    )
                    context = browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent=self.get_random_user_agent(),
                        java_script_enabled=True,
                        bypass_csp=True,
                        ignore_https_errors=True
                    )
                    page = context.new_page()
                    
                    # More resilient page load
                    try:
                        page.goto(self.base_url, wait_until='domcontentloaded', timeout=60000)
                        page.wait_for_load_state('domcontentloaded', timeout=60000)
                    except Exception as e:
                        logger.warning(f"Initial page load failed, retrying: {e}")
                        # Retry with backup URL
                        page.goto("https://www.sofascore.com/basketball", wait_until='domcontentloaded', timeout=60000)
                    
                    time.sleep(2)
                    
                    cookies = context.cookies()
                    cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies if 'name' in cookie and 'value' in cookie}
                    
                    self._browsers[thread_id] = {
                        'playwright': playwright,
                        'browser': browser,
                        'context': context,
                        'page': page,
                        'cookies': cookie_dict
                    }
                    
                    logger.info(f"✅ Browser setup completed for thread {thread_id}")
                except Exception as e:
                    logger.error(f"❌ Failed to setup browser for thread {thread_id}: {e}")
                    self.cleanup_thread_browser()
                    raise

    def cleanup_thread_browser(self):
        """Clean up Playwright resources for current thread"""
        thread_id = threading.get_ident()
        with self._browsers_lock:
            if thread_id in self._browsers:
                try:
                    browser_data = self._browsers[thread_id]
                    if browser_data['page']:
                        browser_data['page'].close()
                    if browser_data['context']:
                        browser_data['context'].close()
                    if browser_data['browser']:
                        browser_data['browser'].close()
                    if browser_data['playwright']:
                        browser_data['playwright'].stop()
                    del self._browsers[thread_id]
                except Exception as e:
                    logger.error(f"Error during cleanup for thread {thread_id}: {e}")

    @property
    def page(self):
        """Get the page instance for current thread"""
        thread_id = threading.get_ident()
        with self._browsers_lock:
            if thread_id not in self._browsers:
                self.setup_thread_browser()
            return self._browsers[thread_id]['page']

    @staticmethod
    def get_random_user_agent() -> str:
        """Return a random modern browser user agent"""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        return random.choice(user_agents)

    def wait_for_page_load(self, timeout: int = 15000):  # Reduced timeout
        """Wait for page to load and perform natural scrolling"""
        try:
            if self.page:
                self.page.wait_for_load_state('domcontentloaded', timeout=timeout)  # Changed from networkidle
                # Simplified scrolling
                self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(0.5)  # Reduced delay
        except PlaywrightTimeout:
            logger.warning("Page load timeout - continuing anyway")

    def extract_match_details(self, url: str) -> Optional[Dict]:
        """Extract match details from a given URL"""
        try:
            logger.info(f"🏀 Fetching match details from: {url}")
            if not self.page:
                logger.error("Browser page not initialized")
                return None
            response = self.page.goto(url)
            self.wait_for_page_load()

            # Get the final URL after any redirects
            final_url = self.page.url
            
            # Try to extract match ID from the page content
            try:
                # Look for match ID in the page's data attributes or scripts
                match_id_script = self.page.evaluate("""
                    () => {
                        // Try to find match ID in various possible locations
                        const eventElement = document.querySelector('[data-event-id]');
                        if (eventElement) return eventElement.getAttribute('data-event-id');
                        
                        // Look for it in script tags
                        const scripts = Array.from(document.getElementsByTagName('script'));
                        for (const script of scripts) {
                            const match = script.textContent.match(/eventId["\']?\\s*:\\s*(\\d+)/);
                            if (match) return match[1];
                        }
                        return null;
                    }
                """)
                
                if not match_id_script:
                    # If we couldn't find it in the page, try the URL
                    match_id_url = re.search(r'/event/(\d+)', final_url)
                    if match_id_url:
                        match_id = int(match_id_url.group(1))
                    else:
                        logger.error("Failed to extract match ID from URL or page content")
                        return None
                else:
                    match_id = int(match_id_script)

                # Extract data using Playwright's locators
                data = {}
                
                # Team names
                teams = self.page.locator('a[href*="/team/"] span, div[class*="teamName"]').all_text_contents()
                if len(teams) >= 2:
                    data['home_team'] = teams[0].strip()
                    data['away_team'] = teams[1].strip()
                
                # Scores
                scores = self.page.locator('div[class*="scoreBox"] div[class*="score"]').all_text_contents()
                if len(scores) >= 2:
                    data['home_score'] = int(scores[0])
                    data['away_score'] = int(scores[1])

                # Match datetime
                datetime_elem = self.page.locator('time').first
                if datetime_elem:
                    data['datetime'] = datetime_elem.get_attribute('datetime')

                # Additional details
                data.update({
                    'match_id': match_id,
                    'url': url,
                    'status': self.page.locator('div[class*="status"]').text_content(),
                    'venue': self.page.locator('div[class*="venue"]').text_content(),
                    'tournament': self.page.locator('div[class*="tournament"] a').text_content(),
                    'fetched_at': datetime.now(UTC).isoformat()
                })

                logger.info("✅ Successfully extracted match details")
                return data

            except Exception as e:
                logger.error(f"❌ Error extracting match details: {e}")
                return None

        except Exception as e:
            logger.error(f"❌ Error navigating to page: {e}")
            return None

    def fetch_statistics(self, match_id: int) -> Optional[Dict]:
        """Fetch match statistics from API"""
        try:
            headers = {
                'User-Agent': self.get_random_user_agent(),
                'Accept': '*/*',
                'Origin': self.base_url,
                'Referer': f'{self.base_url}/event/{match_id}'
            }

            # Get cookies from current context
            if not self.page: # Changed from self.context
                logger.error("Browser page not initialized")
                return None
            cookies = self.page.context.cookies() # Changed from self.context
            cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies if 'name' in cookie and 'value' in cookie}

            urls = [
                f'{self.api_base_url}/event/{match_id}/statistics',
                f'{self.base_url}/api/v1/event/{match_id}/statistics'
            ]

            for url in urls:
                try:
                    self.random_delay()
                    response = requests.get(url, headers=headers, cookies=cookie_dict, timeout=30)
                    
                    if response.status_code == 200:
                        return response.json()
                    logger.warning(f"Failed with status code: {response.status_code}")
                    
                except Exception as e:
                    logger.error(f"Error with {url}: {e}")
                    continue

            return None

        except Exception as e:
            logger.error(f"Error fetching statistics: {e}")
            return None

    def random_delay(self, min_seconds: float = 0.5, max_seconds: float = 1.5):
        """Enhanced random delay with minimal jitter"""
        base_delay = random.uniform(min_seconds, max_seconds)
        jitter = random.uniform(0, 0.2)  # Reduced jitter from 0.5 to 0.2
        time.sleep(base_delay + jitter)

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=random_exponential()),
        exception=(requests.exceptions.RequestException, PlaywrightTimeout),
        max_tries=3,  # Reduced from 5 to 3
        jitter=backoff.full_jitter,
        giveup=lambda e: not should_retry_request(e)
    )
    def process_match(self, match_id: int, season_path: Path) -> bool:
        """Process and save all data for a single match with backoff"""
        # Create all required directories first
        create_season_directories(season_path)
        
        # Core endpoints that should always work
        core_endpoints = {
            'quarter': f'/event/{match_id}',
            'match_statistics': f'/event/{match_id}/statistics',
            'team-streak': f'/event/{match_id}/team-streaks'
        }
        
        # Optional endpoints that might not be available for older seasons
        optional_endpoints = {
            'score_commentary': f'/event/{match_id}/incidents',
            'player_statistics': f'/event/{match_id}/lineups',
            'text_commentary': f'/event/{match_id}/comments'
        }

        success = True
        with ThreadPoolExecutor(max_workers=3) as endpoint_executor:
            futures = {}
            
            # Process core endpoints first
            for data_type, endpoint in core_endpoints.items():
                try:
                    url = f'{self.api_base_url}{endpoint}'
                    futures[data_type] = endpoint_executor.submit(self.fetch_api_data, url)
                except Exception as e:
                    logger.error(f"Error submitting {data_type} for match ID: {match_id}: {e}")
                    success = False

            # Try optional endpoints but don't fail if they're not available
            for data_type, endpoint in optional_endpoints.items():
                try:
                    url = f'{self.api_base_url}{endpoint}'
                    futures[data_type] = endpoint_executor.submit(self.fetch_api_data, url)
                except Exception as e:
                    logger.warning(f"Optional endpoint {data_type} not available for match ID: {match_id}: {e}")

            # Process completed futures
            for data_type, future in futures.items():
                try:
                    data = future.result()
                    if data:
                        file_path = season_path / data_type / f"{match_id}.json"
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(file_path, 'w') as f:
                            json.dump(data, f, indent=2)
                        logger.info(f"✅ Saved {data_type} for match ID: {match_id}")
                    else:
                        # Don't mark as failure for optional endpoints
                        if data_type in core_endpoints:
                            logger.error(f"Failed to fetch {data_type} for match ID: {match_id}")
                            success = False
                        else:
                            logger.warning(f"Optional data {data_type} not available for match ID: {match_id}")
                except Exception as e:
                    if data_type in core_endpoints:
                        logger.error(f"Error saving {data_type} for match ID: {match_id}: {e}")
                        success = False
                    else:
                        logger.warning(f"Could not save optional {data_type} for match ID: {match_id}: {e}")

            return success

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=random_exponential()),
        exception=(requests.exceptions.RequestException, PlaywrightTimeout),
        max_tries=3,
        jitter=backoff.full_jitter,
        giveup=lambda e: not should_retry_request(e)
    )
    def fetch_season_matches(self, season_id: int, season_path: Path) -> List[Dict]:
        """Fetch all matches for a given WNBA season with pagination and backoff"""
        all_matches = []
        page_no = 0
        
        while True:
            try:
                # Updated URL to use WNBA tournament ID (486)
                url = f'{self.api_base_url}/unique-tournament/486/season/{season_id}/events/last/{page_no}'
                matches_data = self.fetch_api_data(url)
                
                if not matches_data or not matches_data.get('events'):
                    break
                
                current_matches = matches_data['events']
                all_matches.extend(current_matches)
                logger.info(f"Fetched page {page_no} - {len(current_matches)} matches")
                
                # Save the current batch
                with open(season_path / 'match_list.json', 'w') as f:
                    json.dump(all_matches, f, indent=2)
                
                page_no += 1
                # Increase delay between pages
                self.random_delay(2.0, 4.0)
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    break
                raise
            
        return all_matches

    def process_match_batch(self, matches: List[Dict], season_path: Path) -> None:
        """Process a batch of matches using ThreadPoolExecutor"""
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for match in matches:
                future = executor.submit(self.process_match, match['id'], season_path)
                futures.append(future)
            
            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing match: {e}")
                finally:
                    # Ensure browser cleanup happens after each thread completes
                    self.cleanup_thread_browser()

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=2),  # Increased from random_exponential
        exception=(requests.exceptions.RequestException, PlaywrightTimeout),
        max_tries=8,  # Increased from 5
        jitter=backoff.full_jitter,
        giveup=lambda e: not should_retry_request(e)
    )
    def fetch_api_data(self, url: str) -> Optional[Dict]:
        """Fetch data from any Sofascore API endpoint with caching and backoff"""
        cache_key = f"api_data:{url}"
        cached_data = self.cache.get(cache_key)
        if cached_data and isinstance(cached_data, dict):
            return cached_data
        
        try:
            self.random_delay(2, 4)  # Increased from 1, 3
            response = self.session.get(
                url,
                headers={
                    'User-Agent': self.get_random_user_agent(),
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': self.base_url,
                    'DNT': '1',
                    'Connection': 'keep-alive'
                }
            )
            
            if response.status_code == 403:
                self.refresh_cookies()
                raise requests.exceptions.RequestException("Rate limited")
                
            response.raise_for_status()
            result = response.json()
            
            # Store in disk cache with longer expiration (4-8 hours)
            self.cache.set(cache_key, result, expire=random.randint(14400, 28800))  # Increased from 7200, 14400
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            raise

    def refresh_cookies(self):
        """Thread-safe cookie refresh"""
        with self._cookie_refresh_lock:
            try:
                if not self.page:
                    logger.warning("No browser page available for cookie refresh")
                    return
                
                cookies = self.page.context.cookies()
                cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies 
                             if 'name' in cookie and 'value' in cookie}
                
                with self._lock:  # Changed from self._session_lock to self._lock
                    for cookie in cookie_dict:
                        self.session.cookies.set(cookie, cookie_dict[cookie])
                
                logger.info("✅ Cookies refreshed successfully")
            except Exception as e:
                logger.error(f"Failed to refresh cookies: {e}")

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=2),
        exception=(requests.exceptions.RequestException, PlaywrightTimeout),
        max_tries=8,
        jitter=backoff.full_jitter,
        giveup=lambda e: not should_retry_request(e)
    )
    def fetch_seasons_list(self) -> Optional[Dict]:
        """Fetch all available WNBA seasons from SofaScore API"""
        url = f'{self.api_base_url}/unique-tournament/486/seasons'
        return self.fetch_api_data(url)

class NFLScraper(BasketballScraper):  # Inherit from existing BasketballScraper for core functionality
    def __init__(self):
        super().__init__()
        self.tournament_id = 9464  # NFL tournament ID
        
    def fetch_nfl_seasons(self) -> Optional[Dict]:
        """Fetch all available NFL seasons"""
        url = f"{self.api_base_url}/unique-tournament/{self.tournament_id}/seasons"
        seasons_data = self.fetch_api_data(url)
        
        if seasons_data and 'seasons' in seasons_data:
            # Sort seasons by ID in descending order (newest first)
            seasons_data['seasons'].sort(key=lambda x: x['id'], reverse=True)
            logger.info(f"Found {len(seasons_data['seasons'])} NFL seasons from {seasons_data['seasons'][-1]['year']} to {seasons_data['seasons'][0]['year']}")
        
        return seasons_data

    def get_season_name(self, season_id: int, seasons_data: Dict) -> str:
        """Get season name from season ID"""
        for season in seasons_data['seasons']:
            if season['id'] == season_id:
                return season['name']
        return f"Unknown Season (ID: {season_id})"

    def fetch_season_matches(self, season_id: int, season_path: Path, seasons_data: Dict) -> List[Dict]:
        """Fetch all matches for a given NFL season"""
        # Validate season ID
        valid_season_ids = [season['id'] for season in seasons_data['seasons']]
        if season_id not in valid_season_ids:
            logger.error(f"Invalid season ID: {season_id}")
            return []
        
        season_name = self.get_season_name(season_id, seasons_data)
        
        # Skip future seasons
        current_year = datetime.now().year
        season_year = int("20" + season_name.split("/")[0][-2:])  # Convert "NFL 25/26" to 2025
        if season_year > current_year:
            logger.info(f"Skipping future season {season_name} (ID: {season_id})")
            return []
        
        logger.info(f"Fetching matches for {season_name} (ID: {season_id})")
        
        matches = []
        page = 0
        
        while True:
            try:
                url = f"{self.api_base_url}/unique-tournament/{self.tournament_id}/season/{season_id}/events/last/{page}"
                data = self.fetch_api_data(url)
                
                if not data or not data.get('events'):
                    break
                
                current_batch = data['events']
                matches.extend(current_batch)
                logger.info(f"{season_name}: Fetched page {page} with {len(current_batch)} matches. Total: {len(matches)}")
                
                page += 1
                self.random_delay()
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # If we get a 404, we've either hit the end of pages or the season doesn't exist
                    logger.info(f"No more matches found for {season_name}")
                    break
                raise  # Re-raise other HTTP errors
            except Exception as e:
                logger.error(f"Error fetching matches for {season_name}: {e}")
                break
        
        logger.info(f"Completed fetching {len(matches)} matches for {season_name}")
        return matches

    def fetch_match_details(self, match_id: int) -> Dict[str, Optional[Dict]]:
        """Fetch all available details for a specific NFL match"""
        endpoints = {
            'match_info': f"{self.api_base_url}/event/{match_id}",
            'team_streaks': f"{self.api_base_url}/event/{match_id}/team-streaks",
            'play_by_play': f"{self.api_base_url}/event/{match_id}/incidents"
        }
        
        results = {}
        for data_type, url in endpoints.items():
            results[data_type] = self.fetch_api_data(url)
            self.random_delay()
            
        return results

    def process_match(self, match_id: int, season_path: Path) -> bool:
        """Process and save all data for a specific NFL match"""
        try:
            match_data = self.fetch_match_details(match_id)
            
            # Define the mapping of data types to directories
            data_directories = {
                'match_info': 'quarter',
                'team_streaks': 'team-streak',
                'play_by_play': 'score_commentary'
            }
            
            # Save each type of data in its respective directory
            for data_type, data in match_data.items():
                if not data:
                    continue
                
                # Get the corresponding directory name
                dir_name = data_directories.get(data_type)
                if not dir_name:
                    continue
                    
                # Create the file path with match_id
                save_path = season_path / dir_name / f"{match_id}.json"
                save_path.parent.mkdir(parents=True, exist_ok=True)
                
                with save_path.open('w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                
                logger.info(f"Saved {dir_name} data for match {match_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing match {match_id}: {e}")
            return False

def main():
    # Create base data directory
    base_path = Path('data')
    base_path.mkdir(exist_ok=True)
    
    seasons = [
        # {"id": 75522, "name": "NFL 25/26"},  # Already done
        # {"id": 60592, "name": "NFL 24/25"},  # Already done
        # {"id": 51361, "name": "NFL 23/24"},  # Already done
        # {"id": 46786, "name": "NFL 22/23"},  # Already done
        {"id": 36422, "name": "NFL 21/22"},    # Start from here
        {"id": 27719, "name": "NFL 20/21"},
        {"id": 23303, "name": "NFL 19/20"},
        {"id": 16754, "name": "18/19"},
        {"id": 13091, "name": "17/18"},
        {"id": 11386, "name": "16/17"},
        {"id": 10155, "name": "15/16"},
        {"id": 9717, "name": "14/15"},
        {"id": 9716, "name": "13/14"},
        {"id": 9715, "name": "12/13"},
        {"id": 9714, "name": "11/12"},
        {"id": 9713, "name": "NFL 10/11"},
        {"id": 9712, "name": "NFL 09/10"},
        {"id": 9711, "name": "NFL 08/09"},
        {"id": 9710, "name": "NFL 07/08"},
        {"id": 9709, "name": "NFL 06/07"},
        {"id": 9708, "name": "NFL 05/06"},
        {"id": 9707, "name": "NFL 04/05"},
        {"id": 9706, "name": "NFL 03/04"},
        {"id": 9705, "name": "NFL 02/03"},
        {"id": 36659, "name": "NFL 01/02"}
    ]
    
    with NFLScraper() as scraper:
        try:
            # Fetch all NFL seasons
            seasons_data = scraper.fetch_nfl_seasons()
            if not seasons_data:
                logger.error("Failed to fetch NFL seasons")
                return
            
            # Save seasons list
            with open(base_path / 'seasons_list.json', 'w') as f:
                json.dump(seasons_data, f, indent=2)
            
            # Process each season
            for season in seasons:
                try:
                    season_id = season['id']
                    season_name = season['name']
                    
                    # Create season directory with formatted name
                    season_folder_name = format_season_folder_name(season_name)
                    season_path = base_path / season_folder_name
                    season_path.mkdir(exist_ok=True)
                    
                    logger.info(f"Processing {season_name} (ID: {season_id})")
                    
                    # Create required directories
                    create_season_directories(season_path)
                    
                    # Fetch all matches for the season
                    matches = scraper.fetch_season_matches(season_id, season_path, seasons_data)
                    
                    if not matches:
                        logger.warning(f"No matches found for {season_name}, continuing to next season")
                        continue
                    
                    # Save match list
                    with open(season_path / 'match_list.json', 'w') as f:
                        json.dump(matches, f, indent=2)
                    
                    # Process matches in batches
                    for i in range(0, len(matches), BATCH_SIZE):
                        batch = matches[i:i + BATCH_SIZE]
                        logger.info(f"{season_name}: Processing batch {i//BATCH_SIZE + 1} of {(len(matches) + BATCH_SIZE - 1)//BATCH_SIZE}")
                        scraper.process_match_batch(batch, season_path)
                    
                    logger.info(f"Completed processing {season_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing season {season.get('name', 'Unknown')}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Fatal error in main process: {e}")
            raise

if __name__ == "__main__":
    main()

