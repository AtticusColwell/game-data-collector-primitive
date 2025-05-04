"""
NBA Player Data Fetcher and Supabase Uploader (Fixed Version)

This script:
1. Reads a list of player names from players.txt
2. Fetches player IDs using the NBA API
3. Retrieves detailed player data from various NBA API endpoints
4. Stores the data in Supabase with improved error handling
"""

import os
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import requests
from supabase import create_client, Client
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_data_fetch.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL and API key must be provided in environment variables")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# NBA API configuration
NBA_API_BASE_URL = "https://stats.nba.com/stats"

# Headers that mimic a browser
NBA_API_HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
    'Origin': 'https://www.nba.com',
    'Connection': 'keep-alive',
    'Referer': 'https://www.nba.com/',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site'
}

# NBA API endpoints
PLAYER_INFO_ENDPOINT = "/commonplayerinfo"
PLAYER_STATS_ENDPOINT = "/playerdashboardbygeneralsplits"
PLAYER_CAREER_ENDPOINT = "/playercareerstats"
PLAYER_PROFILE_ENDPOINT = "/playerprofilev2"
PLAYER_ALL_ENDPOINT = "/commonallplayers"

# Constants
CURRENT_SEASON = "2024-25"  # Update as needed


class NBADataFetcher:
    """Class to handle fetching data from the NBA API"""

    def __init__(self, rate_limit_wait: float = 1.5, max_retries: int = 3):
        """
        Initialize the NBA data fetcher

        Args:
            rate_limit_wait: Time to wait between API calls in seconds
            max_retries: Maximum number of retry attempts for failed requests
        """
        self.rate_limit_wait = rate_limit_wait
        self.max_retries = max_retries
        self.all_players_cache = None  # Cache for all players

    def _make_api_request(self, endpoint: str, params: Dict[str, Any], skip_500_retry: bool = False) -> Optional[Dict[str, Any]]:
        """
        Make a request to the NBA API with rate limiting and retries

        Args:
            endpoint: The API endpoint
            params: Query parameters for the request
            skip_500_retry: Whether to skip retrying on 500 errors (some endpoints consistently fail with 500)

        Returns:
            The JSON response or None if the request failed after retries
        """
        url = f"{NBA_API_BASE_URL}{endpoint}"
        retries = 0
        backoff = self.rate_limit_wait

        # Always wait BEFORE making the request
        time.sleep(self.rate_limit_wait)
        
        while retries <= self.max_retries:
            try:
                logger.debug(f"Making API request to {url} with params {params}")
                response = requests.get(
                    url, 
                    headers=NBA_API_HEADERS, 
                    params=params, 
                    timeout=20,
                    allow_redirects=False  # Prevent redirect loops
                )
                
                # Check if we got a 500 error and should skip retries
                if response.status_code == 500 and skip_500_retry and retries > 0:
                    logger.warning(f"Received 500 error and skip_500_retry is True. Giving up after {retries+1} attempts.")
                    return None
                
                response.raise_for_status()
                
                # Check if we got a redirect
                if response.status_code >= 300 and response.status_code < 400:
                    logger.warning(f"Received redirect response ({response.status_code}). Adjusting approach.")
                    retries += 1
                    time.sleep(backoff)
                    backoff *= 2  # Exponential backoff
                    continue
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request error (attempt {retries+1}/{self.max_retries+1}): {e}")
                retries += 1
                
                # If we're rate limited, wait longer
                if hasattr(e, 'response') and e.response:
                    if e.response.status_code == 429:
                        backoff = max(10, backoff * 2)  # At least 10 seconds, doubling each time
                        logger.warning(f"Rate limited. Waiting {backoff} seconds before retrying...")
                    elif e.response.status_code == 500 and skip_500_retry and retries > 0:
                        # Skip retrying for certain 500 errors that consistently fail
                        logger.warning(f"Received 500 error and skip_500_retry is True. Giving up after {retries} attempts.")
                        return None
                
                # Standard exponential backoff for other errors
                backoff *= 2
                
                if retries <= self.max_retries:
                    logger.info(f"Retrying in {backoff} seconds...")
                    time.sleep(backoff)
                else:
                    logger.error(f"Failed after {self.max_retries+1} attempts.")
                    return None
        
        return None
    
    def fetch_all_players(self, force_refresh: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch list of all NBA players
        
        Args:
            force_refresh: Whether to force a refresh of the cached player list
            
        Returns:
            List of player dictionaries or None if the request failed
        """
        # Return cached results if available
        if self.all_players_cache is not None and not force_refresh:
            return self.all_players_cache
            
        logger.info("Fetching list of all NBA players...")
        
        # Using a more reliable endpoint
        params = {
            "LeagueID": "00",
            "Season": CURRENT_SEASON,
            "IsOnlyCurrentSeason": "1"
        }
        
        response = self._make_api_request(PLAYER_ALL_ENDPOINT, params)
        
        if not response:
            return None
            
        try:
            result_sets = response.get("resultSets", [])
            if not result_sets or not result_sets[0].get("rowSet"):
                logger.warning("No players found in the response")
                return None
                
            # Get column headers and row data
            headers = result_sets[0]["headers"]
            rows = result_sets[0]["rowSet"]
            
            # Create list of player dictionaries
            players = []
            for row in rows:
                player = dict(zip(headers, row))
                players.append(player)
            
            # Cache the results
            self.all_players_cache = players
            
            logger.info(f"Successfully fetched {len(players)} players")
            return players
            
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing all players response: {e}")
            return None

    def search_player_by_name(self, player_name: str) -> Optional[int]:
        """
        Search for a player by name to get their player ID using cached list

        Args:
            player_name: The player's full name

        Returns:
            The player ID or None if not found
        """
        logger.info(f"Searching for player: {player_name}")
        
        # Get all players if we haven't already
        all_players = self.fetch_all_players()
        if not all_players:
            logger.error("Could not fetch player list")
            return None
        
        # Normalize the player name for comparison
        normalized_name = player_name.lower().strip()
        
        # First try exact match
        for player in all_players:
            # Check both display name formats
            display_name = player.get("DISPLAY_FIRST_LAST", "").lower()
            display_last_first = player.get("DISPLAY_LAST_COMMA_FIRST", "").lower()
            
            if normalized_name == display_name or normalized_name == display_last_first:
                player_id = player.get("PERSON_ID")
                logger.info(f"Found exact match for {player_name}: ID {player_id}")
                return player_id
        
        # Then try partial matches
        partial_matches = []
        for player in all_players:
            display_name = player.get("DISPLAY_FIRST_LAST", "").lower()
            
            # Check if the search name is contained in the player name
            # or if each part of the search name appears in the player name
            name_parts = normalized_name.split()
            if (normalized_name in display_name or 
                all(part in display_name for part in name_parts)):
                partial_matches.append((
                    player.get("PERSON_ID"),
                    player.get("DISPLAY_FIRST_LAST")
                ))
        
        if partial_matches:
            # Just take the first match for simplicity
            player_id, found_name = partial_matches[0]
            logger.info(f"Found partial match for {player_name}: {found_name} (ID {player_id})")
            return player_id
        
        logger.warning(f"No player found for: {player_name}")
        return None

    def fetch_player_info(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch basic information for a player

        Args:
            player_id: The NBA player ID

        Returns:
            Dictionary with player information or None if the request failed
        """
        logger.info(f"Fetching basic info for player {player_id}")

        params = {
            "PlayerID": player_id,
            "LeagueID": "00"
        }

        response = self._make_api_request(PLAYER_INFO_ENDPOINT, params)

        if not response:
            return None

        try:
            # Extract player info from the response
            result_sets = response.get("resultSets", [])
            if not result_sets or not result_sets[0].get("rowSet"):
                logger.warning(f"No data found for player {player_id}")
                return None

            # Get column headers and row data
            headers = result_sets[0]["headers"]
            row_data = result_sets[0]["rowSet"][0]

            # Create dictionary mapping headers to values
            player_info = dict(zip(headers, row_data))
            return player_info

        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing player info for {player_id}: {e}")
            return None

    def fetch_player_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch current season stats for a player

        Args:
            player_id: The NBA player ID

        Returns:
            Dictionary with player stats or None if the request failed
        """
        logger.info(f"Fetching current season stats for player {player_id}")

        params = {
            "PlayerID": player_id,
            "LeagueID": "00",
            "Season": CURRENT_SEASON,
            "SeasonType": "Regular Season",
            "MeasureType": "Base"
        }

        # Skip retrying for 500 errors after first attempt - this endpoint often gives 500 for valid requests
        response = self._make_api_request(PLAYER_STATS_ENDPOINT, params, skip_500_retry=True)

        if not response:
            return None

        try:
            # Extract player stats from the response
            result_sets = response.get("resultSets", [])
            if not result_sets or not result_sets[0].get("rowSet") or not result_sets[0]["rowSet"]:
                logger.warning(f"No current season stats found for player {player_id}")
                return None

            # Get column headers and row data
            headers = result_sets[0]["headers"]
            row_data = result_sets[0]["rowSet"][0]

            # Create dictionary mapping headers to values
            player_stats = dict(zip(headers, row_data))
            return player_stats

        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing player stats for {player_id}: {e}")
            return None

    def fetch_player_career_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch career stats for a player

        Args:
            player_id: The NBA player ID

        Returns:
            Dictionary with career stats or None if the request failed
        """
        logger.info(f"Fetching career stats for player {player_id}")

        params = {
            "PlayerID": player_id,
            "LeagueID": "00",
            "PerMode": "PerGame"
        }

        response = self._make_api_request(PLAYER_CAREER_ENDPOINT, params)

        if not response:
            return None

        try:
            # Find the career totals in the response
            result_sets = response.get("resultSets", [])
            career_totals_set = None

            for result_set in result_sets:
                if result_set.get("name") == "CareerTotalsRegularSeason":
                    career_totals_set = result_set
                    break

            if not career_totals_set or not career_totals_set.get("rowSet") or not career_totals_set["rowSet"]:
                logger.warning(f"No career stats found for player {player_id}")
                return None

            # Get column headers and row data
            headers = career_totals_set["headers"]
            row_data = career_totals_set["rowSet"][0]

            # Create dictionary mapping headers to values
            career_stats = dict(zip(headers, row_data))
            return career_stats

        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing career stats for {player_id}: {e}")
            return None

    def fetch_player_season_highs(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch season highs for a player

        Args:
            player_id: The NBA player ID

        Returns:
            Dictionary with season highs or None if the request failed
        """
        logger.info(f"Fetching season highs for player {player_id}")

        params = {
            "PlayerID": player_id,
            "LeagueID": "00"
        }

        # Skip retrying for 500 errors after first attempt - this endpoint often gives 500 for valid requests
        response = self._make_api_request(PLAYER_PROFILE_ENDPOINT, params, skip_500_retry=True)

        if not response:
            return None

        try:
            # Extract needed sections from the profile
            result_sets = response.get("resultSets", [])
            if not result_sets:
                logger.warning(f"No profile data found for player {player_id}")
                return None

            season_highs_set = None
            for result_set in result_sets:
                if result_set.get("name") == "SeasonHighs":
                    season_highs_set = result_set
                    break

            if not season_highs_set or not season_highs_set.get("rowSet") or not season_highs_set["rowSet"]:
                logger.warning(f"No season highs found for player {player_id}")
                return None

            # Get column headers and row data
            headers = season_highs_set["headers"]
            row_data = season_highs_set["rowSet"][0]

            # Create dictionary mapping headers to values
            season_highs = dict(zip(headers, row_data))
            return season_highs

        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing season highs for {player_id}: {e}")
            return None


class SupabaseUploader:
    """Class to handle uploading data to Supabase"""

    def __init__(self, client: Client):
        """
        Initialize the Supabase uploader

        Args:
            client: Initialized Supabase client
        """
        self.client = client

    def store_player_basic_info(self, player_data: Dict[str, Any]) -> bool:
        """
        Store basic player information in Supabase

        Args:
            player_data: Player data dictionary

        Returns:
            True if successful, False otherwise
        """
        player_id = player_data.get("PERSON_ID")
        player_name = player_data.get("DISPLAY_FIRST_LAST", f"ID: {player_id}")
        logger.info(f"Storing basic info for {player_name}")

        try:
            # Format data for the nba_players table
            formatted_data = {
                "player_id": player_id,
                "first_name": player_data.get("FIRST_NAME"),
                "last_name": player_data.get("LAST_NAME"),
                "full_name": player_data.get("DISPLAY_FIRST_LAST"),
                "jersey_number": player_data.get("JERSEY"),
                "position": player_data.get("POSITION"),
                "height": player_data.get("HEIGHT"),
                "weight": player_data.get("WEIGHT"),
                "birth_date": player_data.get("BIRTHDATE"),
                "country": player_data.get("COUNTRY"),
                "school": player_data.get("SCHOOL"),
                "draft_year": player_data.get("DRAFT_YEAR"),
                "draft_round": player_data.get("DRAFT_ROUND"),
                "draft_number": player_data.get("DRAFT_NUMBER"),
                "team_id": player_data.get("TEAM_ID"),
                "team_name": player_data.get("TEAM_NAME"),
                "from_year": player_data.get("FROM_YEAR"),
                "to_year": player_data.get("TO_YEAR"),
                "updated_at": datetime.now().isoformat()
            }

            # Insert or update player data
            result = self.client.table("nba_players").upsert(
                formatted_data,
                on_conflict="player_id"
            ).execute()

            if hasattr(result, 'error') and result.error:
                logger.error(f"Error storing basic info for {player_name}: {result.error}")
                return False

            logger.info(f"Successfully stored basic info for {player_name}")
            return True

        except Exception as e:
            logger.error(f"Exception storing basic info for {player_name}: {e}", exc_info=True)
            return False

    def store_player_current_stats(self, player_id: int, stats_data: Dict[str, Any]) -> bool:
        """
        Store current season stats in Supabase

        Args:
            player_id: Player ID
            stats_data: Stats data dictionary

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Storing current stats for player {player_id}")

        try:
            # Format data for the player_current_stats table
            formatted_data = {
                "player_id": player_id,
                "season": CURRENT_SEASON,
                "games_played": stats_data.get("GP"),
                "ppg": stats_data.get("PTS"),
                "rpg": stats_data.get("REB"),
                "apg": stats_data.get("AST"),
                "spg": stats_data.get("STL"),
                "bpg": stats_data.get("BLK"),
                "fg_pct": stats_data.get("FG_PCT"),
                "ft_pct": stats_data.get("FT_PCT"),
                "fg3_pct": stats_data.get("FG3_PCT"),
                "minutes": stats_data.get("MIN"),
                "additional_stats": {
                    "fg_m": stats_data.get("FGM"),
                    "fg_a": stats_data.get("FGA"),
                    "fg3_m": stats_data.get("FG3M"),
                    "fg3_a": stats_data.get("FG3A"),
                    "ft_m": stats_data.get("FTM"),
                    "ft_a": stats_data.get("FTA"),
                    "oreb": stats_data.get("OREB"),
                    "dreb": stats_data.get("DREB"),
                    "tov": stats_data.get("TOV"),
                    "pf": stats_data.get("PF"),
                    "plus_minus": stats_data.get("PLUS_MINUS")
                },
                "updated_at": datetime.now().isoformat()
            }

            # Insert or update the stats data
            result = self.client.table("player_current_stats").upsert(
                formatted_data,
                on_conflict="player_id"
            ).execute()

            if hasattr(result, 'error') and result.error:
                logger.error(f"Error storing current stats for player {player_id}: {result.error}")
                return False

            logger.info(f"Successfully stored current stats for player {player_id}")
            return True

        except Exception as e:
            logger.error(f"Exception storing current stats for player {player_id}: {e}", exc_info=True)
            return False

    def store_player_career_stats(self, player_id: int, career_data: Dict[str, Any]) -> bool:
        """
        Store career stats in Supabase

        Args:
            player_id: Player ID
            career_data: Career stats data dictionary

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Storing career stats for player {player_id}")

        try:
            # Format data for the player_career_stats table
            formatted_data = {
                "player_id": player_id,
                "games_played": career_data.get("GP"),
                "ppg": career_data.get("PTS"),
                "rpg": career_data.get("REB"),
                "apg": career_data.get("AST"),
                "spg": career_data.get("STL"),
                "bpg": career_data.get("BLK"),
                "fg_pct": career_data.get("FG_PCT"),
                "ft_pct": career_data.get("FT_PCT"),
                "fg3_pct": career_data.get("FG3_PCT"),
                "additional_stats": {
                    "fg_m": career_data.get("FGM"),
                    "fg_a": career_data.get("FGA"),
                    "fg3_m": career_data.get("FG3M"),
                    "fg3_a": career_data.get("FG3A"),
                    "ft_m": career_data.get("FTM"),
                    "ft_a": career_data.get("FTA"),
                    "oreb": career_data.get("OREB"),
                    "dreb": career_data.get("DREB"),
                    "tov": career_data.get("TOV"),
                    "pf": career_data.get("PF")
                },
                "updated_at": datetime.now().isoformat()
            }

            # Insert or update the career stats data
            result = self.client.table("player_career_stats").upsert(
                formatted_data,
                on_conflict="player_id"
            ).execute()

            if hasattr(result, 'error') and result.error:
                logger.error(f"Error storing career stats for player {player_id}: {result.error}")
                return False

            logger.info(f"Successfully stored career stats for player {player_id}")
            return True

        except Exception as e:
            logger.error(f"Exception storing career stats for player {player_id}: {e}", exc_info=True)
            return False

    def store_player_season_highs(self, player_id: int, highs_data: Dict[str, Any]) -> bool:
        """
        Store season highs in Supabase

        Args:
            player_id: Player ID
            highs_data: Season highs data dictionary

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Storing season highs for player {player_id}")

        try:
            # Format data for the player_season_highs table
            formatted_data = {
                "player_id": player_id,
                "season": CURRENT_SEASON,
                "points": highs_data.get("PTS"),
                "rebounds": highs_data.get("REB"),
                "assists": highs_data.get("AST"),
                "steals": highs_data.get("STL"),
                "blocks": highs_data.get("BLK"),
                "highest_minutes": highs_data.get("MIN"),
                "additional_highs": {
                    "field_goals_made": highs_data.get("FGM"),
                    "three_pointers_made": highs_data.get("FG3M"),
                    "free_throws_made": highs_data.get("FTM"),
                    "offensive_rebounds": highs_data.get("OREB"),
                    "defensive_rebounds": highs_data.get("DREB"),
                    "turnovers": highs_data.get("TOV")
                },
                "updated_at": datetime.now().isoformat()
            }

            # Insert or update the season highs data
            result = self.client.table("player_season_highs").upsert(
                formatted_data,
                on_conflict="player_id"
            ).execute()

            if hasattr(result, 'error') and result.error:
                logger.error(f"Error storing season highs for player {player_id}: {result.error}")
                return False

            logger.info(f"Successfully stored season highs for player {player_id}")
            return True

        except Exception as e:
            logger.error(f"Exception storing season highs for player {player_id}: {e}", exc_info=True)
            return False


def load_player_names(file_path: str = "players.txt") -> List[str]:
    """
    Load player names from a text file

    Args:
        file_path: Path to the text file containing player names

    Returns:
        List of player names
    """
    try:
        with open(file_path, 'r') as f:
            # Read lines and strip whitespace
            player_names = [line.strip() for line in f if line.strip()]
        
        logger.info(f"Loaded {len(player_names)} player names from {file_path}")
        return player_names
            
    except Exception as e:
        logger.error(f"Error loading player names: {e}")
        return []


def main():
    """Main function to process all players"""
    # Check that Supabase credentials are available
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Supabase URL or API key not found in environment variables")
        return
    
    # Load player names
    player_names = load_player_names()
    
    if not player_names:
        logger.error("No player names loaded. Please check your players.txt file.")
        return
    
    # Initialize classes
    fetcher = NBADataFetcher(rate_limit_wait=1.5, max_retries=3)
    uploader = SupabaseUploader(supabase)
    
    # Record start time for overall process
    start_time = time.time()
    
    # Print rate limit warning
    logger.info("=== NBA API CONNECTION INFO ===")
    logger.info("Using strict 1.5 second delay between API requests to avoid rate limiting")
    logger.info(f"Estimated processing time for {len(player_names)} players: " + 
                f"approximately {len(player_names) * 4 * 1.5 / 60:.1f} minutes minimum " +
                f"(4 API calls per player with 1.5s delay)")
    logger.info("Added improved error handling for 500 errors")
    logger.info("Using cache for player lookup to minimize API calls")
    logger.info("=============================")
    
    # Preload all players at the start to reduce API calls
    logger.info("Preloading all current NBA players...")
    all_players = fetcher.fetch_all_players()
    if not all_players:
        logger.error("Failed to preload player list from NBA API. Continuing with individual lookups...")
    else:
        logger.info(f"Successfully preloaded {len(all_players)} players from NBA API")
    
    # Track progress
    progress_interval = max(1, len(player_names) // 20)  # Show progress every ~5%
    last_progress_time = time.time()
    
    # Create a resumption point file to allow continuing after interruptions
    resumption_file = "nba_progress.json"
    processed_players = []
    start_index = 0
    
    # Check if we have a resumption point
    if os.path.exists(resumption_file):
        try:
            with open(resumption_file, 'r') as f:
                resume_data = json.load(f)
                processed_players = resume_data.get('processed', [])
                last_player = resume_data.get('last_player', '')
                
                if last_player and last_player in player_names:
                    start_index = player_names.index(last_player) + 1
                    logger.info(f"Resuming from player {start_index}/{len(player_names)}: {last_player}")
                else:
                    logger.info(f"Found {len(processed_players)} previously processed players")
        except Exception as e:
            logger.error(f"Error reading resumption file: {e}")
            # Continue from the beginning if there's an error
    
    # Process each player
    total_players = len(player_names)
    success_count = 0
    fail_count = 0
    
    logger.info(f"Starting to process {total_players} players from index {start_index}...")
    
    try:
        for i in range(start_index, len(player_names)):
            player_name = player_names[i]
            
            # Show progress periodically
            if (i - start_index + 1) % progress_interval == 0 or time.time() - last_progress_time > 300:
                elapsed = time.time() - start_time
                players_done = i - start_index + 1
                if players_done > 0:
                    avg_time_per_player = elapsed / players_done
                    est_remaining = avg_time_per_player * (total_players - i - 1)
                    logger.info(f"Progress: {i+1}/{total_players} players ({(i+1)/total_players*100:.1f}%)")
                    logger.info(f"Elapsed: {elapsed/60:.1f} minutes, Est. remaining: {est_remaining/60:.1f} minutes")
                    last_progress_time = time.time()
            
            logger.info(f"Processing player {i+1}/{total_players}: {player_name}...")
            
            # Check if this player was already processed
            already_processed = False
            for proc in processed_players:
                if proc.get("name") == player_name:
                    logger.info(f"Player {player_name} was already processed in a previous run, skipping...")
                    already_processed = True
                    success_count += 1
                    break
                    
            if already_processed:
                continue
            
            # First, search for the player to get their ID
            player_id = fetcher.search_player_by_name(player_name)
            
            if not player_id:
                logger.error(f"Could not find player ID for: {player_name}")
                fail_count += 1
                continue
            
            # Fetch player basic info
            basic_info = fetcher.fetch_player_info(player_id)
            
            if not basic_info:
                logger.error(f"Could not fetch basic info for player: {player_name} (ID: {player_id})")
                fail_count += 1
                continue
            
            # Store basic info
            if not uploader.store_player_basic_info(basic_info):
                logger.error(f"Failed to store basic info for player: {player_name}")
                fail_count += 1
                continue
                
            success = True
            
            # Fetch and store current season stats - don't worry if they fail
            current_stats = fetcher.fetch_player_stats(player_id)
            if current_stats:
                success = uploader.store_player_current_stats(player_id, current_stats) and success
            else:
                logger.warning(f"No current season stats available for player: {player_name}")
                # Not counting this as a failure - many players don't have current stats
            
            # Fetch and store career stats
            career_stats = fetcher.fetch_player_career_stats(player_id)
            if career_stats:
                success = uploader.store_player_career_stats(player_id, career_stats) and success
            else:
                logger.warning(f"No career stats available for player: {player_name}")
                # Not counting as a failure, but it's unusual
            
            # Fetch and store season highs - don't worry if they fail
            season_highs = fetcher.fetch_player_season_highs(player_id)
            if season_highs:
                success = uploader.store_player_season_highs(player_id, season_highs) and success
            else:
                logger.warning(f"No season highs available for player: {player_name}")
                # Not counting this as a failure - many players don't have season highs
            
            # Consider the player processed even if some stats are missing
            # The most important thing is that we have the basic player info
            player_info = {
                "name": player_name,
                "id": player_id,
                "full_name": basic_info.get("DISPLAY_FIRST_LAST", player_name),
                "processed_at": datetime.now().isoformat()
            }
            
            processed_players.append(player_info)
            success_count += 1
            logger.info(f"Successfully processed player: {player_name}")
                
            # Update the resumption file periodically
            if (i + 1) % 5 == 0:  # Every 5 players
                with open(resumption_file, 'w') as f:
                    json.dump({
                        'last_player': player_name,
                        'processed': processed_players
                    }, f)
    
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        # Save progress before exiting
        with open(resumption_file, 'w') as f:
            json.dump({
                'last_player': player_name if 'player_name' in locals() else '',
                'processed': processed_players
            }, f)
        logger.info(f"Progress saved. Resume from player: {player_name if 'player_name' in locals() else ''}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        # Save progress before exiting
        with open(resumption_file, 'w') as f:
            json.dump({
                'last_player': player_name if 'player_name' in locals() else '',
                'processed': processed_players
            }, f)
        logger.info(f"Progress saved. Resume from player: {player_name if 'player_name' in locals() else ''}")
        raise  # Re-raise the exception after saving progress
    
    # Processing completed
    elapsed_time = time.time() - start_time
    
    # Print summary
    logger.info("=" * 50)
    logger.info(f"Processing complete. Runtime: {elapsed_time/60:.1f} minutes")
    logger.info(f"Successfully processed {success_count} of {total_players} players.")
    logger.info(f"Failed to process {fail_count} players.")
    
    # Save the final list of processed players
    with open("processed_players.json", "w") as f:
        json.dump(processed_players, f, indent=2)
    
    logger.info(f"Saved list of processed players to processed_players.json")
    
    # Clean up resumption file if everything completed successfully
    if success_count + fail_count >= total_players - start_index and os.path.exists(resumption_file):
        try:
            os.remove(resumption_file)
            logger.info(f"Removed resumption file: {resumption_file}")
        except Exception as e:
            logger.warning(f"Could not remove resumption file: {e}")


if __name__ == "__main__":
    main()