"""
NBA Player Data Fetcher and Supabase Uploader
This script fetches basic player info, career stats, and headshots for NBA players 
and stores it in Supabase (nba_players and player_career_stats tables).
"""

import os
import time
import json
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime

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

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# NBA API configuration
NBA_API_BASE_URL = "https://stats.nba.com/stats"
NBA_API_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Host': 'stats.nba.com',
    'Origin': 'https://www.nba.com',
    'Referer': 'https://www.nba.com/',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

# NBA API endpoints
PLAYER_INFO_ENDPOINT = "/commonplayerinfo"
PLAYER_CAREER_ENDPOINT = "/playercareerstats"
PLAYER_ALL_ENDPOINT = "/commonallplayers"


class NBADataFetcher:
    """Class to handle fetching data from the NBA API"""
    
    def __init__(self, rate_limit_wait: float = 1.0, max_retries: int = 3):
        """
        Initialize the NBA data fetcher
        
        Args:
            rate_limit_wait: Time to wait between API calls in seconds
            max_retries: Maximum number of retry attempts for failed requests
        """
        self.rate_limit_wait = rate_limit_wait
        self.max_retries = max_retries
        self.all_players_cache = None  # Cache for all players
    
    def _make_api_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Make a request to the NBA API with rate limiting and retries
        
        Args:
            endpoint: The API endpoint
            params: Query parameters for the request
            
        Returns:
            The JSON response or None if the request failed
        """
        url = f"{NBA_API_BASE_URL}{endpoint}"
        retries = 0
        backoff = self.rate_limit_wait
        
        while retries <= self.max_retries:
            try:
                logger.debug(f"Making API request to {url} with params {params}")
                response = requests.get(
                    url, 
                    headers=NBA_API_HEADERS, 
                    params=params, 
                    timeout=20
                )
                
                response.raise_for_status()
                
                # Wait to avoid rate limiting
                time.sleep(self.rate_limit_wait)
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request error (attempt {retries+1}/{self.max_retries+1}): {e}")
                retries += 1
                
                # If we're rate limited, wait longer
                if hasattr(e, 'response') and e.response and e.response.status_code == 429:
                    backoff = max(10, backoff * 2)  # At least 10 seconds, doubling each time
                    logger.warning(f"Rate limited. Waiting {backoff} seconds before retrying...")
                else:
                    # Standard exponential backoff for other errors
                    backoff *= 2
                
                if retries <= self.max_retries:
                    logger.info(f"Retrying in {backoff} seconds...")
                    time.sleep(backoff)
                else:
                    logger.error(f"Failed after {self.max_retries+1} attempts.")
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
        current_season = "2024-25"  # Update as needed
        params = {
            "LeagueID": "00",
            "Season": current_season,
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
        Search for a player by name to get their player ID
        
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
            
        Returns:a
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
            
            if not career_totals_set or not career_totals_set.get("rowSet"):
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
            
    def get_player_headshot_url(self, player_id: int) -> str:
        """
        Generate URL for a player's headshot image
        
        Args:
            player_id: The NBA player ID
            
        Returns:
            URL string for the player's headshot
        """
        logger.info(f"Generating headshot URL for player {player_id}")
        
        # NBA.com headshot URL format - 1040x760 is the high resolution version
        headshot_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
        
        return headshot_url


class SupabaseUploader:
    """Class to handle uploading data to Supabase"""
    
    def __init__(self, client: Client):
        """
        Initialize the Supabase uploader
        
        Args:
            client: Initialized Supabase client
        """
        self.client = client
    
    def store_player_basic_info(self, player_data: Dict[str, Any], headshot_url: str) -> bool:
        """
        Store basic player information in Supabase
        
        Args:
            player_data: Player data dictionary from API
            headshot_url: URL to the player's headshot image
            
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
                "headshot_url": headshot_url,
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
            logger.error(f"Exception storing basic info for {player_name}: {e}")
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
            logger.error(f"Exception storing career stats for player {player_id}: {e}")
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
    # Check for Supabase credentials
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Supabase URL or API key not found. Set SUPABASE_URL and SUPABASE_KEY environment variables.")
        return
    
    # Load player names from players.txt instead of player IDs
    player_names = load_player_names()
    
    if not player_names:
        logger.error("No player names loaded. Please check your players.txt file.")
        return
    
    # Initialize classes
    fetcher = NBADataFetcher(rate_limit_wait=1.2, max_retries=3)
    uploader = SupabaseUploader(supabase)
    
    # Process each player
    total_players = len(player_names)
    success_count = 0
    fail_count = 0
    
    logger.info(f"Starting to process {total_players} players...")
    
    # First, preload all players to optimize searching
    all_players = fetcher.fetch_all_players()
    if not all_players:
        logger.warning("Could not preload player list, will attempt individual lookups")
    
    for i, player_name in enumerate(player_names):
        logger.info(f"Processing player {i+1}/{total_players}: {player_name}...")
        
        # First, search for the player to get their ID
        player_id = fetcher.search_player_by_name(player_name)
        
        if not player_id:
            logger.error(f"Could not find player ID for: {player_name}")
            fail_count += 1
            continue
        
        # Fetch basic info for the player
        basic_info = fetcher.fetch_player_info(player_id)
        
        if not basic_info:
            logger.error(f"Could not fetch basic info for player: {player_name} (ID: {player_id})")
            fail_count += 1
            continue
        
        # Generate headshot URL
        headshot_url = fetcher.get_player_headshot_url(player_id)
        
        # Store basic info with headshot URL
        if not uploader.store_player_basic_info(basic_info, headshot_url):
            logger.error(f"Failed to store basic info for player: {player_name}")
            fail_count += 1
            continue
        
        # Fetch and store career stats
        career_stats = fetcher.fetch_player_career_stats(player_id)
        if career_stats:
            if not uploader.store_player_career_stats(player_id, career_stats):
                logger.warning(f"Failed to store career stats for player: {player_name}")
                # Not counting as a full failure since we got the basic info
        else:
            logger.warning(f"No career stats available for player: {player_name}")
            # Not counting as a failure, but it's unusual
        
        success_count += 1
        logger.info(f"Successfully processed player: {player_name}")
    
    # Processing completed
    logger.info("=" * 50)
    logger.info(f"Processing complete. Successfully processed {success_count} of {total_players} players.")
    logger.info(f"Failed to process {fail_count} players.")


if __name__ == "__main__":
    main()