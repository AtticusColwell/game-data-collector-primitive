"""
NBA Player Data Fetcher and Supabase Uploader

This script:
1. Reads a list of player names from players.txt
2. Fetches player IDs using the NBA API's search endpoint
3. Retrieves detailed player data from various NBA API endpoints
4. Stores the data in a structured Supabase database
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
PLAYER_SEARCH_ENDPOINT = "/searchplayers"
PLAYER_INFO_ENDPOINT = "/commonplayerinfo"
PLAYER_STATS_ENDPOINT = "/playerdashboardbygeneralsplits"
PLAYER_CAREER_ENDPOINT = "/playercareerstats"
PLAYER_PROFILE_ENDPOINT = "/playerprofilev2"

# Constants
CURRENT_SEASON = "2024-25"  # Update as needed for the current season


class NBADataFetcher:
    """Class to handle fetching data from the NBA API"""

    def __init__(self, rate_limit_wait: float = 1.5):
        """
        Initialize the NBA data fetcher

        Args:
            rate_limit_wait: Time to wait between API calls in seconds to avoid rate limiting
        """
        self.rate_limit_wait = rate_limit_wait

    def _make_api_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Make a request to the NBA API with rate limiting

        Args:
            endpoint: The API endpoint
            params: Query parameters for the request

        Returns:
            The JSON response or None if the request failed
        """
        url = f"{NBA_API_BASE_URL}{endpoint}"

        # Always wait BEFORE making the request to ensure proper spacing between requests
        # This is critical for the NBA API which has strict rate limiting
        time.sleep(self.rate_limit_wait)
        
        try:
            logger.debug(f"Making API request to {url} with params {params}")
            response = requests.get(url, headers=NBA_API_HEADERS, params=params, timeout=15)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making API request to {endpoint}: {e}")
            # If we get a 429 (Too Many Requests), wait longer before retrying
            if hasattr(e, 'response') and e.response and e.response.status_code == 429:
                logger.warning("Rate limited. Waiting 10 seconds before retrying...")
                time.sleep(10)
                try:
                    # Wait again before the retry
                    time.sleep(self.rate_limit_wait)
                    response = requests.get(url, headers=NBA_API_HEADERS, params=params, timeout=15)
                    response.raise_for_status()
                    return response.json()
                except requests.exceptions.RequestException as retry_e:
                    logger.error(f"Retry failed: {retry_e}")
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

        params = {
            "SearchCriteria": player_name
        }

        response = self._make_api_request(PLAYER_SEARCH_ENDPOINT, params)

        if not response:
            return None

        try:
            result_sets = response.get("resultSets", [])
            if not result_sets or not result_sets[0].get("rowSet"):
                logger.warning(f"No search results found for player: {player_name}")
                return None

            # Get all player matches
            headers = result_sets[0]["headers"]
            player_id_index = headers.index("PERSON_ID")
            display_name_index = headers.index("DISPLAY_FIRST_LAST")
            is_active_index = headers.index("IS_ACTIVE")
            
            # Filter for active players that match the name exactly
            for row in result_sets[0]["rowSet"]:
                if row[is_active_index] == 1 and row[display_name_index].lower() == player_name.lower():
                    player_id = row[player_id_index]
                    logger.info(f"Found exact match for {player_name}: ID {player_id}")
                    return player_id
            
            # If no exact active match, try partial matches
            for row in result_sets[0]["rowSet"]:
                if row[is_active_index] == 1 and player_name.lower() in row[display_name_index].lower():
                    player_id = row[player_id_index]
                    logger.info(f"Found partial match for {player_name}: {row[display_name_index]} (ID {player_id})")
                    return player_id

            logger.warning(f"No active player found for: {player_name}")
            return None

        except (KeyError, IndexError, ValueError) as e:
            logger.error(f"Error parsing search results for {player_name}: {e}")
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

        response = self._make_api_request(PLAYER_STATS_ENDPOINT, params)

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

        response = self._make_api_request(PLAYER_PROFILE_ENDPOINT, params)

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

            # Upsert the player data (insert or update if exists)
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

            # Upsert the stats data
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
            logger.error(f"Exception storing current stats for player {player_id}: {e}")
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

            # Upsert the career stats data
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

            # Upsert the season highs data
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
            logger.error(f"Exception storing season highs for player {player_id}: {e}")
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
    fetcher = NBADataFetcher(rate_limit_wait=1.5)  # Strict 1.5 second rate limit for NBA API
    uploader = SupabaseUploader(supabase)
    
    # Print rate limit warning
    logger.info("=== NBA API RATE LIMIT WARNING ===")
    logger.info("Using strict 1.5 second delay between API requests to avoid rate limiting")
    logger.info(f"Estimated processing time for {len(player_names)} players: " + 
                f"approximately {len(player_names) * 4 * 1.5 / 60:.1f} minutes minimum " +
                f"(4 API calls per player with 1.5s delay)")
    logger.info("=============================")
    
    # Process each player
    total_players = len(player_names)
    success_count = 0
    fail_count = 0
    players_processed = []
    
    logger.info(f"Starting to process {total_players} players...")
    
    for i, player_name in enumerate(player_names):
        logger.info(f"Processing player {i+1}/{total_players}: {player_name}...")
        
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
            
        # Fetch and store current season stats
        current_stats = fetcher.fetch_player_stats(player_id)
        if current_stats:
            uploader.store_player_current_stats(player_id, current_stats)
        else:
            logger.warning(f"No current season stats for player: {player_name}")
        
        # Fetch and store career stats
        career_stats = fetcher.fetch_player_career_stats(player_id)
        if career_stats:
            uploader.store_player_career_stats(player_id, career_stats)
        else:
            logger.warning(f"No career stats for player: {player_name}")
        
        # Fetch and store season highs
        season_highs = fetcher.fetch_player_season_highs(player_id)
        if season_highs:
            uploader.store_player_season_highs(player_id, season_highs)
        else:
            logger.warning(f"No season highs for player: {player_name}")
        
        # Record successful processing
        players_processed.append({
            "name": player_name,
            "id": player_id,
            "full_name": basic_info.get("DISPLAY_FIRST_LAST", player_name)
        })
        success_count += 1
        
        logger.info(f"Successfully processed player: {player_name}")
    
    # Print summary
    logger.info(f"Processing complete. Successfully processed {success_count} of {total_players} players.")
    logger.info(f"Failed to process {fail_count} players.")
    
    # Save the list of processed players for reference
    with open("processed_players.json", "w") as f:
        json.dump(players_processed, f, indent=2)
    
    logger.info(f"Saved list of processed players to processed_players.json")


if __name__ == "__main__":
    main()