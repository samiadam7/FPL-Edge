"""
Fantasy Premier League (FPL) API Data Fetcher

This module provides functions to fetch data from the Fantasy Premier League API endpoints.
It handles rate limiting, connection errors, and JSON parsing with proper error handling.
"""

import json
import requests
import time
from typing import Dict, Any, Optional
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FPLRequestError(Exception):
    """Raised when there's an error making requests to the FPL API."""
    pass

class FPLDataParsingError(Exception):
    """Raised when there's an error parsing the FPL API response."""
    pass

def make_fpl_request(url: str, max_retries: int = 3, retry_delay: int = 5) -> Dict[str, Any]:
    """
    Helper function to make requests to FPL API with retry logic.

    Args:
        url (str): The API endpoint URL
        max_retries (int, optional): Maximum number of retry attempts. Defaults to 3.
        retry_delay (int, optional): Delay between retries in seconds. Defaults to 5.

    Returns:
        Dict[str, Any]: Parsed JSON response from the API

    Raises:
        FPLRequestError: If request fails after all retries
        FPLDataParsingError: If response parsing fails
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers={'User-Agent': 'Mozilla/5.0'}  # Add user agent to prevent blocking
            )
            response.raise_for_status()  # Raises HTTPError for bad status codes
            
            return response.json()  # Uses .json() instead of json.loads()
            
        except HTTPError as e:
            if response.status_code == 429:  # Rate limit exceeded
                retry_delay = int(response.headers.get('Retry-After', retry_delay))
            raise FPLRequestError(f"HTTP {response.status_code} error: {str(e)}")
            
        except ConnectionError as e:
            if attempt == max_retries - 1:
                raise FPLRequestError(f"Connection failed after {max_retries} attempts: {str(e)}")
            
        except Timeout as e:
            if attempt == max_retries - 1:
                raise FPLRequestError(f"Request timed out after {max_retries} attempts: {str(e)}")
            
        except json.JSONDecodeError as e:
            raise FPLDataParsingError(f"Failed to parse API response: {str(e)}")
            
        except Exception as e:
            raise FPLRequestError(f"Unexpected error: {str(e)}")
        
        # Wait before retrying
        time.sleep(retry_delay)
        print(f"Retrying request (attempt {attempt + 2}/{max_retries})...")

def get_data() -> Dict[str, Any]:
    """
    Fetch general FPL data including players, teams, and game rules.

    Returns:
        Dict[str, Any]: Dictionary containing FPL bootstrap-static data

    Raises:
        FPLRequestError: If the API request fails
        FPLDataParsingError: If response parsing fails
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    return make_fpl_request(url)

def get_fixture_data() -> Dict[str, Any]:
    """
    Fetch FPL fixture data for all matches.

    Returns:
        Dict[str, Any]: Dictionary containing fixture data

    Raises:
        FPLRequestError: If the API request fails
        FPLDataParsingError: If response parsing fails
    """
    url = "https://fantasy.premierleague.com/api/fixtures/"
    return make_fpl_request(url)

def get_individual_data(player_id: int) -> Dict[str, Any]:
    """
    Fetch detailed data for a specific player.

    Args:
        player_id (int): FPL ID of the player

    Returns:
        Dict[str, Any]: Dictionary containing player-specific data

    Raises:
        FPLRequestError: If the API request fails
        FPLDataParsingError: If response parsing fails
        ValueError: If player_id is invalid
    """
    # Validate player_id
    if not isinstance(player_id, int) or player_id < 1:
        raise ValueError("player_id must be a positive integer")

    base_url = "https://fantasy.premierleague.com/api/element-summary/"
    url = f"{base_url}{player_id}/"
    return make_fpl_request(url)

def main():
    """
    Main function to orchestrate the data collection process.
    """
    
    try:
        # Fetch general FPL data
        fpl_data = get_data()
        print("Successfully fetched FPL data")

        # Fetch fixture data
        fixtures = get_fixture_data()
        print("Successfully fetched fixture data")

        # Fetch data for player with ID 1
        player_data = get_individual_data(1)
        print("Successfully fetched player data")
        
        return 0
    
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        return 1
    
    
if __name__ == "__main__":
    exit(main())