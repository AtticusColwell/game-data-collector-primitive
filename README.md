# NBA Player Data Scraper

A Python tool for collecting NBA player game logs across multiple seasons (2022-23 through 2010-11).

## Requirements
- Python 3.6+
- nba_api
- pandas
- tqdm

## Setup
```bash
# Create virtual environment
python3 -m venv nba_stats_env

# Activate virtual environment
source nba_stats_env/bin/activate

# Install dependencies
pip install nba_api tqdm pandas
```

## Usage
```bash
python fetch_game_logs.py all_players_by_season.txt --max_workers 1 --rate_limit 1.5
```

Parameters:
- `--max_workers`: Number of concurrent threads (default: 6)
- `--rate_limit`: Seconds to wait between API requests (default: 0.75)
- `--outdir`: Output directory (default: ./player_logs)

## Output
Game logs are saved to `./player_logs/{season}/` as CSV files.