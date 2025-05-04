# NBA Player Data Scraper

A Python tool for collecting NBA player game logs across multiple seasons - playing around with this for Scout DB.

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

## Scripts

### Regular Season Game Logs
```bash
python fetch_game_logs.py all_players_by_season.txt --max_workers 1 --rate_limit 1.5
```

Parameters:
- `--max_workers`: Number of concurrent threads (default: 6)
- `--rate_limit`: Seconds to wait between API requests (default: 0.75)
- `--outdir`: Output directory (default: ./player_logs)

### Playoff Game Logs
```bash
python fetch_playoff_logs.py
```

This script fetches playoff game logs for players. Output is saved to `./playoff_logs/{season}/`.

### Supabase Upload
```bash
# First, set up environment variables in .env
cp supabase-upload/.env.example supabase-upload/.env
# Edit .env with your Supabase credentials

# Then run the upload script
python supabase-upload/nba_player_upload.py
```

This script uploads the collected data to a Supabase database. Make sure to set up your Supabase credentials in the `.env` file first.

## Output
- Regular season game logs: `./player_logs/{season}/`
- Playoff game logs: `./playoff_logs/{season}/`