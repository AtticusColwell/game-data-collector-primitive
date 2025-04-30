
#!/usr/bin/env python3
"""
fetch_game_logs.py
------------------
Given a text file exported by your roster‑scraper (one season header, then
player names), pull every player's game log for that season using nba_api
and save them to individual CSVs in ./<season>/.

Usage
-----
$ pip install nba_api tqdm pandas
$ python fetch_game_logs.py all_players_by_season.txt               --max_workers 6 --rate_limit 0.75

Arguments
---------
positional:
    roster_file   Path to the text file generated earlier.

optional:
    --outdir      Root output directory (default = ./player_logs)
    --max_workers Concurrent threads (default 6).
    --rate_limit  Seconds to sleep **after** every request in each thread
                  (default 0.75 → approx 6‑7 req/s total).

The script prints a season‑level progress bar so you can watch it advance.
"""

import argparse, time, re, os, sys, unicodedata
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import Dict, List
import pandas as pd
from tqdm.auto import tqdm

from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog


# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #
def slugify(name: str) -> str:
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    return re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_')

def parse_roster_txt(path: Path) -> Dict[str, List[str]]:
    """Return {season: [player, ...]} dict"""
    seasons = {}
    current_season = None
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line.startswith('Season:'):
                current_season = line.split('Season:')[1].strip()
                seasons[current_season] = []
            elif line and not line.startswith('='):
                if current_season is None:
                    continue
                seasons[current_season].append(line)
    return seasons

def find_player_id(name: str) -> int | None:
    hits = players.find_players_by_full_name(name)
    if not hits:
        return None
    # perfect case match first
    for h in hits:
        if h['full_name'].lower() == name.lower():
            return h['id']
    return hits[0]['id']

def fetch_save(player_name: str, season: str, out_dir: Path,
               sleep_seconds: float) -> tuple[str, bool, str]:
    """Return (name, success, msg)"""
    time.sleep(sleep_seconds + random.random()*0.3)
    pid = find_player_id(player_name)
    if pid is None:
        return player_name, False, 'Player ID not found'
    try:
        df = playergamelog.PlayerGameLog(
            player_id=pid,
            season=season,
            timeout=3          # <-- explicit timeout (seconds)
        ).get_data_frames()[0]
        if df.empty:
            return player_name, False, 'Empty log'
        fname = out_dir / f"{slugify(player_name)}.csv"
        df.to_csv(fname, index=False)
        time.sleep(sleep_seconds)
        return player_name, True, ''
    except Exception as e:
        time.sleep(sleep_seconds)
        return player_name, False, str(e)

# --------------------------------------------------------------------- #
# Main routine
# --------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('roster_file', type=Path)
    ap.add_argument('--outdir', type=Path, default=Path('player_logs'))
    ap.add_argument('--max_workers', type=int, default=6)
    ap.add_argument('--rate_limit', type=float, default=0.75)
    args = ap.parse_args()

    seasons = parse_roster_txt(args.roster_file)
    root = args.outdir
    root.mkdir(parents=True, exist_ok=True)

    # Define our target seasons in the desired order
    target_seasons = []
    for year in range(2022, 2009, -1):  # Start with 2022, go backward to 2010
        season_str = f"{year}-{str(year+1)[-2:]}"
        if season_str in seasons:
            target_seasons.append(season_str)
    
    for season in target_seasons:
        roster = seasons[season]
        season_dir = root / season
        season_dir.mkdir(exist_ok=True)
        desc = f"{season} ({len(roster)} players)"
        with ThreadPoolExecutor(max_workers=args.max_workers) as exe:
            futures = [exe.submit(fetch_save, name, season,
                                  season_dir, args.rate_limit)
                       for name in roster]
            for _ in tqdm(as_completed(futures), total=len(futures), desc=desc):
                pass  # progress bar updates as futures complete

        # simple report
        failures = [f.result() for f in futures if not f.result()[1]]
        if failures:
            print(f"\n[!] {len(failures)} failures in {season}:")
            for name, _, msg in failures:
                print(f"   - {name}: {msg}")

    print('\n✓ Game logs saved under', root.resolve())

if __name__ == '__main__':
    main()
