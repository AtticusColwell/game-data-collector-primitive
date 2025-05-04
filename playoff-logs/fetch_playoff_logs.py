#!/usr/bin/env python3
"""
fetch_playoff_logs.py
---------------------
• Downloads **playoff** game logs for every player listed in `roster.txt`.
• One CSV per player is saved to  ./playoff_logs/<Season>/<slug>.csv
• Skips files that already exist, retries once on time-out, and writes a
  central `failed_playoff_logs.txt` with any misses.
• Processes seasons BACKWARD from 2024-2025 to 2015-2016

USAGE
-----
python fetch_playoff_logs.py roster.txt \
       --outdir playoff_logs --max_workers 8 --rate_limit 1.0 --timeout 6
"""

import argparse, random, re, time, unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
from requests.exceptions import ReadTimeout, Timeout
from tqdm.auto import tqdm


# ───────────────────────────── helpers ────────────────────────────────
def slugify(name: str) -> str:
    cleaned = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Za-z0-9]+", "_", cleaned).strip("_")


def parse_roster_txt(path: Path) -> Dict[str, List[str]]:
    """Return {'1995-96': [player1, player2, …], …}"""
    seasons, current = {}, None
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if ln.startswith("Season:"):
            current = ln.split("Season:")[1].strip()
            seasons[current] = []
        elif ln and not ln.startswith("=") and current:
            seasons[current].append(ln)
    return seasons


def find_player_id(full_name: str) -> Optional[int]:
    hits = players.find_players_by_full_name(full_name)
    for h in hits:
        if h["full_name"].lower() == full_name.lower():
            return h["id"]
    return hits[0]["id"] if hits else None


# ──────────────────────────── worker ──────────────────────────────────
def fetch_and_save(
    player_name: str,
    season: str,
    out_dir: Path,
    sleep_s: float,
    timeout_s: int,
) -> Tuple[str, bool, str]:
    """
    Return (player_name, success_flag, status_string)
    status ∈ {'ok','empty','already','timeout','no_id','error:<msg>'}
    """
    csv_path = out_dir / f"{slugify(player_name)}.csv"
    if csv_path.exists():
        return player_name, True, "already"

    # polite rate-limit
    time.sleep(sleep_s + random.random() * 0.3)

    pid = find_player_id(player_name)
    if pid is None:
        return player_name, False, "no_id"

    for attempt in (1, 2):  # one retry
        try:
            df = playergamelog.PlayerGameLog(
                player_id=pid,
                season=season,
                season_type_all_star="Playoffs",
                timeout=timeout_s,
            ).get_data_frames()[0]

            if df.empty:
                return player_name, False, "empty"

            df.to_csv(csv_path, index=False)
            return player_name, True, "ok"

        except (ReadTimeout, Timeout):
            if attempt == 1:
                time.sleep(60)
                continue
            return player_name, False, "timeout"

        except Exception as e:
            return player_name, False, f"error:{e}"

    return player_name, False, "unknown"


# ───────────────────────────── main ───────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("roster_file", type=Path)
    ap.add_argument("--outdir", type=Path, default=Path("playoff_logs"))
    ap.add_argument("--max_workers", type=int, default=4)
    ap.add_argument("--rate_limit", type=float, default=1.0,
                    help="seconds to sleep between requests per thread")
    ap.add_argument("--timeout", type=int, default=6,
                    help="HTTP timeout (seconds) for nba_api calls")
    args = ap.parse_args()

    seasons = parse_roster_txt(args.roster_file)
    args.outdir.mkdir(parents=True, exist_ok=True)
    fail_log_path = args.outdir / "failed_playoff_logs.txt"
    fail_log = fail_log_path.open("a", encoding="utf-8")

    # Get all seasons and sort them in descending order (backward from most recent)
    all_seasons = list(seasons.keys())
    
    # Filter to only seasons from 2015-16 to 2024-25
    filtered_seasons = []
    for season in all_seasons:
        # Extract the starting year from the season string (e.g., "2024-25" -> 2024)
        try:
            year = int(season.split('-')[0])
            if 2015 <= year <= 2024:
                filtered_seasons.append(season)
        except ValueError:
            continue
    
    # Sort seasons in descending order
    filtered_seasons.sort(key=lambda s: int(s.split('-')[0]), reverse=True)
    
    # Process seasons in backward order
    for season in filtered_seasons:
        if season not in seasons:
            continue
            
        roster = seasons[season]
        season_dir = args.outdir / season
        season_dir.mkdir(exist_ok=True)

        desc = f"{season} playoffs ({len(roster)} players)"
        with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
            futs = [
                pool.submit(
                    fetch_and_save,
                    name,
                    season,
                    season_dir,
                    args.rate_limit,
                    args.timeout,
                )
                for name in roster
            ]

            for fut in tqdm(as_completed(futs), total=len(futs), desc=desc):
                player, ok, status = fut.result()
                if not ok and status not in ("already",):
                    fail_log.write(f"{season}\t{player}\t{status}\n")

    fail_log.close()
    print("\n✓ Playoff logs saved under", args.outdir.resolve())
    print("⚠️  Any failures recorded in", fail_log_path.resolve())


if __name__ == "__main__":
    main()