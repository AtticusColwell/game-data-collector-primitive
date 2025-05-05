#!/usr/bin/env python3
"""
fetch_player_bio.py
-------------------
Fetch biographical / draft data for every player in roster.txt.

Outputs
-------
player_bio_master.csv               – one row per player (all seasons merged)
player_bios/<slug>.json             – raw CommonPlayerInfo JSON  (optional)
player_bios/failed_bio.txt          – season, name, reason

Example
-------
python fetch_player_bio.py roster.txt --outdir player_bios --threads 8
"""

import argparse, json, random, re, time, unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.static import players
from requests.exceptions import ReadTimeout, Timeout
from tqdm.auto import tqdm
from unidecode import unidecode

# ---------------- parameters ----------------------------------------- #
DELAY_BASE  = 1.0         # polite sleep per thread (seconds)
DELAY_JIT   = 0.5         # extra jitter
TIMEOUT_S   = 6
HEADERS     = {"User-Agent": "bio-scraper/0.1"}
# --------------------------------------------------------------------- #


# ------------------------- helpers ----------------------------------- #
def slugify(name: str) -> str:
    clean = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Za-z0-9]+", "_", clean).strip("_")


def parse_roster_txt(path: Path) -> List[str]:
    names = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if ln and not ln.startswith(("=", "Season:")):
            names.append(ln)
    return sorted(set(names))


def player_id_from_name(full_name: str) -> Optional[int]:
    hits = players.find_players_by_full_name(full_name)
    for h in hits:
        if h["full_name"].lower() == full_name.lower():
            return h["id"]
    return hits[0]["id"] if hits else None


def safe_get(ci_row: pd.Series, field: str):
    return ci_row.get(field, "")


# ------------------------- worker ------------------------------------ #
def fetch_bio(
    name: str,
    outdir: Path,
) -> Tuple[str, bool, str, Dict[str, str]]:
    """
    Return (player_name, success_flag, status, dict_of_fields)

    status ∈  {'ok','no_id','timeout','error:<msg>'}
    """
    pid = player_id_from_name(name)
    if pid is None:
        return name, False, "no_id", {}

    # polite rate-limit
    time.sleep(DELAY_BASE + random.random() * DELAY_JIT)

    try:
        ci = commonplayerinfo.CommonPlayerInfo(player_id=pid, timeout=TIMEOUT_S,
                                               headers=HEADERS)
        df = ci.get_data_frames()[0]        # always single row
        row = df.iloc[0]

        bio = {
            "Player":          name,
            "Player_ID":       pid,
            "Birthdate":       safe_get(row, "BIRTHDATE"),
            "Country":         safe_get(row, "COUNTRY"),
            "Height":          safe_get(row, "HEIGHT"),   # e.g. 6-10
            "Weight_lbs":      safe_get(row, "WEIGHT"),
            "Position":        safe_get(row, "POSITION"),
            "College":         safe_get(row, "SCHOOL"),
            "Draft_Year":      safe_get(row, "DRAFT_YEAR"),
            "Draft_Round":     safe_get(row, "DRAFT_ROUND"),
            "Draft_Number":    safe_get(row, "DRAFT_NUMBER"),
            "Draft_Team":      safe_get(row, "DRAFT_TEAM_ID"),
            "Shoot_Hand":      safe_get(row, "HAND"),
        }

        # save raw json (optional)
        (outdir / "raw_json").mkdir(exist_ok=True)
        with (outdir / "raw_json" / f"{slugify(name)}.json").open("w") as f:
            json.dump(ci.get_normalized_json(), f)

        return name, True, "ok", bio

    except (ReadTimeout, Timeout):
        return name, False, "timeout", {}
    except Exception as e:
        return name, False, f"error:{e}", {}


# ---------------------------- main ----------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("roster_file", type=Path)
    ap.add_argument("--outdir", type=Path, default=Path("player_bios"))
    ap.add_argument("--threads", type=int, default=8)
    args = ap.parse_args()

    names = parse_roster_txt(args.roster_file)
    args.outdir.mkdir(parents=True, exist_ok=True)
    fail_log = (args.outdir / "failed_bio.txt").open("a", encoding="utf-8")

    results = []
    with ThreadPoolExecutor(max_workers=args.threads) as pool, \
            tqdm(total=len(names), unit="player") as bar:

        fut_to_name = {
            pool.submit(fetch_bio, n, args.outdir): n
            for n in names
        }
        for fut in as_completed(fut_to_name):
            n, ok, status, bio = fut.result()
            if ok:
                results.append(bio)
            else:
                fail_log.write(f"{n}\t{status}\n")
            bar.update(1)

    fail_log.close()

    if results:
        pd.DataFrame(results).to_csv(args.outdir / "player_bio_master.csv",
                                     index=False)

    print(f"\n✓ Bio rows saved: {len(results)}   "
          f"|  failures logged: {sum(1 for _ in open(fail_log.name))}")


if __name__ == "__main__":
    main()
