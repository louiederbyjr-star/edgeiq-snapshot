"""
EdgeIQ — Hourly Odds Snapshot
Runs every hour via Railway cron job.
Saves current odds for all games to Supabase so we can
track real line movement over time.

Uses only: requests, python-dotenv
"""

import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

ODDS_API_KEY = os.environ["ODDS_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

SPORTS = [
    "basketball_nba",
    "americanfootball_nfl",
    "baseball_mlb",
    "icehockey_nhl",
]

# ── Supabase helpers ──────────────────────────────────────────────────

def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def sb_insert(table, record):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=sb_headers(),
        json=record
    )
    if r.status_code not in (200, 201):
        print(f"  Warning: insert {r.status_code}: {r.text}")

# ── helpers ───────────────────────────────────────────────────────────

def safe_avg(values):
    return sum(values) / len(values) if values else 0

# ── main ──────────────────────────────────────────────────────────────

def take_snapshot():
    now       = datetime.now(timezone.utc)
    snapshot_time = now.isoformat()
    today     = now.date().isoformat()
    total     = 0

    for sport in SPORTS:
        print(f"Snapshotting {sport}...")
        try:
            resp = requests.get(
                f"https://api.the-odds-api.com/v4/sports/{sport}/odds/",
                params={
                    "apiKey":     ODDS_API_KEY,
                    "regions":    "us",
                    "markets":    "spreads,h2h",
                    "oddsFormat": "american",
                },
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            print(f"  Network error: {e}")
            continue

        if resp.status_code == 401:
            print("  ERROR: Invalid ODDS_API_KEY")
            return
        if resp.status_code == 422:
            print(f"  {sport} out of season, skipping.")
            continue
        if resp.status_code != 200:
            print(f"  Error {resp.status_code}: {resp.text}")
            continue

        games = resp.json()
        print(f"  {len(games)} games found")

        for game in games:
            bookmakers = game.get("bookmakers", [])
            if not bookmakers:
                continue

            # collect odds per team across all books
            juices  = {}
            spreads = {}

            for book in bookmakers:
                for market in book.get("markets", []):
                    mkey = market["key"]
                    if mkey not in ("spreads", "h2h"):
                        continue
                    for outcome in market["outcomes"]:
                        name  = outcome["name"]
                        price = int(outcome.get("price", -110))
                        point = outcome.get("point", 0)
                        juices.setdefault(name, []).append(price)
                        if mkey == "spreads":
                            spreads.setdefault(name, []).append(point)

            # save one snapshot row per team per game
            for team, juice_list in juices.items():
                avg_juice  = safe_avg(juice_list)
                avg_spread = safe_avg(spreads.get(team, [0]))

                # skip invalid odds values
                if avg_juice == 0 or -99 < avg_juice < 99:
                    continue

                record = {
                    "snapshot_time": snapshot_time,
                    "snap_date":     today,
                    "game_id":       game["id"],
                    "sport":         game["sport_key"],
                    "home_team":     game["home_team"],
                    "away_team":     game["away_team"],
                    "team":          team,
                    "avg_juice":     round(avg_juice, 1),
                    "avg_spread":    round(avg_spread, 2),
                    "num_books":     len(bookmakers),
                    "game_time":     game["commence_time"],
                }
                sb_insert("odds_snapshots", record)
                total += 1

    print(f"\n✅ Snapshot complete — {total} rows saved at {snapshot_time}")

if __name__ == "__main__":
    take_snapshot()
