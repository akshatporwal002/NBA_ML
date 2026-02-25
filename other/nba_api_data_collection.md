# NBA API Data Collection

This repo contains a single script, `collect_nba_api_data.py`, that downloads the data points referenced in the four models described in `explore.pdf` for the last 10 seasons (by default). The script uses `nba_api` endpoints to collect team, player, lineup, and matchup data.

## Requirements

- Python 3.9+
- `nba_api`
- `pandas`

Example install:

```bash
pip install nba_api pandas
```

## Usage

```bash
python collect_nba_api_data.py \
  --past-seasons 10 \
  --output-dir data/nba_api \
  --format csv
```

### Optional flags

- `--start-year` / `--end-year`: Override the season range (start years).
- `--player-scope`: `all` (default), `active`, or `sample` for faster collection.
- `--skip-player-game-logs`: Skip per-player game logs.
- `--skip-player-shots`: Skip per-player shot charts.
- `--skip-player-on-off`: Skip per-player on/off summaries.
- `--skip-team-shot-charts`: Skip per-team shot charts.
- `--skip-matchups`: Skip box score matchups (per game).

## Output structure

```
data/nba_api/
  2014-15/
    model_1_team_playstyle/
    model_2_player/
    model_3_lineup_synergy/
    model_4_matchup_advantage/
  ...
```

Each subdirectory contains CSV or Parquet files for the relevant endpoints. The script sleeps between requests to respect NBA API rate limits.
