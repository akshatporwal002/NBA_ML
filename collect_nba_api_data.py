#!/usr/bin/env python
"""Collect NBA API data for the feature sets described in the explore.pdf models."""
from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path

import pandas as pd
from nba_api.stats import endpoints
from nba_api.stats.static import players, teams


DEFAULT_RATE_LIMIT_SECONDS = 0.6


MODEL_1_ENDPOINTS = {
    "team_stats_advanced": (endpoints.LeagueDashTeamStats, {"measure_type_detailed_defense": "Advanced"}),
    "team_stats_opponent": (endpoints.LeagueDashTeamStats, {"measure_type_detailed_defense": "Opponent"}),
    "team_stats_four_factors": (endpoints.LeagueDashTeamStats, {"measure_type_detailed_defense": "Four Factors"}),
    "pt_stats_speed_distance": (endpoints.LeagueDashPtStats, {"pt_measure_type": "SpeedDistance"}),
    "pt_stats_possessions": (endpoints.LeagueDashPtStats, {"pt_measure_type": "Possessions"}),
    "team_pt_shot": (endpoints.LeagueDashTeamPtShot, {}),
    "pt_stats_passing": (endpoints.LeagueDashPtStats, {"pt_measure_type": "Passing"}),
    "pt_stats_rebounding": (endpoints.LeagueDashPtStats, {"pt_measure_type": "Rebounding"}),
    "pt_team_defend": (endpoints.LeagueDashPtTeamDefend, {}),
    "hustle_stats_team": (endpoints.LeagueHustleStatsTeam, {}),
    "team_clutch": (endpoints.LeagueDashTeamClutch, {}),
    "synergy_play_types": (endpoints.SynergyPlayTypes, {}),
    "league_standings": (endpoints.LeagueStandingsV3, {}),
    "team_splits_general": (endpoints.TeamDashboardByGeneralSplits, {}),
    "league_lineups_5man": (endpoints.LeagueDashLineups, {"group_quantity": 5}),
    "team_lineups_5man": (endpoints.TeamDashLineups, {"group_quantity": 5}),
}

MODEL_2_ENDPOINTS = {
    "player_bio_stats": (endpoints.LeagueDashPlayerBioStats, {}),
    "player_stats_base": (endpoints.LeagueDashPlayerStats, {"measure_type_detailed_defense": "Base"}),
    "player_stats_advanced": (endpoints.LeagueDashPlayerStats, {"measure_type_detailed_defense": "Advanced"}),
    "player_estimated_metrics": (endpoints.PlayerEstimatedMetrics, {}),
    "hustle_stats_player": (endpoints.LeagueHustleStatsPlayer, {}),
    "player_clutch": (endpoints.LeagueDashPlayerClutch, {}),
    "player_pt_stats_passing": (endpoints.LeagueDashPtStats, {"pt_measure_type": "Passing", "player_or_team": "Player"}),
    "player_pt_stats_possessions": (endpoints.LeagueDashPtStats, {"pt_measure_type": "Possessions", "player_or_team": "Player"}),
    "player_pt_stats_speed_distance": (endpoints.LeagueDashPtStats, {"pt_measure_type": "SpeedDistance", "player_or_team": "Player"}),
}

MODEL_3_ENDPOINTS = {
    "league_lineups_5man": (endpoints.LeagueDashLineups, {"group_quantity": 5}),
    "team_lineups_5man": (endpoints.TeamDashLineups, {"group_quantity": 5}),
    "team_player_on_off_details": (endpoints.TeamPlayerOnOffDetails, {}),
}

MODEL_4_ENDPOINTS = {
    "league_season_matchups": (endpoints.LeagueSeasonMatchups, {}),
    "pt_team_defend": (endpoints.LeagueDashPtTeamDefend, {}),
}


def season_str(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def resolve_seasons(past_seasons: int, start_year: int | None, end_year: int | None) -> list[str]:
    if start_year is not None and end_year is not None:
        return [season_str(year) for year in range(start_year, end_year + 1)]

    today = dt.date.today()
    end_start_year = today.year - 1
    start_start_year = end_start_year - (past_seasons - 1)
    return [season_str(year) for year in range(start_start_year, end_start_year + 1)]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_frames(frames: list[pd.DataFrame], out_dir: Path, name: str, fmt: str) -> None:
    for idx, frame in enumerate(frames, start=1):
        suffix = f"_{idx}" if len(frames) > 1 else ""
        file_path = out_dir / f"{name}{suffix}.{fmt}"
        if fmt == "csv":
            frame.to_csv(file_path, index=False)
        else:
            try:
                frame.to_parquet(file_path, index=False)
            except ImportError:
                fallback = file_path.with_suffix(".csv")
                frame.to_csv(fallback, index=False)


def call_endpoint(endpoint_cls, out_dir: Path, name: str, sleep_s: float, **params) -> None:
    response = endpoint_cls(**params)
    frames = response.get_data_frames()
    save_frames(frames, out_dir, name, params.get("output_format", "csv"))
    time.sleep(sleep_s)


def collect_model_endpoints(
    endpoints_map: dict[str, tuple[type, dict]],
    season: str,
    out_dir: Path,
    season_type: str,
    per_mode: str,
    sleep_s: float,
    fmt: str,
) -> None:
    for name, (endpoint_cls, extra_params) in endpoints_map.items():
        params = {
            "season": season,
            "season_type_all_star": season_type,
            "per_mode_detailed": per_mode,
            "output_format": fmt,
        }
        params.update(extra_params)
        call_endpoint(endpoint_cls, out_dir, name, sleep_s, **params)


def collect_shot_charts_for_teams(season: str, out_dir: Path, sleep_s: float, fmt: str) -> None:
    team_data = teams.get_teams()
    for team in team_data:
        params = {
            "season": season,
            "team_id": team["id"],
            "player_id": 0,
            "output_format": fmt,
        }
        name = f"shot_chart_team_{team['abbreviation']}"
        call_endpoint(endpoints.ShotChartDetail, out_dir, name, sleep_s, **params)


def collect_player_specific(
    season: str,
    out_dir: Path,
    player_scope: str,
    sleep_s: float,
    fmt: str,
    include_game_logs: bool,
    include_shots: bool,
    include_on_off: bool,
) -> None:
    all_players = players.get_players()
    if player_scope == "active":
        selected = [player for player in all_players if player.get("is_active")]
    elif player_scope == "sample":
        selected = all_players[:25]
    else:
        selected = all_players

    for player in selected:
        player_id = player["id"]
        name_suffix = f"{player_id}"
        info_dir = out_dir / "player_info"
        ensure_dir(info_dir)
        call_endpoint(
            endpoints.CommonPlayerInfo,
            info_dir,
            f"common_player_info_{name_suffix}",
            sleep_s,
            player_id=player_id,
            output_format=fmt,
        )

        if include_game_logs:
            logs_dir = out_dir / "player_game_logs"
            ensure_dir(logs_dir)
            call_endpoint(
                endpoints.PlayerGameLog,
                logs_dir,
                f"player_game_log_{name_suffix}",
                sleep_s,
                player_id=player_id,
                season=season,
                output_format=fmt,
            )

        career_dir = out_dir / "player_career"
        ensure_dir(career_dir)
        call_endpoint(
            endpoints.PlayerCareerStats,
            career_dir,
            f"player_career_{name_suffix}",
            sleep_s,
            player_id=player_id,
            output_format=fmt,
        )

        if include_on_off:
            on_off_dir = out_dir / "player_on_off"
            ensure_dir(on_off_dir)
            call_endpoint(
                endpoints.TeamPlayerOnOffSummary,
                on_off_dir,
                f"player_on_off_{name_suffix}",
                sleep_s,
                player_id=player_id,
                season=season,
                output_format=fmt,
            )

        if include_shots:
            shots_dir = out_dir / "player_shots"
            ensure_dir(shots_dir)
            call_endpoint(
                endpoints.ShotChartDetail,
                shots_dir,
                f"shot_chart_player_{name_suffix}",
                sleep_s,
                player_id=player_id,
                team_id=0,
                season=season,
                output_format=fmt,
            )


def collect_matchups_from_games(season: str, out_dir: Path, sleep_s: float, fmt: str) -> None:
    log = endpoints.LeagueGameLog(season=season)
    games = log.get_data_frames()[0]
    matchup_dir = out_dir / "boxscore_matchups"
    ensure_dir(matchup_dir)
    for game_id in games["GAME_ID"].unique():
        call_endpoint(
            endpoints.BoxScoreMatchupsV3,
            matchup_dir,
            f"boxscore_matchups_{game_id}",
            sleep_s,
            game_id=game_id,
            output_format=fmt,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect NBA API data for the last 10 seasons.")
    parser.add_argument("--past-seasons", type=int, default=10, help="How many past seasons to fetch.")
    parser.add_argument("--start-year", type=int, default=None, help="Season start year (e.g., 2014).")
    parser.add_argument("--end-year", type=int, default=None, help="Season start year (e.g., 2023).")
    parser.add_argument("--output-dir", default="data/nba_api", help="Base output directory.")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Output file format.")
    parser.add_argument("--season-type", default="Regular Season", help="Season type (Regular Season/Playoffs).")
    parser.add_argument("--per-mode", default="PerGame", help="Per-mode for league dash endpoints.")
    parser.add_argument("--sleep", type=float, default=DEFAULT_RATE_LIMIT_SECONDS, help="Sleep between calls.")
    parser.add_argument("--player-scope", choices=["all", "active", "sample"], default="all")
    parser.add_argument("--skip-player-game-logs", action="store_true")
    parser.add_argument("--skip-player-shots", action="store_true")
    parser.add_argument("--skip-player-on-off", action="store_true")
    parser.add_argument("--skip-team-shot-charts", action="store_true")
    parser.add_argument("--skip-matchups", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seasons = resolve_seasons(args.past_seasons, args.start_year, args.end_year)
    base_dir = Path(args.output_dir)

    for season in seasons:
        season_dir = base_dir / season
        ensure_dir(season_dir)

        model_1_dir = season_dir / "model_1_team_playstyle"
        ensure_dir(model_1_dir)
        collect_model_endpoints(
            MODEL_1_ENDPOINTS,
            season,
            model_1_dir,
            args.season_type,
            args.per_mode,
            args.sleep,
            args.format,
        )

        if not args.skip_team_shot_charts:
            collect_shot_charts_for_teams(season, model_1_dir / "shot_charts", args.sleep, args.format)

        model_2_dir = season_dir / "model_2_player"
        ensure_dir(model_2_dir)
        collect_model_endpoints(
            MODEL_2_ENDPOINTS,
            season,
            model_2_dir,
            args.season_type,
            args.per_mode,
            args.sleep,
            args.format,
        )
        collect_player_specific(
            season,
            model_2_dir,
            args.player_scope,
            args.sleep,
            args.format,
            include_game_logs=not args.skip_player_game_logs,
            include_shots=not args.skip_player_shots,
            include_on_off=not args.skip_player_on_off,
        )

        model_3_dir = season_dir / "model_3_lineup_synergy"
        ensure_dir(model_3_dir)
        collect_model_endpoints(
            MODEL_3_ENDPOINTS,
            season,
            model_3_dir,
            args.season_type,
            args.per_mode,
            args.sleep,
            args.format,
        )

        model_4_dir = season_dir / "model_4_matchup_advantage"
        ensure_dir(model_4_dir)
        collect_model_endpoints(
            MODEL_4_ENDPOINTS,
            season,
            model_4_dir,
            args.season_type,
            args.per_mode,
            args.sleep,
            args.format,
        )
        if not args.skip_matchups:
            collect_matchups_from_games(season, model_4_dir, args.sleep, args.format)


if __name__ == "__main__":
    main()
