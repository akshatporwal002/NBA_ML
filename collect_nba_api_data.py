#!/usr/bin/env python
"""Collect NBA API data and store it in consolidated database relations."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import inspect
import json
import sqlite3
import time
import traceback
from pathlib import Path

import pandas as pd
import requests
from nba_api.stats import endpoints
from nba_api.stats.static import players, teams
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout as RequestsReadTimeout


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
    "league_lineups_5man": (endpoints.LeagueDashLineups, {"group_quantity": 5}),
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


class ProgressTracker:
    """Persist season and request-level progress for resumable runs."""

    def __init__(self, progress_path: Path) -> None:
        self.progress_path = progress_path
        ensure_dir(self.progress_path.parent)
        self.state = self._load()

    def _load(self) -> dict:
        if not self.progress_path.exists():
            return {"seasons": {}, "completed_tasks": {}}
        try:
            with self.progress_path.open("r", encoding="utf-8") as file:
                state = json.load(file)
        except (json.JSONDecodeError, OSError):
            return {"seasons": {}, "completed_tasks": {}}
        state.setdefault("seasons", {})
        state.setdefault("completed_tasks", {})
        return state

    def _save(self) -> None:
        tmp_path = self.progress_path.with_suffix(f"{self.progress_path.suffix}.tmp")
        with tmp_path.open("w", encoding="utf-8") as file:
            json.dump(self.state, file, indent=2, sort_keys=True)
        tmp_path.replace(self.progress_path)

    def _task_hash(self, model: str, dataset: str, request_params: dict) -> str:
        payload = json.dumps(
            {"model": model, "dataset": dataset, "request_params": request_params},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def season_status(self, season: str) -> str | None:
        season_state = self.state["seasons"].get(season, {})
        return season_state.get("status")

    def start_season(self, season: str) -> None:
        self.state["seasons"][season] = {
            "status": "in_progress",
            "updated_at": dt.datetime.now(dt.UTC).isoformat(),
            "error_message": None,
        }
        self._save()

    def mark_season_interrupted(self, season: str, error_message: str | None = None) -> None:
        self.state["seasons"][season] = {
            "status": "interrupted",
            "updated_at": dt.datetime.now(dt.UTC).isoformat(),
            "error_message": error_message,
        }
        self._save()

    def mark_season_completed(self, season: str) -> None:
        self.state["seasons"][season] = {
            "status": "completed",
            "updated_at": dt.datetime.now(dt.UTC).isoformat(),
            "error_message": None,
        }
        self._save()

    def is_task_completed(self, season: str, model: str, dataset: str, request_params: dict) -> bool:
        task_hash = self._task_hash(model, dataset, request_params)
        return task_hash in self.state["completed_tasks"].get(season, [])

    def mark_task_completed(self, season: str, model: str, dataset: str, request_params: dict) -> None:
        task_hash = self._task_hash(model, dataset, request_params)
        completed = self.state["completed_tasks"].setdefault(season, [])
        if task_hash not in completed:
            completed.append(task_hash)
            self._save()


def save_frames(frames: list[pd.DataFrame], out_dir: Path, name: str, fmt: str) -> None:
    ensure_dir(out_dir)
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


def filter_endpoint_params(endpoint_cls, params: dict) -> dict:
    signature = inspect.signature(endpoint_cls.__init__)
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()):
        return params
    allowed = {name for name in signature.parameters if name != "self"}
    return {key: value for key, value in params.items() if key in allowed}


class StorageWriter:
    def __init__(self, storage_mode: str, output_dir: Path, db_path: Path, fmt: str) -> None:
        self.storage_mode = storage_mode
        self.output_dir = output_dir
        self.db_path = db_path
        self.fmt = fmt
        self.conn: sqlite3.Connection | None = None

        if self.storage_mode in {"db", "both"}:
            ensure_dir(self.db_path.parent)
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self._create_tables()

    def _create_tables(self) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nba_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season TEXT NOT NULL,
                model TEXT NOT NULL,
                dataset TEXT NOT NULL,
                frame_index INTEGER NOT NULL,
                team_id INTEGER,
                player_id INTEGER,
                game_id TEXT,
                request_json TEXT NOT NULL,
                row_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season TEXT NOT NULL,
                model TEXT NOT NULL,
                dataset TEXT NOT NULL,
                frame_count INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def write_endpoint(
        self,
        season: str,
        model: str,
        dataset: str,
        out_dir: Path,
        frames: list[pd.DataFrame],
        request_params: dict,
    ) -> None:
        if self.storage_mode in {"files", "both"}:
            save_frames(frames, out_dir, dataset, self.fmt)
        if self.storage_mode in {"db", "both"}:
            self._write_endpoint_to_db(season, model, dataset, frames, request_params)

    def _write_endpoint_to_db(
        self,
        season: str,
        model: str,
        dataset: str,
        frames: list[pd.DataFrame],
        request_params: dict,
    ) -> None:
        assert self.conn is not None
        created_at = dt.datetime.now(dt.UTC).isoformat()
        request_json = json.dumps(request_params, default=str)

        row_count = 0
        try:
            for frame_index, frame in enumerate(frames, start=1):
                if frame.empty:
                    continue
                records = []
                for row in frame.to_dict(orient="records"):
                    team_id = row.get("TEAM_ID")
                    player_id = row.get("PLAYER_ID")
                    game_id = row.get("GAME_ID")
                    records.append(
                        (
                            season,
                            model,
                            dataset,
                            frame_index,
                            int(team_id) if pd.notna(team_id) else None,
                            int(player_id) if pd.notna(player_id) else None,
                            str(game_id) if pd.notna(game_id) else None,
                            request_json,
                            json.dumps(row, default=str),
                            created_at,
                        )
                    )
                row_count += len(records)
                self.conn.executemany(
                    """
                    INSERT INTO nba_data (
                        season, model, dataset, frame_index, team_id, player_id, game_id,
                        request_json, row_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    records,
                )
            self.conn.execute(
                """
                INSERT INTO ingestion_log (
                    season, model, dataset, frame_count, row_count, status, error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (season, model, dataset, len(frames), row_count, "ok", None, created_at),
            )
            self.conn.commit()
        except Exception as exc:
            self.conn.execute(
                """
                INSERT INTO ingestion_log (
                    season, model, dataset, frame_count, row_count, status, error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (season, model, dataset, len(frames), row_count, "error", str(exc), created_at),
            )
            self.conn.commit()
            raise

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()


def call_endpoint(
    endpoint_cls,
    storage: StorageWriter,
    progress: ProgressTracker,
    model: str,
    season_label: str,
    out_dir: Path,
    dataset: str,
    sleep_s: float,
    max_retries: int,
    retry_backoff_s: float,
    request_timeout_s: int,
    **params,
) -> None:
    request_params = filter_endpoint_params(endpoint_cls, params)
    if "timeout" not in request_params:
        request_params["timeout"] = request_timeout_s
    if progress.is_task_completed(season_label, model, dataset, request_params):
        print(f"[SKIP] {dataset} (already downloaded)")
        return
    for attempt in range(max_retries + 1):
        print(f"[CALL] {dataset} (attempt {attempt + 1}/{max_retries + 1})")
        try:
            response = endpoint_cls(**request_params)
            frames = response.get_data_frames()
            storage.write_endpoint(season_label, model, dataset, out_dir, frames, request_params)
            progress.mark_task_completed(season_label, model, dataset, request_params)
            print(f"[OK] {dataset}")
            break
        except Exception as exc:
            is_retryable = isinstance(exc, (RequestsReadTimeout, RequestsConnectionError, requests.Timeout))
            print(f"[WARN] {dataset}: {exc}")
            traceback.print_exc()
            if not is_retryable or attempt >= max_retries:
                break
            delay = retry_backoff_s * (2**attempt)
            print(f"[RETRY] {dataset} in {delay:.1f}s")
            time.sleep(delay)
    time.sleep(sleep_s)


def collect_model_endpoints(
    endpoints_map: dict[str, tuple[type, dict]],
    storage: StorageWriter,
    progress: ProgressTracker,
    model: str,
    season: str,
    out_dir: Path,
    season_type: str,
    per_mode: str,
    sleep_s: float,
    max_retries: int,
    retry_backoff_s: float,
    request_timeout_s: int,
) -> None:
    for dataset, (endpoint_cls, extra_params) in endpoints_map.items():
        params = {
            "season": season,
            "season_type_all_star": season_type,
            "per_mode_detailed": per_mode,
        }
        params.update(extra_params)
        call_endpoint(
            endpoint_cls,
            storage,
            progress,
            model,
            season,
            out_dir,
            dataset,
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            **params,
        )


def collect_shot_charts_for_teams(
    season: str,
    storage: StorageWriter,
    progress: ProgressTracker,
    model: str,
    out_dir: Path,
    sleep_s: float,
    max_retries: int,
    retry_backoff_s: float,
    request_timeout_s: int,
) -> None:
    team_data = teams.get_teams()
    for team in team_data:
        call_endpoint(
            endpoints.ShotChartDetail,
            storage,
            progress,
            model,
            season,
            out_dir,
            "shot_chart_team",
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            season=season,
            team_id=team["id"],
            player_id=0,
        )


def collect_team_specific_endpoints(
    season: str,
    storage: StorageWriter,
    progress: ProgressTracker,
    model: str,
    out_dir: Path,
    season_type: str,
    per_mode: str,
    sleep_s: float,
    max_retries: int,
    retry_backoff_s: float,
    request_timeout_s: int,
) -> None:
    team_data = teams.get_teams()
    for team in team_data:
        team_id = team["id"]
        call_endpoint(
            endpoints.TeamDashboardByGeneralSplits,
            storage,
            progress,
            model,
            season,
            out_dir,
            "team_splits_general",
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            team_id=team_id,
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed=per_mode,
        )
        call_endpoint(
            endpoints.TeamDashLineups,
            storage,
            progress,
            model,
            season,
            out_dir,
            "team_lineups_5man",
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            team_id=team_id,
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed=per_mode,
            group_quantity=5,
        )


def collect_player_specific(
    season: str,
    storage: StorageWriter,
    progress: ProgressTracker,
    model: str,
    out_dir: Path,
    player_scope: str,
    sleep_s: float,
    include_game_logs: bool,
    include_shots: bool,
    include_on_off: bool,
    max_retries: int,
    retry_backoff_s: float,
    request_timeout_s: int,
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
        call_endpoint(
            endpoints.CommonPlayerInfo,
            storage,
            progress,
            model,
            season,
            out_dir,
            "common_player_info",
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            player_id=player_id,
        )
        if include_game_logs:
            call_endpoint(
                endpoints.PlayerGameLog,
                storage,
                progress,
                model,
                season,
                out_dir,
                "player_game_log",
                sleep_s,
                max_retries=max_retries,
                retry_backoff_s=retry_backoff_s,
                request_timeout_s=request_timeout_s,
                player_id=player_id,
                season=season,
            )
        call_endpoint(
            endpoints.PlayerCareerStats,
            storage,
            progress,
            model,
            season,
            out_dir,
            "player_career",
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            player_id=player_id,
        )
        if include_on_off:
            call_endpoint(
                endpoints.TeamPlayerOnOffSummary,
                storage,
                progress,
                model,
                season,
                out_dir,
                "player_on_off",
                sleep_s,
                max_retries=max_retries,
                retry_backoff_s=retry_backoff_s,
                request_timeout_s=request_timeout_s,
                player_id=player_id,
                season=season,
            )
        if include_shots:
            call_endpoint(
                endpoints.ShotChartDetail,
                storage,
                progress,
                model,
                season,
                out_dir,
                "shot_chart_player",
                sleep_s,
                max_retries=max_retries,
                retry_backoff_s=retry_backoff_s,
                request_timeout_s=request_timeout_s,
                player_id=player_id,
                team_id=0,
                season=season,
            )


def collect_matchups_from_games(
    season: str,
    storage: StorageWriter,
    progress: ProgressTracker,
    model: str,
    out_dir: Path,
    sleep_s: float,
    max_retries: int,
    retry_backoff_s: float,
    request_timeout_s: int,
) -> None:
    log = endpoints.LeagueGameLog(season=season)
    games = log.get_data_frames()[0]
    for game_id in games["GAME_ID"].unique():
        call_endpoint(
            endpoints.BoxScoreMatchupsV3,
            storage,
            progress,
            model,
            season,
            out_dir,
            "boxscore_matchups",
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            game_id=game_id,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect NBA API data and store in consolidated relations.")
    parser.add_argument("--past-seasons", type=int, default=10, help="How many past seasons to fetch.")
    parser.add_argument("--start-year", type=int, default=None, help="Season start year (e.g., 2014).")
    parser.add_argument("--end-year", type=int, default=None, help="Season start year (e.g., 2023).")
    parser.add_argument("--storage-mode", choices=["db", "files", "both"], default="db")
    parser.add_argument("--db-path", default="data/nba_data.sqlite", help="Path to SQLite database.")
    parser.add_argument("--output-dir", default="data/nba_api", help="Base output directory for file mode.")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Output file format for file mode.")
    parser.add_argument("--season-type", default="Regular Season", help="Season type (Regular Season/Playoffs).")
    parser.add_argument("--per-mode", default="PerGame", help="Per-mode for league dash endpoints.")
    parser.add_argument("--sleep", type=float, default=DEFAULT_RATE_LIMIT_SECONDS, help="Sleep between calls.")
    parser.add_argument("--player-scope", choices=["all", "active", "sample"], default="all")
    parser.add_argument("--skip-player-game-logs", action="store_true")
    parser.add_argument("--skip-player-shots", action="store_true")
    parser.add_argument("--skip-player-on-off", action="store_true")
    parser.add_argument("--skip-team-shot-charts", action="store_true")
    parser.add_argument("--skip-matchups", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries for timeout/network errors per call.")
    parser.add_argument("--retry-backoff", type=float, default=2.0, help="Base seconds for exponential retry backoff.")
    parser.add_argument("--request-timeout", type=int, default=45, help="HTTP timeout seconds per API request.")
    parser.add_argument(
        "--progress-path",
        default=None,
        help="Path to resume checkpoint JSON (defaults to <output-dir>/download_progress.json).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seasons = resolve_seasons(args.past_seasons, args.start_year, args.end_year)
    base_dir = Path(args.output_dir)
    db_path = Path(args.db_path)
    storage = StorageWriter(args.storage_mode, base_dir, db_path, args.format)
    progress_path = Path(args.progress_path) if args.progress_path else (base_dir / "download_progress.json")
    progress = ProgressTracker(progress_path)

    try:
        for season in seasons:
            if progress.season_status(season) == "completed":
                print(f"[SKIP] Season {season} already completed.")
                continue

            progress.start_season(season)
            season_dir = base_dir / season
            ensure_dir(season_dir)

            try:
                model_1_dir = season_dir / "model_1_team_playstyle"
                collect_model_endpoints(
                    MODEL_1_ENDPOINTS,
                    storage,
                    progress,
                    "model_1_team_playstyle",
                    season,
                    model_1_dir,
                    args.season_type,
                    args.per_mode,
                    args.sleep,
                    args.max_retries,
                    args.retry_backoff,
                    args.request_timeout,
                )
                collect_team_specific_endpoints(
                    season,
                    storage,
                    progress,
                    "model_1_team_playstyle",
                    model_1_dir,
                    args.season_type,
                    args.per_mode,
                    args.sleep,
                    args.max_retries,
                    args.retry_backoff,
                    args.request_timeout,
                )
                if not args.skip_team_shot_charts:
                    collect_shot_charts_for_teams(
                        season,
                        storage,
                        progress,
                        "model_1_team_playstyle",
                        model_1_dir,
                        args.sleep,
                        args.max_retries,
                        args.retry_backoff,
                        args.request_timeout,
                    )

                model_2_dir = season_dir / "model_2_player"
                collect_model_endpoints(
                    MODEL_2_ENDPOINTS,
                    storage,
                    progress,
                    "model_2_player",
                    season,
                    model_2_dir,
                    args.season_type,
                    args.per_mode,
                    args.sleep,
                    args.max_retries,
                    args.retry_backoff,
                    args.request_timeout,
                )
                collect_player_specific(
                    season,
                    storage,
                    progress,
                    "model_2_player",
                    model_2_dir,
                    args.player_scope,
                    args.sleep,
                    include_game_logs=not args.skip_player_game_logs,
                    include_shots=not args.skip_player_shots,
                    include_on_off=not args.skip_player_on_off,
                    max_retries=args.max_retries,
                    retry_backoff_s=args.retry_backoff,
                    request_timeout_s=args.request_timeout,
                )

                model_3_dir = season_dir / "model_3_lineup_synergy"
                collect_model_endpoints(
                    MODEL_3_ENDPOINTS,
                    storage,
                    progress,
                    "model_3_lineup_synergy",
                    season,
                    model_3_dir,
                    args.season_type,
                    args.per_mode,
                    args.sleep,
                    args.max_retries,
                    args.retry_backoff,
                    args.request_timeout,
                )
                collect_team_specific_endpoints(
                    season,
                    storage,
                    progress,
                    "model_3_lineup_synergy",
                    model_3_dir,
                    args.season_type,
                    args.per_mode,
                    args.sleep,
                    args.max_retries,
                    args.retry_backoff,
                    args.request_timeout,
                )

                model_4_dir = season_dir / "model_4_matchup_advantage"
                collect_model_endpoints(
                    MODEL_4_ENDPOINTS,
                    storage,
                    progress,
                    "model_4_matchup_advantage",
                    season,
                    model_4_dir,
                    args.season_type,
                    args.per_mode,
                    args.sleep,
                    args.max_retries,
                    args.retry_backoff,
                    args.request_timeout,
                )
                if not args.skip_matchups:
                    collect_matchups_from_games(
                        season,
                        storage,
                        progress,
                        "model_4_matchup_advantage",
                        model_4_dir,
                        args.sleep,
                        args.max_retries,
                        args.retry_backoff,
                        args.request_timeout,
                    )
            except KeyboardInterrupt:
                progress.mark_season_interrupted(season, "KeyboardInterrupt")
                print(f"[INTERRUPTED] Season {season} marked as interrupted.")
                raise
            except Exception as exc:
                progress.mark_season_interrupted(season, str(exc))
                raise
            else:
                progress.mark_season_completed(season)
                print(f"[DONE] Season {season} completed.")
    finally:
        storage.close()


if __name__ == "__main__":
    main()
