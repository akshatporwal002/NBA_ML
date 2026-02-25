#!/usr/bin/env python
"""Collect NBA API data and store it in consolidated database relations."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import inspect
import json
import sqlite3
import socket
import sys
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from nba_api.stats import endpoints
from nba_api.stats.static import players, teams
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout as RequestsReadTimeout


DEFAULT_RATE_LIMIT_SECONDS = 1.2
SCHEMA_VERSION = "1"


class EndpointTerminalError(RuntimeError):
    """Raised when an endpoint fails terminally under fail_fast mode."""


@dataclass(frozen=True)
class EntityContext:
    entity_type: str
    entity_id: str | None


def enable_ipv4_only() -> None:
    """Force IPv4 DNS results to avoid unstable IPv6 routes."""
    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
        return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    socket.getaddrinfo = getaddrinfo_ipv4


def patch_nba_api_session() -> None:
    """Inject required headers and a per-request session factory into NBAStatsHTTP.

    stats.nba.com will silently hang (ReadTimeout) when:
    - the x-nba-stats-token / x-nba-stats-origin gating headers are absent, or
    - a stale keep-alive connection is reused after the server closes it.
    Both problems are fixed here.
    """
    from nba_api.stats.library.http import NBAStatsHTTP

    extra_headers = {
        "x-nba-stats-token": "true",
        "x-nba-stats-origin": "stats",
        "Origin": "https://www.nba.com",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Ch-Ua-Platform": '"Windows"',
    }
    NBAStatsHTTP.headers = {**NBAStatsHTTP.headers, **extra_headers}

    @classmethod  # type: ignore[misc]
    def _fresh_session(cls):  # noqa: N805
        session = requests.Session()
        session.headers.update(cls.headers)
        return session

    NBAStatsHTTP.get_session = _fresh_session
    print("[PATCH] NBAStatsHTTP patched: required headers injected, per-call session enabled.")


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
    """Compatibility JSON checkpoint cache. DB status is source of truth."""

    def __init__(self, progress_path: Path) -> None:
        self.progress_path = progress_path
        ensure_dir(self.progress_path.parent)
        self.state = self._load()

    def _load(self) -> dict:
        if not self.progress_path.exists():
            return {"seasons": {}, "completed_tasks": {}, "cursors": {}}
        try:
            with self.progress_path.open("r", encoding="utf-8") as file:
                state = json.load(file)
        except (json.JSONDecodeError, OSError):
            return {"seasons": {}, "completed_tasks": {}, "cursors": {}}
        state.setdefault("seasons", {})
        state.setdefault("completed_tasks", {})
        state.setdefault("cursors", {})
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

    def get_cursor(self, season: str, model: str, loop_name: str) -> int:
        key = f"{season}::{model}::{loop_name}"
        return self.state["cursors"].get(key, 0)

    def advance_cursor(self, season: str, model: str, loop_name: str) -> None:
        key = f"{season}::{model}::{loop_name}"
        self.state["cursors"][key] = self.state["cursors"].get(key, 0) + 1
        self._save()

    def set_cursor(self, season: str, model: str, loop_name: str, value: int) -> None:
        key = f"{season}::{model}::{loop_name}"
        self.state["cursors"][key] = max(0, value)
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


def request_hash(model: str, dataset: str, request_params: dict) -> str:
    payload = json.dumps(
        {"model": model, "dataset": dataset, "request_params": request_params},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class StorageWriter:
    def __init__(self, storage_mode: str, output_dir: Path, db_path: Path, fmt: str) -> None:
        self.storage_mode = storage_mode
        self.output_dir = output_dir
        self.db_path = db_path
        self.fmt = fmt
        self.conn: sqlite3.Connection | None = None
        self._table_cache: dict[str, set[str]] = {}
        self.run_id: str | None = None

        if self.storage_mode in {"db", "both"}:
            ensure_dir(self.db_path.parent)
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self._create_tables()

    @staticmethod
    def _to_json(payload: dict) -> str:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _entity_id_value(entity_id: str | int | None) -> str | None:
        if entity_id is None:
            return None
        return str(entity_id)

    def _create_tables(self) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
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
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_run (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                fail_reason TEXT,
                cli_args_json TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                season TEXT NOT NULL,
                model TEXT NOT NULL,
                dataset TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                attempt INTEGER NOT NULL,
                status TEXT NOT NULL,
                is_retryable INTEGER,
                error_type TEXT,
                error_message TEXT,
                request_hash TEXT NOT NULL,
                request_json TEXT,
                frame_count INTEGER,
                row_count INTEGER,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_ms INTEGER,
                FOREIGN KEY(run_id) REFERENCES ingestion_run(run_id)
            )
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ingestion_status_lookup
            ON ingestion_status (season, model, dataset, entity_type, entity_id, request_hash, status)
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ingestion_status_run
            ON ingestion_status (run_id, season, status)
            """
        )
        now = dt.datetime.now(dt.UTC).isoformat()
        self.conn.execute(
            """
            INSERT INTO meta(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            ("schema_version", SCHEMA_VERSION, now),
        )
        self.conn.commit()

        for (tbl,) in self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'nba__%'"
        ):
            cols = {row[1] for row in self.conn.execute(f'PRAGMA table_info("{tbl}")')}
            self._table_cache[tbl] = cols

    def start_run(self, cli_args: dict) -> str:
        if self.conn is None:
            self.run_id = str(uuid.uuid4())
            return self.run_id
        self.run_id = str(uuid.uuid4())
        started_at = dt.datetime.now(dt.UTC).isoformat()
        self.conn.execute(
            """
            INSERT INTO ingestion_run (run_id, started_at, ended_at, status, fail_reason, cli_args_json)
            VALUES (?, ?, NULL, ?, NULL, ?)
            """,
            (self.run_id, started_at, "running", self._to_json(cli_args)),
        )
        self.conn.commit()
        return self.run_id

    def finish_run(self, status: str, fail_reason: str | None = None) -> None:
        if self.conn is None or self.run_id is None:
            return
        ended_at = dt.datetime.now(dt.UTC).isoformat()
        self.conn.execute(
            """
            UPDATE ingestion_run
            SET ended_at = ?, status = ?, fail_reason = ?
            WHERE run_id = ?
            """,
            (ended_at, status, fail_reason, self.run_id),
        )
        self.conn.commit()

    def start_status_row(
        self,
        season: str,
        model: str,
        dataset: str,
        entity: EntityContext,
        attempt: int,
        req_hash: str,
    ) -> int | None:
        if self.conn is None or self.run_id is None:
            return None
        started_at = dt.datetime.now(dt.UTC).isoformat()
        cur = self.conn.execute(
            """
            INSERT INTO ingestion_status (
                run_id, season, model, dataset, entity_type, entity_id,
                attempt, status, request_hash, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.run_id,
                season,
                model,
                dataset,
                entity.entity_type,
                self._entity_id_value(entity.entity_id),
                attempt,
                "started",
                req_hash,
                started_at,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_status_success(
        self,
        status_id: int | None,
        frame_count: int,
        row_count: int,
        duration_ms: int,
        commit: bool = True,
    ) -> None:
        if self.conn is None or status_id is None:
            return
        ended_at = dt.datetime.now(dt.UTC).isoformat()
        self.conn.execute(
            """
            UPDATE ingestion_status
            SET status = ?, frame_count = ?, row_count = ?, ended_at = ?, duration_ms = ?
            WHERE id = ?
            """,
            ("ok", frame_count, row_count, ended_at, duration_ms, status_id),
        )
        if commit:
            self.conn.commit()

    def finish_status_error(
        self,
        status_id: int | None,
        exc: Exception,
        is_retryable: bool,
        request_params: dict,
        duration_ms: int,
        commit: bool = True,
    ) -> None:
        if self.conn is None or status_id is None:
            return
        ended_at = dt.datetime.now(dt.UTC).isoformat()
        self.conn.execute(
            """
            UPDATE ingestion_status
            SET status = ?, is_retryable = ?, error_type = ?, error_message = ?,
                request_json = ?, ended_at = ?, duration_ms = ?
            WHERE id = ?
            """,
            (
                "error",
                1 if is_retryable else 0,
                type(exc).__name__,
                str(exc),
                self._to_json(request_params),
                ended_at,
                duration_ms,
                status_id,
            ),
        )
        if commit:
            self.conn.commit()

    def was_endpoint_successful(
        self,
        season: str,
        model: str,
        dataset: str,
        entity: EntityContext,
        req_hash: str,
    ) -> bool:
        if self.conn is None:
            return False
        cur = self.conn.execute(
            """
            SELECT 1
            FROM ingestion_status
            WHERE season = ?
              AND model = ?
              AND dataset = ?
              AND entity_type = ?
              AND COALESCE(entity_id, '') = COALESCE(?, '')
              AND request_hash = ?
              AND status = 'ok'
            LIMIT 1
            """,
            (
                season,
                model,
                dataset,
                entity.entity_type,
                self._entity_id_value(entity.entity_id),
                req_hash,
            ),
        )
        return cur.fetchone() is not None

    def run_has_errors(self) -> bool:
        if self.conn is None or self.run_id is None:
            return False
        cur = self.conn.execute(
            """
            SELECT 1 FROM ingestion_status
            WHERE run_id = ? AND status = 'error'
            LIMIT 1
            """,
            (self.run_id,),
        )
        return cur.fetchone() is not None

    def run_has_season_errors(self, season: str) -> bool:
        if self.conn is None or self.run_id is None:
            return False
        cur = self.conn.execute(
            """
            SELECT 1 FROM ingestion_status
            WHERE run_id = ? AND season = ? AND status = 'error'
            LIMIT 1
            """,
            (self.run_id, season),
        )
        return cur.fetchone() is not None

    def compute_resume_index(
        self,
        season: str,
        model: str,
        entity_type: str,
        ordered_entity_ids: list[str],
        required_datasets: list[str],
    ) -> int:
        if self.conn is None:
            return 0
        if not ordered_entity_ids:
            return 0
        if not required_datasets:
            return 0

        per_dataset_success: dict[str, set[str]] = {}
        for dataset in required_datasets:
            cur = self.conn.execute(
                """
                SELECT DISTINCT entity_id
                FROM ingestion_status
                WHERE season = ?
                  AND model = ?
                  AND dataset = ?
                  AND entity_type = ?
                  AND status = 'ok'
                  AND entity_id IS NOT NULL
                """,
                (season, model, dataset, entity_type),
            )
            per_dataset_success[dataset] = {str(row[0]) for row in cur.fetchall()}

        for idx, entity_id in enumerate(ordered_entity_ids):
            if not all(entity_id in per_dataset_success[dataset] for dataset in required_datasets):
                return idx
        return len(ordered_entity_ids)

    @staticmethod
    def _table_name(dataset: str) -> str:
        safe = "".join(c if (c.isalnum() or c == "_") else "_" for c in dataset.lower())
        return f"nba__{safe}"

    def _ensure_flat_table(self, dataset: str, frame: pd.DataFrame) -> str:
        table = self._table_name(dataset)
        assert self.conn is not None
        frame_cols = set(frame.columns)

        if table not in self._table_cache:
            existing_cols = {row[1] for row in self.conn.execute(f'PRAGMA table_info("{table}")')}
            if not existing_cols:
                meta = (
                    "_season TEXT NOT NULL, "
                    "_model TEXT NOT NULL, "
                    "_frame_index INTEGER NOT NULL, "
                    "_created_at TEXT NOT NULL"
                )
                data_cols = ", ".join(f'"{c}" TEXT' for c in frame.columns)
                self.conn.execute(
                    f'CREATE TABLE "{table}" '
                    f"(id INTEGER PRIMARY KEY AUTOINCREMENT, {meta}, {data_cols})"
                )
                self._table_cache[table] = set(frame.columns)
            else:
                new_cols = [c for c in frame.columns if c not in existing_cols]
                for col in new_cols:
                    self.conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')
                self._table_cache[table] = existing_cols | frame_cols
        else:
            known_cols = self._table_cache[table]
            new_cols = [c for c in frame.columns if c not in known_cols]
            for col in new_cols:
                self.conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')
            self._table_cache[table] = known_cols | frame_cols

        return table

    def write_endpoint_files(self, frames: list[pd.DataFrame], out_dir: Path, dataset: str) -> None:
        if self.storage_mode in {"files", "both"}:
            save_frames(frames, out_dir, dataset, self.fmt)

    def write_endpoint_db(
        self,
        season: str,
        model: str,
        dataset: str,
        frames: list[pd.DataFrame],
        commit: bool,
    ) -> tuple[int, int]:
        assert self.conn is not None
        created_at = dt.datetime.now(dt.UTC).isoformat()

        row_count = 0
        frame_count = len(frames)
        for frame_index, frame in enumerate(frames, start=1):
            if frame.empty:
                continue
            table = self._ensure_flat_table(dataset, frame)
            cols = list(frame.columns)
            col_list = ", ".join(f'"{c}"' for c in cols)
            placeholders = ", ".join(["?"] * (4 + len(cols)))
            insert_sql = (
                f'INSERT INTO "{table}" '
                f"(_season, _model, _frame_index, _created_at, {col_list}) "
                f"VALUES ({placeholders})"
            )
            records = []
            for row in frame.to_dict(orient="records"):
                meta = (season, model, frame_index, created_at)
                data = tuple(
                    None if (v is None or (isinstance(v, float) and v != v)) else v
                    for v in (row[c] for c in cols)
                )
                records.append(meta + data)
            row_count += len(records)
            if records:
                self.conn.executemany(insert_sql, records)

        self.conn.execute(
            """
            INSERT INTO ingestion_log (
                season, model, dataset, frame_count, row_count, status, error_message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (season, model, dataset, frame_count, row_count, "ok", None, created_at),
        )

        if commit:
            self.conn.commit()
        return frame_count, row_count

    def log_legacy_error(self, season: str, model: str, dataset: str, error_message: str) -> None:
        if self.conn is None:
            return
        created_at = dt.datetime.now(dt.UTC).isoformat()
        self.conn.execute(
            """
            INSERT INTO ingestion_log (
                season, model, dataset, frame_count, row_count, status, error_message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (season, model, dataset, 0, 0, "error", error_message[:4000], created_at),
        )
        self.conn.commit()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()


def sync_cursor_with_db(
    progress: ProgressTracker,
    storage: StorageWriter,
    season: str,
    model: str,
    loop_name: str,
    entity_type: str,
    ordered_entity_ids: list[str],
    required_datasets: list[str],
) -> int:
    db_done = storage.compute_resume_index(
        season=season,
        model=model,
        entity_type=entity_type,
        ordered_entity_ids=ordered_entity_ids,
        required_datasets=required_datasets,
    )
    json_done = progress.get_cursor(season, model, loop_name)
    if json_done != db_done:
        print(f"[RESUME] {loop_name}: DB cursor={db_done}, JSON cursor={json_done}; using DB cursor")
        progress.set_cursor(season, model, loop_name, db_done)
    return db_done


def call_endpoint(
    endpoint_cls,
    storage: StorageWriter,
    progress: ProgressTracker,
    model: str,
    season_label: str,
    out_dir: Path,
    dataset: str,
    entity: EntityContext,
    sleep_s: float,
    max_retries: int,
    retry_backoff_s: float,
    request_timeout_s: int,
    fail_mode: str,
    **params,
) -> bool:
    request_params = filter_endpoint_params(endpoint_cls, params)
    if "timeout" not in request_params:
        request_params["timeout"] = request_timeout_s

    req_hash = request_hash(model, dataset, request_params)
    if storage.was_endpoint_successful(season_label, model, dataset, entity, req_hash):
        print(f"[SKIP] {dataset} ({entity.entity_type}={entity.entity_id}) already downloaded")
        progress.mark_task_completed(season_label, model, dataset, request_params)
        return True

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        status_row_id = storage.start_status_row(
            season=season_label,
            model=model,
            dataset=dataset,
            entity=entity,
            attempt=attempt + 1,
            req_hash=req_hash,
        )
        started_at = time.monotonic()
        print(f"[CALL] {dataset} ({entity.entity_type}={entity.entity_id}) attempt {attempt + 1}/{max_retries + 1}")
        try:
            response = endpoint_cls(**request_params)
            frames = response.get_data_frames()
            storage.write_endpoint_files(frames, out_dir, dataset)

            row_count = sum(0 if frame.empty else len(frame.index) for frame in frames)
            frame_count = len(frames)
            duration_ms = int((time.monotonic() - started_at) * 1000)

            if storage.conn is not None and storage.storage_mode in {"db", "both"}:
                assert storage.conn is not None
                storage.conn.execute("BEGIN")
                try:
                    frame_count, row_count = storage.write_endpoint_db(
                        season=season_label,
                        model=model,
                        dataset=dataset,
                        frames=frames,
                        commit=False,
                    )
                    storage.finish_status_success(
                        status_id=status_row_id,
                        frame_count=frame_count,
                        row_count=row_count,
                        duration_ms=duration_ms,
                        commit=False,
                    )
                    storage.conn.commit()
                except Exception:
                    if storage.conn.in_transaction:
                        storage.conn.rollback()
                    raise
            else:
                storage.finish_status_success(
                    status_id=status_row_id,
                    frame_count=frame_count,
                    row_count=row_count,
                    duration_ms=duration_ms,
                )

            progress.mark_task_completed(season_label, model, dataset, request_params)
            print(f"[OK] {dataset}")
            time.sleep(sleep_s)
            return True
        except Exception as exc:
            last_exc = exc
            is_retryable = isinstance(exc, (RequestsReadTimeout, RequestsConnectionError, requests.Timeout))
            duration_ms = int((time.monotonic() - started_at) * 1000)
            if storage.conn is not None and storage.conn.in_transaction:
                storage.conn.rollback()
            storage.finish_status_error(
                status_id=status_row_id,
                exc=exc,
                is_retryable=is_retryable,
                request_params=request_params,
                duration_ms=duration_ms,
            )
            storage.log_legacy_error(season_label, model, dataset, str(exc))
            print(f"[WARN] {dataset}: {exc}")
            traceback.print_exc()

            if is_retryable and attempt < max_retries:
                delay = retry_backoff_s * (2**attempt)
                print(f"[RETRY] {dataset} in {delay:.1f}s")
                time.sleep(delay)
                continue

            if fail_mode == "fail_fast":
                raise EndpointTerminalError(
                    f"Terminal failure for dataset={dataset}, entity={entity.entity_type}:{entity.entity_id}: {exc}"
                ) from exc

            print(f"[ERROR] {dataset} terminal failure logged; continuing due to fail_mode=log")
            time.sleep(sleep_s)
            return False

    if fail_mode == "fail_fast" and last_exc is not None:
        raise EndpointTerminalError(
            f"Terminal failure for dataset={dataset}, entity={entity.entity_type}:{entity.entity_id}: {last_exc}"
        ) from last_exc
    time.sleep(sleep_s)
    return False


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
    fail_mode: str,
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
            EntityContext(entity_type="season", entity_id=season),
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            fail_mode=fail_mode,
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
    fail_mode: str,
) -> None:
    team_data = teams.get_teams()
    ordered_ids = [str(team["id"]) for team in team_data]
    done = sync_cursor_with_db(
        progress,
        storage,
        season,
        model,
        "shot_chart_teams",
        "team",
        ordered_ids,
        ["shot_chart_team"],
    )
    if done:
        print(f"[RESUME] shot_chart_teams: skipping first {done}/{len(team_data)} teams")

    cursor_contiguous = True
    for i, team in enumerate(team_data):
        if i < done:
            continue
        ok = call_endpoint(
            endpoints.ShotChartDetail,
            storage,
            progress,
            model,
            season,
            out_dir,
            "shot_chart_team",
            EntityContext(entity_type="team", entity_id=str(team["id"])),
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            fail_mode=fail_mode,
            season=season,
            team_id=team["id"],
            player_id=0,
        )
        if cursor_contiguous and ok:
            progress.advance_cursor(season, model, "shot_chart_teams")
        else:
            cursor_contiguous = False


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
    fail_mode: str,
) -> None:
    team_data = teams.get_teams()
    ordered_ids = [str(team["id"]) for team in team_data]
    required = ["team_splits_general", "team_lineups_5man"]
    done = sync_cursor_with_db(
        progress,
        storage,
        season,
        model,
        "team_specific",
        "team",
        ordered_ids,
        required,
    )
    if done:
        print(f"[RESUME] team_specific: skipping first {done}/{len(team_data)} teams")

    cursor_contiguous = True
    for i, team in enumerate(team_data):
        if i < done:
            continue
        team_id = team["id"]
        ok = True
        ok = call_endpoint(
            endpoints.TeamDashboardByGeneralSplits,
            storage,
            progress,
            model,
            season,
            out_dir,
            "team_splits_general",
            EntityContext(entity_type="team", entity_id=str(team_id)),
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            fail_mode=fail_mode,
            team_id=team_id,
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed=per_mode,
        ) and ok
        ok = call_endpoint(
            endpoints.TeamDashLineups,
            storage,
            progress,
            model,
            season,
            out_dir,
            "team_lineups_5man",
            EntityContext(entity_type="team", entity_id=str(team_id)),
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            fail_mode=fail_mode,
            team_id=team_id,
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed=per_mode,
            group_quantity=5,
        ) and ok

        if cursor_contiguous and ok:
            progress.advance_cursor(season, model, "team_specific")
        else:
            cursor_contiguous = False


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
    fail_mode: str,
) -> None:
    all_players = players.get_players()
    if player_scope == "active":
        selected = [player for player in all_players if player.get("is_active")]
    elif player_scope == "sample":
        selected = all_players[:25]
    else:
        selected = all_players

    required = ["common_player_info", "player_career"]
    if include_game_logs:
        required.append("player_game_log")
    if include_on_off:
        required.append("player_on_off")
    if include_shots:
        required.append("shot_chart_player")

    ordered_ids = [str(player["id"]) for player in selected]
    done = sync_cursor_with_db(
        progress,
        storage,
        season,
        model,
        "players",
        "player",
        ordered_ids,
        required,
    )
    if done:
        print(f"[RESUME] players: skipping first {done}/{len(selected)} players")

    cursor_contiguous = True
    for i, player in enumerate(selected):
        if i < done:
            continue
        player_id = player["id"]
        entity = EntityContext(entity_type="player", entity_id=str(player_id))
        ok = True

        ok = call_endpoint(
            endpoints.CommonPlayerInfo,
            storage,
            progress,
            model,
            season,
            out_dir,
            "common_player_info",
            entity,
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            fail_mode=fail_mode,
            player_id=player_id,
        ) and ok

        if include_game_logs:
            ok = call_endpoint(
                endpoints.PlayerGameLog,
                storage,
                progress,
                model,
                season,
                out_dir,
                "player_game_log",
                entity,
                sleep_s,
                max_retries=max_retries,
                retry_backoff_s=retry_backoff_s,
                request_timeout_s=request_timeout_s,
                fail_mode=fail_mode,
                player_id=player_id,
                season=season,
            ) and ok

        ok = call_endpoint(
            endpoints.PlayerCareerStats,
            storage,
            progress,
            model,
            season,
            out_dir,
            "player_career",
            entity,
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            fail_mode=fail_mode,
            player_id=player_id,
        ) and ok

        if include_on_off:
            ok = call_endpoint(
                endpoints.TeamPlayerOnOffSummary,
                storage,
                progress,
                model,
                season,
                out_dir,
                "player_on_off",
                entity,
                sleep_s,
                max_retries=max_retries,
                retry_backoff_s=retry_backoff_s,
                request_timeout_s=request_timeout_s,
                fail_mode=fail_mode,
                player_id=player_id,
                season=season,
            ) and ok

        if include_shots:
            ok = call_endpoint(
                endpoints.ShotChartDetail,
                storage,
                progress,
                model,
                season,
                out_dir,
                "shot_chart_player",
                entity,
                sleep_s,
                max_retries=max_retries,
                retry_backoff_s=retry_backoff_s,
                request_timeout_s=request_timeout_s,
                fail_mode=fail_mode,
                player_id=player_id,
                team_id=0,
                season=season,
            ) and ok

        if cursor_contiguous and ok:
            progress.advance_cursor(season, model, "players")
        else:
            cursor_contiguous = False


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
    fail_mode: str,
) -> None:
    log = endpoints.LeagueGameLog(season=season)
    game_ids = sorted(log.get_data_frames()[0]["GAME_ID"].unique())
    ordered_ids = [str(game_id) for game_id in game_ids]
    done = sync_cursor_with_db(
        progress,
        storage,
        season,
        model,
        "matchup_games",
        "game",
        ordered_ids,
        ["boxscore_matchups"],
    )
    if done:
        print(f"[RESUME] matchup_games: skipping first {done}/{len(game_ids)} games")

    cursor_contiguous = True
    for i, game_id in enumerate(game_ids):
        if i < done:
            continue
        ok = call_endpoint(
            endpoints.BoxScoreMatchupsV3,
            storage,
            progress,
            model,
            season,
            out_dir,
            "boxscore_matchups",
            EntityContext(entity_type="game", entity_id=str(game_id)),
            sleep_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
            request_timeout_s=request_timeout_s,
            fail_mode=fail_mode,
            game_id=game_id,
        )
        if cursor_contiguous and ok:
            progress.advance_cursor(season, model, "matchup_games")
        else:
            cursor_contiguous = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect NBA API data and store in consolidated relations.")
    parser.add_argument("--past-seasons", type=int, default=1, help="How many past seasons to fetch.")
    parser.add_argument("--start-year", type=int, default=2024, help="Season start year (e.g., 2014).")
    parser.add_argument("--end-year", type=int, default=2024, help="Season start year (e.g., 2023).")
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
    parser.add_argument("--skip-player-on-off", action="store_true", help="Backward-compatible alias to disable player on/off.")
    parser.add_argument("--enable-player-on-off", action="store_true", help="Enable player on/off endpoint (disabled by default).")
    parser.add_argument("--skip-team-shot-charts", action="store_true")
    parser.add_argument("--skip-matchups", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries for timeout/network errors per call.")
    parser.add_argument("--retry-backoff", type=float, default=2.0, help="Base seconds for exponential retry backoff.")
    parser.add_argument("--request-timeout", type=int, default=45, help="HTTP timeout seconds per API request.")
    parser.add_argument("--fail-mode", choices=["fail_fast", "log"], default="fail_fast", help="Failure handling mode.")
    parser.add_argument("--force-ipv4", action="store_true", help="Force IPv4 to avoid problematic IPv6 routing.")
    parser.add_argument(
        "--progress-path",
        default=None,
        help="Path to compatibility checkpoint JSON (defaults to <output-dir>/download_progress.json).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.force_ipv4:
        enable_ipv4_only()
        print("[INFO] IPv4-only mode enabled")
    patch_nba_api_session()

    seasons = resolve_seasons(args.past_seasons, args.start_year, args.end_year)
    base_dir = Path(args.output_dir)
    db_path = Path(args.db_path)
    storage = StorageWriter(args.storage_mode, base_dir, db_path, args.format)
    progress_path = Path(args.progress_path) if args.progress_path else (base_dir / "download_progress.json")
    progress = ProgressTracker(progress_path)

    include_on_off = args.enable_player_on_off and not args.skip_player_on_off
    if args.enable_player_on_off and args.skip_player_on_off:
        print("[INFO] --skip-player-on-off overrides --enable-player-on-off; player on/off remains disabled")
    if not include_on_off:
        print("[INFO] Player on/off endpoint disabled by default. Use --enable-player-on-off to enable it.")

    storage.start_run(vars(args))

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
                    args.fail_mode,
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
                    args.fail_mode,
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
                        args.fail_mode,
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
                    args.fail_mode,
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
                    include_on_off=include_on_off,
                    max_retries=args.max_retries,
                    retry_backoff_s=args.retry_backoff,
                    request_timeout_s=args.request_timeout,
                    fail_mode=args.fail_mode,
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
                    args.fail_mode,
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
                    args.fail_mode,
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
                    args.fail_mode,
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
                        args.fail_mode,
                    )
            except KeyboardInterrupt:
                progress.mark_season_interrupted(season, "KeyboardInterrupt")
                print(f"[INTERRUPTED] Season {season} marked as interrupted.")
                raise
            except Exception as exc:
                progress.mark_season_interrupted(season, str(exc))
                if args.fail_mode == "fail_fast":
                    raise
                print(f"[WARN] Season {season} encountered errors: {exc}")
            finally:
                if storage.run_has_season_errors(season):
                    progress.mark_season_interrupted(season, "One or more endpoint failures recorded")
                    print(f"[WARN] Season {season} has recorded endpoint failures.")
                elif progress.season_status(season) == "in_progress":
                    progress.mark_season_completed(season)
                    print(f"[DONE] Season {season} completed.")

        if storage.run_has_errors():
            storage.finish_run(status="completed_with_errors", fail_reason="One or more endpoint calls failed")
            return 1
        storage.finish_run(status="completed", fail_reason=None)
        return 0
    except KeyboardInterrupt:
        storage.finish_run(status="interrupted", fail_reason="KeyboardInterrupt")
        return 130
    except Exception as exc:
        storage.finish_run(status="failed", fail_reason=str(exc))
        return 1
    finally:
        storage.close()


if __name__ == "__main__":
    sys.exit(main())
