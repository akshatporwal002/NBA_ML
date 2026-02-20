#!/usr/bin/env python
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from collect_nba_api_data import StorageWriter


def main() -> None:
    db_path = Path("data/test_nba_data.sqlite")
    if db_path.exists():
        db_path.unlink()

    storage = StorageWriter(
        storage_mode="db",
        output_dir=Path("data/unused"),
        db_path=db_path,
        fmt="csv",
    )

    frames = [
        pd.DataFrame(
            [
                {"TEAM_ID": 1610612737, "PLAYER_ID": 203507, "GAME_ID": "001", "PTS": 28},
                {"TEAM_ID": 1610612737, "PLAYER_ID": 1630162, "GAME_ID": "001", "PTS": 14},
            ]
        ),
        pd.DataFrame([{"TEAM_ID": 1610612737, "PLAYER_ID": 203507, "GAME_ID": "002", "PTS": 31}]),
    ]

    storage.write_endpoint(
        season="2024-25",
        model="model_2_player",
        dataset="player_game_log",
        out_dir=Path("data/unused"),
        frames=frames,
        request_params={"season": "2024-25", "player_id": 203507},
    )
    storage.close()

    conn = sqlite3.connect(db_path)
    data_rows = conn.execute("SELECT COUNT(*) FROM nba_data").fetchone()[0]
    log_rows = conn.execute("SELECT COUNT(*) FROM ingestion_log WHERE status = 'ok'").fetchone()[0]
    distinct_datasets = conn.execute("SELECT COUNT(DISTINCT dataset) FROM nba_data").fetchone()[0]
    conn.close()

    assert data_rows == 3, f"Expected 3 rows, got {data_rows}"
    assert log_rows == 1, f"Expected 1 log row, got {log_rows}"
    assert distinct_datasets == 1, f"Expected 1 dataset, got {distinct_datasets}"
    print("Smoke test passed: consolidated DB storage works.")


if __name__ == "__main__":
    main()
