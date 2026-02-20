#!/usr/bin/env bash

set -uo pipefail

TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
LOG_DIR="logs"
OUTPUT_DIR="data/nba_api"
DB_PATH="data/nba_data.sqlite"
PROGRESS_PATH="${OUTPUT_DIR}/download_progress.json"
LOG_FILE="${LOG_DIR}/nba_download_${TIMESTAMP}.log"

mkdir -p "${LOG_DIR}" "${OUTPUT_DIR}"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="python3"
fi

INTERRUPTED=0
on_interrupt() {
  INTERRUPTED=1
  echo
  echo "[INTERRUPTED] Stop signal received. Marking current season as interrupted and exiting..."
}
trap on_interrupt INT TERM

CMD=(
  "${PYTHON_BIN}" collect_nba_api_data.py
  --output-dir "${OUTPUT_DIR}"
  --db-path "${DB_PATH}"
  --progress-path "${PROGRESS_PATH}"
  "$@"
)

echo "[START] NBA download started at $(date)"
echo "[INFO] Log file: ${LOG_FILE}"
echo "[INFO] Progress file: ${PROGRESS_PATH}"
echo "[INFO] Python: ${PYTHON_BIN}"
echo "[INFO] Command: ${CMD[*]}"
echo "[INFO] Press Ctrl+C to interrupt safely and resume later."
echo

if ! "${PYTHON_BIN}" -c "import pandas" >/dev/null 2>&1; then
  echo "[ERROR] Missing dependency: pandas"
  echo "[INFO] Install dependencies in your venv, for example:"
  echo "       source .venv/bin/activate && pip install pandas nba_api"
  exit 1
fi

"${CMD[@]}" 2>&1 | tee "${LOG_FILE}"
STATUS=${PIPESTATUS[0]}

if [[ ${INTERRUPTED} -eq 1 || ${STATUS} -eq 130 || ${STATUS} -eq 143 ]]; then
  echo "[INTERRUPTED] Download interrupted. Re-run this script to resume from checkpoint."
  exit 130
fi

if [[ ${STATUS} -eq 0 ]]; then
  echo "[DONE] Download completed successfully."
else
  echo "[ERROR] Download failed with exit code ${STATUS}. Check ${LOG_FILE}."
fi

exit "${STATUS}"
