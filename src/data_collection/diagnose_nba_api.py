#!/usr/bin/env python3
from __future__ import annotations

import argparse
import platform
import socket
import ssl
import sys
import time
import traceback
from importlib.metadata import version

import requests
from nba_api.stats import endpoints
from nba_api.stats.library.http import NBAStatsHTTP


def enable_ipv4_only() -> None:
    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
        return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    socket.getaddrinfo = getaddrinfo_ipv4


def print_header() -> None:
    print("NBA API Diagnostics")
    print(f"python: {sys.version.split()[0]}")
    print(f"platform: {platform.platform()}")
    print(f"nba_api: {version('nba_api')}")
    print(f"requests: {version('requests')}")
    print("-" * 60)


def log_result(name: str, ok: bool, detail: str) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}: {detail}")


def run_dns_check(host: str) -> tuple[bool, str]:
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        addrs = sorted({info[4][0] for info in infos})
        return True, f"resolved {host} -> {', '.join(addrs[:4])}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_tls_check(host: str, timeout: float) -> tuple[bool, str]:
    try:
        context = ssl.create_default_context()
        started = time.perf_counter()
        with socket.create_connection((host, 443), timeout=timeout) as sock:
            with context.wrap_socket(sock, server_hostname=host) as wrapped:
                cipher = wrapped.cipher()[0] if wrapped.cipher() else "unknown"
                elapsed = time.perf_counter() - started
                return True, f"handshake ok ({elapsed:.2f}s), cipher={cipher}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_raw_stats_request(timeout: float) -> tuple[bool, str]:
    params = {
        "MeasureType": "Base",
        "PerMode": "PerGame",
        "PlusMinus": "N",
        "PaceAdjust": "N",
        "Rank": "N",
        "LeagueID": "00",
        "Season": "2024-25",
        "SeasonType": "Regular Season",
    }
    try:
        started = time.perf_counter()
        response = requests.get(
            "https://stats.nba.com/stats/leaguedashteamstats",
            params=params,
            headers=NBAStatsHTTP.headers,
            timeout=timeout,
        )
        elapsed = time.perf_counter() - started
        sample = response.text[:120].replace("\n", " ")
        if response.status_code != 200:
            return False, f"status={response.status_code}, body={sample!r}"
        data = response.json()
        keys = list(data.keys())[:5]
        return True, f"status=200 ({elapsed:.2f}s), json_keys={keys}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_nba_api_endpoint(timeout: float) -> tuple[bool, str]:
    try:
        started = time.perf_counter()
        response = endpoints.LeagueDashTeamStats(
            season="2024-25",
            season_type_all_star="Regular Season",
            per_mode_detailed="PerGame",
            timeout=timeout,
        )
        frames = response.get_data_frames()
        elapsed = time.perf_counter() - started
        rows = len(frames[0]) if frames else 0
        return True, f"frames={len(frames)}, rows_first_frame={rows}, {elapsed:.2f}s"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose NBA stats API connectivity and timeout issues.")
    parser.add_argument("--attempts", type=int, default=3, help="How many attempts per check.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Timeout seconds for network checks.")
    parser.add_argument("--sleep", type=float, default=1.5, help="Sleep seconds between attempts.")
    parser.add_argument("--traceback", action="store_true", help="Print full traceback for failures.")
    parser.add_argument("--force-ipv4", action="store_true", help="Force IPv4 DNS resolution for all checks.")
    args = parser.parse_args()
    if args.force_ipv4:
        enable_ipv4_only()

    checks = [
        ("DNS resolve stats.nba.com", lambda: run_dns_check("stats.nba.com")),
        ("TLS handshake stats.nba.com:443", lambda: run_tls_check("stats.nba.com", args.timeout)),
        ("Raw HTTPS request to leaguedashteamstats", lambda: run_raw_stats_request(args.timeout)),
        ("nba_api LeagueDashTeamStats call", lambda: run_nba_api_endpoint(args.timeout)),
    ]

    print_header()
    overall_failures = 0

    for check_name, check_fn in checks:
        successes = 0
        print(f"\n{check_name}")
        for attempt in range(1, args.attempts + 1):
            try:
                ok, detail = check_fn()
            except Exception as exc:
                ok = False
                detail = f"{type(exc).__name__}: {exc}"
                if args.traceback:
                    traceback.print_exc()
            log_result(f"attempt {attempt}/{args.attempts}", ok, detail)
            if ok:
                successes += 1
            if attempt < args.attempts:
                time.sleep(args.sleep)
        if successes == 0:
            overall_failures += 1
        print(f"[SUMMARY] {successes}/{args.attempts} succeeded")

    print("\n" + "-" * 60)
    if overall_failures == 0:
        print("Result: all checks had at least one success.")
    else:
        print(f"Result: {overall_failures} check group(s) had 0 successful attempts.")
        print("Likely causes: unstable network, ISP throttling, VPN/proxy filtering, or rate-limit pressure.")


if __name__ == "__main__":
    main()
