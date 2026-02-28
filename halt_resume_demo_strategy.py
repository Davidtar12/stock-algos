#!/usr/bin/env python3
"""
IBKR demo strategy around EA's 14:34 ET news/halts:
 - Prefer single-call 1-second TRADES bars for price/volume
 - Fetch ticks only to extract IBKR Halted/Unhalted markers, then overlay onto bars
 - Fallback (if no markers): detect halts via zero-volume run >= min seconds
 - Compute OBV, OBV SMA(10), z-scores and slopes (OBV, price), volume z-score, and EWMA-smoothed OBV slope
 - Entry: immediate on halt_lift; otherwise require positive EWMA-smoothed OBV slope + vol_z > threshold
 - Exits (compare A/B/C/D/E):
     A) OBV < OBV_SMA(10)
     B) Volume exhaustion: very low volume + strong negative OBV momentum + price down
     C) Hard stop (e.g., -0.75%)
     D) Trailing stop: exit if price falls 5% from its post-entry peak
     E) Max hold time (e.g., 1500s)

Requires: pip install ib_insync pandas numpy vectorbt
"""

from __future__ import annotations

import sys
import argparse
from pathlib import Path
import math
import time

import numpy as np
import pandas as pd
import vectorbt as vbt
from ib_insync import IB, Stock
from contextlib import contextmanager
import json


def connect_any(ib: IB, ports=(7497, 7496, 4002, 4001), client_id: int = 21001) -> int | None:
    for p in ports:
        try:
            ib.connect("127.0.0.1", p, clientId=client_id, timeout=3)
            if ib.isConnected():
                print(f"Connected to IBKR on port {p}")
                return p
        except Exception as e:
            print(f"API connection failed on port {p}: {e}")
    print("Make sure API port on TWS/IBG is open")
    return None


class Pacer:
    """Conservative pacing guard to respect IB limits: ≤60/10min, ≤5/2s, ≥15s identical.

    Use one instance across all requests in this run.
    """

    def __init__(self, max_per_10min: int = 60, max_per_2s: int = 5, min_identical_gap: int = 15, hard_cap: int = 0):
        self.window10 = 600.0
        self.window2 = 2.0
        self.max10 = max_per_10min
        self.max2 = max_per_2s
        self.min_identical = min_identical_gap
        self.hard_cap = hard_cap
        self.times: list[float] = []
        self.last_by_key: dict[tuple[str, str, str], float] = {}
        self.total = 0

    def _purge(self, now: float) -> None:
        cutoff = now - self.window10
        # keep only recent timestamps within 10 mins
        self.times = [t for t in self.times if t >= cutoff]

    def wait_or_abort(self, key: tuple[str, str, str]) -> bool:
        if self.hard_cap and self.total >= self.hard_cap:
            return False
        now = time.time()
        # identical-request 15s gap
        last_t = self.last_by_key.get(key)
        if last_t is not None:
            gap = now - last_t
            if gap < self.min_identical:
                time.sleep(self.min_identical - gap)
                now = time.time()
        # enforce 2s and 10-min windows
        self._purge(now)
        count2 = sum(1 for t in self.times if now - t <= self.window2)
        if count2 >= self.max2:
            # sleep until we drop under 5/2s
            oldest2 = min(t for t in self.times if now - t <= self.window2)
            sleep_for = (oldest2 + self.window2) - now + 0.01
            time.sleep(max(sleep_for, 0))
            now = time.time()
        self._purge(now)
        if len(self.times) >= self.max10:
            # sleep until earliest timestamp leaves the 10-min window
            oldest10 = self.times[0]
            sleep_for = (oldest10 + self.window10) - now + 0.01
            time.sleep(max(sleep_for, 0))
        return True

    def record(self, key: tuple[str, str, str]) -> None:
        now = time.time()
        self.times.append(now)
        self.last_by_key[key] = now
        self.total += 1


class Profiler:
    """Lightweight timing profiler.

    Usage:
        prof = Profiler()
        with prof.cm("step-name"):
            ...
        prof.print_report()
    """

    def __init__(self, live: bool = False):
        self.events: dict[str, dict[str, float | int]] = {}
        self.live = live

    @contextmanager
    def cm(self, label: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self.add(label, dt)

    def add(self, label: str, duration: float):
        rec = self.events.get(label)
        if rec is None:
            rec = {"total": 0.0, "count": 0}
            self.events[label] = rec
        rec["total"] = float(rec["total"]) + float(duration)
        rec["count"] = int(rec["count"]) + 1
        if self.live:
            total = rec["total"]
            cnt = rec["count"]
            avg = (total / cnt) if cnt else 0.0
            print(f"[profile] {label}: +{duration:.4f}s (total={total:.4f}s, count={cnt}, avg={avg:.6f}s)")

    def to_rows(self):
        rows = [
            {"label": k, "total_sec": round(v["total"], 6), "count": v["count"]}
            for k, v in self.events.items()
        ]
        rows.sort(key=lambda r: r["total_sec"], reverse=True)
        return rows

    def print_report(self, extra: dict | None = None):
        rows = self.to_rows()
        print("\n=== Profiling Breakdown (by total time) ===")
        for r in rows:
            avg = (r["total_sec"] / r["count"]) if r["count"] else 0.0
            print(f"- {r['label']}: total={r['total_sec']:.4f}s count={r['count']} avg={avg:.6f}s")
        if extra:
            print("--- Extra Metrics ---")
            for k, v in extra.items():
                print(f"{k}: {v}")
        print("=== End Profiling ===\n")
        return rows


def fetch_bars_1s(
    ib: IB,
    contract: Stock,
    start_et: pd.Timestamp,
    end_et: pd.Timestamp,
    use_rth: int = 0,
) -> pd.DataFrame:
    """Fetch 1-second historical bars in a SINGLE request (<=2000 seconds window).

    Returns DataFrame indexed by time (America/New_York) with columns close, volume.
    """
    duration_sec = int((end_et - start_et).total_seconds())
    if duration_sec <= 0:
        return pd.DataFrame(columns=["time", "close", "volume"]).set_index("time")
    if duration_sec > 2000:
        raise ValueError(
            f"Window {duration_sec}s exceeds 2000s single-request limit; please shorten or split the window."
        )
    end_str = end_et.tz_convert("US/Eastern").strftime("%Y%m%d %H:%M:%S US/Eastern")
    bars = ib.reqHistoricalData(
        contract=contract,
        endDateTime=end_str,
        durationStr=f"{min(duration_sec, 2000)} S",
        barSizeSetting="1 secs",
        whatToShow="TRADES",
        useRTH=use_rth,
        formatDate=2,
    )
    if not bars:
        return pd.DataFrame(columns=["time", "close", "volume"]).set_index("time")
    rows: list[dict] = []
    for b in bars:
        dt = pd.to_datetime(getattr(b, "date", None))
        if getattr(dt, "tzinfo", None) is None or dt.tzinfo is None:
            dt = dt.tz_localize("UTC")
        else:
            dt = dt.tz_convert("UTC")
        dt_ny = dt.tz_convert("America/New_York")
        rows.append({
            "time": dt_ny,
            "close": float(getattr(b, "close", np.nan)),
            "volume": float(getattr(b, "volume", 0.0) or 0.0),
        })
    df = pd.DataFrame(rows).sort_values("time").set_index("time")
    # Build a full 1-second index in the same timezone as the bars' index
    idx_tz = df.index.tz
    def _as_tz(ts: pd.Timestamp, tz):
        ts = pd.to_datetime(ts)
        if ts.tzinfo is None:
            return ts.tz_localize(tz)
        return ts.tz_convert(tz)
    start_idx = _as_tz(start_et, idx_tz)
    end_idx = _as_tz(end_et, idx_tz)
    full_index = pd.date_range(start=start_idx, end=end_idx, freq="1s", tz=idx_tz)
    df = df.reindex(full_index)
    df.index.name = "time"
    # Fill initial NaNs by backfilling once, then forward-fill across gaps
    df["close"] = df["close"].bfill().ffill()
    df["volume"] = df["volume"].fillna(0.0)
    return df


def normalize_tick_time(val) -> pd.Timestamp:
    dt = pd.to_datetime(val)
    if getattr(dt, "tzinfo", None) is None or dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    else:
        dt = dt.tz_convert("UTC")
    return dt


def fetch_ticks_for_markers(
    ib: IB,
    contract: Stock,
    start_et: pd.Timestamp,
    end_et: pd.Timestamp,
    use_rth: int = 0,
    pacer: Pacer | None = None,
    pause_sec: float = 2.5,
    max_requests: int = 0,
    profiler: Profiler | None = None,
) -> pd.DataFrame:
    """Fetch TRADES historical ticks over [start_et, end_et] only to extract halt markers.

    Returns a DataFrame with time (NY tz), price, size.
    """
    pacer = pacer or Pacer(hard_cap=max_requests)
    rows: list[dict] = []
    start_utc = start_et.tz_convert("UTC")
    end_utc = end_et.tz_convert("UTC")
    cursor = start_utc
    key = (getattr(contract, "symbol", ""), getattr(contract, "exchange", ""), "TRADES")
    while cursor < end_utc:
        start_str = cursor.tz_convert("US/Eastern").strftime("%Y%m%d %H:%M:%S US/Eastern")
        t_wait0 = time.perf_counter()
        ok = pacer.wait_or_abort(key)
        t_wait1 = time.perf_counter()
        if profiler is not None:
            profiler.add("pacer_wait", t_wait1 - t_wait0)
        if not ok:
            break
        try:
            t_req0 = time.perf_counter()
            ticks = ib.reqHistoricalTicks(
                contract=contract,
                startDateTime=start_str,
                endDateTime="",
                numberOfTicks=1000,
                whatToShow="TRADES",
                useRth=use_rth,
                ignoreSize=True,
            )
            t_req1 = time.perf_counter()
            if profiler is not None:
                profiler.add("reqHistoricalTicks", t_req1 - t_req0)
        except Exception as e:
            # swallows intermittent pacing errors and stops
            break
        pacer.record(key)
        if not ticks:
            break
        last_ts = None
        for t in ticks:
            ts = getattr(t, "time", None)
            last_ts = ts
            dt = normalize_tick_time(ts)
            if not (start_utc <= dt <= end_utc):
                continue
            rows.append({
                "time": dt,
                "price": getattr(t, "price", None),
                "size": getattr(t, "size", None),
            })
        if last_ts is None:
            break
        dt_last = normalize_tick_time(last_ts)
        cursor = dt_last + pd.Timedelta(seconds=1)
        if profiler is not None:
            profiler.add("ticks_rows", 0.0 if ticks is None else 0.0 + 0.0)
            # Using duration 0.0 just to increment count and show per-chunk
            # We can attach size info via a print if live
            if profiler.live:
                print(f"[profile] ticks_chunk: rows={len(ticks)} last={dt_last.tz_convert('America/New_York')}")
        t_sleep0 = time.perf_counter()
        time.sleep(pause_sec)
        t_sleep1 = time.perf_counter()
        if profiler is not None:
            profiler.add("post_chunk_sleep", t_sleep1 - t_sleep0)

    if not rows:
        return pd.DataFrame(columns=["time", "price", "size"])
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"]).dt.tz_convert("America/New_York")
    df = df.sort_values("time").reset_index(drop=True)
    return df


def extract_halt_markers_from_ticks(ticks: pd.DataFrame) -> list[dict]:
    """Return list of {halted_at, unhalted_at} times (same tz as ticks['time']).

    Heuristic: find consecutive zero-price & zero-size ticks and pair them as [Halted, Unhalted].
    """
    out: list[dict] = []
    if ticks is None or ticks.empty:
        return out
    df = ticks.copy().sort_values("time").reset_index(drop=True)
    if not set(["time", "price", "size"]).issubset(df.columns):
        return out
    zero_zero = df["price"].fillna(0).eq(0) & df["size"].fillna(0).eq(0)
    idx = zero_zero[zero_zero].index.to_list()
    i = 0
    while i < len(idx):
        halted_time = df.loc[idx[i], "time"]
        unhalted_time = df.loc[idx[i + 1], "time"] if i + 1 < len(idx) else None
        i += 2 if i + 1 < len(idx) else 1
        if pd.notna(halted_time) and pd.notna(unhalted_time):
            out.append({"halted_at": halted_time, "unhalted_at": unhalted_time})
    return out


def _build_halt_columns(is_halted: pd.Series, volume: pd.Series) -> pd.DataFrame:
    dfh = pd.DataFrame(index=is_halted.index)
    dfh["is_halted"] = is_halted.fillna(False)
    grp = dfh["is_halted"].ne(dfh["is_halted"].shift()).cumsum()
    halted_grp = grp.where(dfh["is_halted"])  # only halted runs
    factorized, _ = pd.factorize(halted_grp)
    dfh["halt_id"] = pd.Series(factorized, index=dfh.index)
    dfh.loc[dfh["halt_id"] == -1, "halt_id"] = pd.NA
    vol = volume.fillna(0)
    dfh["halt_started"] = dfh["is_halted"] & (~dfh["is_halted"].shift(fill_value=False))
    dfh["halt_lifted"] = (~dfh["is_halted"]) & (dfh["is_halted"].shift(fill_value=False)) & vol.gt(0)
    return dfh[["is_halted", "halt_id", "halt_started", "halt_lifted"]]


def apply_halt_markers_to_bars(bars_1s: pd.DataFrame, markers: list[dict]) -> pd.DataFrame:
    if bars_1s.empty or not markers:
        return bars_1s
    df = bars_1s.copy()
    existing = df["is_halted"].copy() if "is_halted" in df.columns else pd.Series(False, index=df.index)
    marker_mask = pd.Series(False, index=df.index)
    for ev in markers:
        start = pd.to_datetime(ev.get("halted_at"))
        end = pd.to_datetime(ev.get("unhalted_at"))
        try:
            start = start.tz_convert(df.index.tz)
            end = end.tz_convert(df.index.tz)
        except Exception:
            pass
        marker_mask |= (df.index >= start.floor("S")) & (df.index < end.floor("S"))
    combined = existing.fillna(False) | marker_mask
    rebuilt = _build_halt_columns(combined, df.get("volume", pd.Series(0, index=df.index)))
    for c in rebuilt.columns:
        df[c] = rebuilt[c]
    return df


def detect_halts(bars_1s: pd.DataFrame, min_seconds: int = 120) -> pd.DataFrame:
    """Detect halts as continuous zero-volume runs >= min_seconds and add flags.

    Adds: is_halted, halt_id, halt_started, halt_lifted
    """
    df = bars_1s.copy()
    if df.empty:
        for c in ["is_halted", "halt_id", "halt_started", "halt_lifted"]:
            df[c] = []
        return df

    vol = df["volume"].fillna(0)
    zero_vol = vol.eq(0)
    grp = zero_vol.ne(zero_vol.shift()).cumsum()
    run_lengths = zero_vol.groupby(grp).transform("size")
    is_halted = zero_vol & (run_lengths >= int(min_seconds))
    df["is_halted"] = is_halted

    halted_grp = grp.where(is_halted)
    factorized, _ = pd.factorize(halted_grp)
    df["halt_id"] = pd.Series(factorized, index=df.index)
    df.loc[df["halt_id"] == -1, "halt_id"] = pd.NA

    df["halt_started"] = df["is_halted"] & (~df["is_halted"].shift(fill_value=False))
    df["halt_lifted"] = (~df["is_halted"]) & (df["is_halted"].shift(fill_value=False)) & vol.gt(0)
    return df


def rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """Rolling least-squares slope per 1s unit over the given window.

    Uses x = [0, 1, ..., window-1]. Returns NaN until full window is available.
    """
    if window <= 1:
        return pd.Series(np.nan, index=series.index, dtype=float)

    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    denom = np.sum((x - x_mean) ** 2)

    def _slope(y: np.ndarray) -> float:
        if np.any(np.isnan(y)):
            return np.nan
        y_mean = y.mean()
        num = np.sum((x - x_mean) * (y - y_mean))
        if denom == 0:
            return 0.0
        return num / denom

    return series.rolling(window=window, min_periods=window).apply(_slope, raw=True)


def rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """Rolling z-score (value - mean) / std with min_periods=window.
    Returns NaN until full window; where std=0, returns 0.
    """
    if window <= 1:
        return pd.Series(np.nan, index=series.index, dtype=float)
    rol = series.rolling(window=window, min_periods=window)
    mean = rol.mean()
    std = rol.std(ddof=0)
    z = (series - mean) / std.replace(0, np.nan)
    return z.fillna(0.0)


def compute_features(df: pd.DataFrame,
                     obv_sma_window: int = 10,
                     obv_slope_window: int = 9,
                     obv_z_window: int = 120,
                     price_slope_window: int = 5,
                     price_ret_z_window: int = 120,
                     vol_z_window: int = 60,
                     obv_ewm_span: int = 9) -> pd.DataFrame:
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    obv = vbt.OBV.run(close, volume).obv.rename("obv")
    obv_sma = obv.rolling(window=obv_sma_window, min_periods=obv_sma_window).mean().rename("obv_sma_10")
    obv_slope = rolling_slope(obv, obv_slope_window).rename("obv_slope")
    obv_ewm = obv.ewm(span=obv_ewm_span, adjust=False).mean().rename("obv_ewm")
    obv_slope_smooth = rolling_slope(obv_ewm, obv_slope_window).rename("obv_slope_smooth")
    obv_z = rolling_zscore(obv, obv_z_window).rename("obv_z")

    price_slope = rolling_slope(close, price_slope_window).rename("price_slope")
    ret = close.pct_change().fillna(0.0)
    ret_z = rolling_zscore(ret, price_ret_z_window).rename("price_ret_z")
    vol_z = rolling_zscore(volume, vol_z_window).rename("vol_z")

    out = pd.concat([df, obv, obv_sma, obv_slope, obv_ewm, obv_slope_smooth, obv_z, price_slope, ret_z, vol_z], axis=1)
    return out


def run_strategy(features: pd.DataFrame,
                 require_halt_lift: bool = True,
                 obv_slope_min: float = 0.0,
                 obv_z_min: float = 0.0,
                 price_slope_min: float = 0.0,
                 price_ret_z_min: float = 0.0,
                 vol_z_min: float = 1.5,
                 stop_loss_pct: float = 0.0075,
                 max_hold_seconds: int = 180,
                 earliest_entry_time: pd.Timestamp | None = None) -> dict:
    """Simulate single long entry/exit.

    Entry policy:
      - If any halt_lifted bar exists: enter immediately on the first such bar.
      - Otherwise (no halts): require positive EWMA-smoothed OBV slope and vol_z > vol_z_min,
        with optional mild price filters (price_slope/ret_z > mins).

                Exit policies (all evaluated from the bar AFTER entry):
                        A) obv < obv_sma_10
                        B) Volume exhaustion: low volume + strong negative OBV momentum + price down
                        C) Hard stop-loss (stop_loss_pct)
                        D) Trailing stop: 5% drawdown from post-entry peak
                        E) Max hold time (max_hold_seconds)
    """
    df = features.copy()
    # Normalize earliest_entry_time to the dataframe's timezone
    if earliest_entry_time is not None:
        try:
            if getattr(earliest_entry_time, "tzinfo", None) is None or earliest_entry_time.tzinfo is None:
                earliest_entry_time = earliest_entry_time.tz_localize(df.index.tz)
            else:
                earliest_entry_time = earliest_entry_time.tz_convert(df.index.tz)
        except Exception:
            pass
    entry_idx = None
    # Immediate on halt_lift when present
    if df.get("halt_lifted") is not None and df["halt_lifted"].any():
        lift_mask = df["halt_lifted"].copy()
        if earliest_entry_time is not None:
            lift_mask &= (df.index >= earliest_entry_time)
        entry_idx = df.index[lift_mask].min() if lift_mask.any() else None
    else:
        # If a halt lift is required and none detected, do not enter
        if require_halt_lift:
            entry_idx = None
        else:
            # Normal trading entry: smoothed OBV slope + volume confirmation (+ optional price filters)
            entry_cond = (
                (~df["is_halted"]) &
                (df["obv_slope_smooth"] > max(obv_slope_min, 0.0)) &
                (df["vol_z"] > vol_z_min) &
                (df["obv_z"] > obv_z_min) &
                (df["price_slope"] > price_slope_min) &
                (df["price_ret_z"] > price_ret_z_min)
            )
            if earliest_entry_time is not None:
                entry_cond &= (df.index >= earliest_entry_time)
            entry_idx = df.index[entry_cond].min() if entry_cond.any() else None

    trade = {
        "entry_time": None,
        "entry_price": None,
        "exit_time": None,
        "exit_price": None,
        "pnl": None,
        "ret_pct": None,
    }

    if entry_idx is None:
        print("No entry signal found in window.")
        df["position"] = 0
        return {"features": df, "trade": trade}

    entry_price = float(df.loc[entry_idx, "close"])
    # Build three exit scenarios starting from bar AFTER entry
    post = df.loc[df.index > entry_idx]
    # Exit A: OBV < OBV_SMA(10)
    exitA_mask = post["obv"].lt(post["obv_sma_10"])
    exitA_triggered = bool(exitA_mask.any())
    exitA_idx = post.index[exitA_mask].min() if exitA_triggered else df.index.max()
    # Exit B: Volume dries up AND momentum turns negative
    exitB_mask = (
        (post["vol_z"] < 0.5) &  # Very low volume
        (post["obv_slope_smooth"] < -50) &  # Strong negative OBV momentum
        (post["price_slope"] < 0)  # Price declining
    )
    exitB_triggered = bool(exitB_mask.any())
    exitB_idx = post.index[exitB_mask].min() if exitB_triggered else df.index.max()
    # Exit C: hard stop only
    # Compute running drawdown vs entry
    price_after = post["close"]
    dd = (price_after - entry_price) / entry_price
    hit_stop = dd.le(-abs(stop_loss_pct))
    exitC_triggered = bool(hit_stop.any())
    exitC_idx = post.index[hit_stop].min() if exitC_triggered else df.index.max()

    # Exit D: trailing stop from peak (5% drop from post-entry running peak)
    running_peak = price_after.expanding().max()
    trailing_dd = (price_after - running_peak) / running_peak
    hit_trailing = trailing_dd.le(-0.05)
    exitD_triggered = bool(hit_trailing.any())
    exitD_idx = post.index[hit_trailing].min() if exitD_triggered else df.index.max()

    # Exit E: max hold time only
    max_hold_end = entry_idx + pd.Timedelta(seconds=int(max_hold_seconds))
    e_mask = (post.index >= max_hold_end)
    exitE_triggered = bool(e_mask.any())
    exitE_idx = post.index[e_mask].min() if exitE_triggered else df.index.max()

    def _calc(trade_exit_idx):
        px = float(df.loc[trade_exit_idx, "close"]) if trade_exit_idx is not None else float(df.iloc[-1]["close"])
        pnl_ = px - entry_price
        ret_ = (pnl_ / entry_price) * 100.0 if entry_price else None
        return trade_exit_idx, px, pnl_, ret_

    exitA_idx, exitA_px, pnlA, retA = _calc(exitA_idx)
    exitB_idx, exitB_px, pnlB, retB = _calc(exitB_idx)
    exitC_idx, exitC_px, pnlC, retC = _calc(exitC_idx)
    exitD_idx, exitD_px, pnlD, retD = _calc(exitD_idx)
    exitE_idx, exitE_px, pnlE, retE = _calc(exitE_idx)

    # Choose best by return
    options = {
        "A": {"name": "OBV under SMA10", "exit_idx": exitA_idx, "exit_px": exitA_px, "pnl": pnlA, "ret": retA, "triggered": exitA_triggered},
        "B": {"name": "Volume exhaustion", "exit_idx": exitB_idx, "exit_px": exitB_px, "pnl": pnlB, "ret": retB, "triggered": exitB_triggered},
        "C": {"name": "Hard stop-loss", "exit_idx": exitC_idx, "exit_px": exitC_px, "pnl": pnlC, "ret": retC, "triggered": exitC_triggered},
        "D": {"name": "Trailing stop 5%", "exit_idx": exitD_idx, "exit_px": exitD_px, "pnl": pnlD, "ret": retD, "triggered": exitD_triggered},
        "E": {"name": "Max hold time", "exit_idx": exitE_idx, "exit_px": exitE_px, "pnl": pnlE, "ret": retE, "triggered": exitE_triggered},
    }
    # Prefer exits that actually triggered; if none triggered, fall back to best by return
    best_key = max(
        options.keys(),
        key=lambda k: (
            1 if options[k]["triggered"] else 0,
            options[k]["ret"] if options[k]["ret"] is not None else -1e9,
        ),
    )
    best = options[best_key]

    # Mark best position path on the dataframe
    df["position"] = 0
    best_exit_idx = best["exit_idx"]
    if entry_idx is not None and best_exit_idx is not None:
        df.loc[(df.index >= entry_idx) & (df.index <= best_exit_idx), "position"] = 1

    trade.update({
        "entry_time": entry_idx,
        "entry_price": entry_price,
        "exit_time": best_exit_idx,
        "exit_price": float(best["exit_px"]) if best["exit_px"] is not None else None,
        "pnl": float(best["pnl"]) if best["pnl"] is not None else None,
        "ret_pct": float(best["ret"]) if best["ret"] is not None else None,
        "best_exit": best_key,
        "best_exit_label": f"{best_key}: {options[best_key]['name']}",
        "best_exit_triggered": bool(best["triggered"]),
        "exit_reason": options[best_key]["name"] + (" (fallback end-of-window)" if not best["triggered"] else ""),
        "ret_A_pct": float(retA) if retA is not None else None,
        "ret_B_pct": float(retB) if retB is not None else None,
        "ret_C_pct": float(retC) if retC is not None else None,
        "ret_D_pct": float(retD) if retD is not None else None,
        "ret_E_pct": float(retE) if retE is not None else None,
        "A_triggered": exitA_triggered,
        "B_triggered": exitB_triggered,
        "C_triggered": exitC_triggered,
        "D_triggered": exitD_triggered,
        "E_triggered": exitE_triggered,
        "earliest_entry_time": earliest_entry_time,
    })
    return {"features": df, "trade": trade}


def parse_time(s: str, tz: str) -> pd.Timestamp:
    ts = pd.to_datetime(s)
    if ts.tzinfo is None:
        return ts.tz_localize(tz)
    return ts.tz_convert(tz)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="IBKR halt-resume demo strategy for EA (1-second bars)")
    p.add_argument("--symbol", default="EA")
    p.add_argument("--exchange", default="NASDAQ")
    p.add_argument("--currency", default="USD")
    p.add_argument("--start", default="2025-09-26 14:34:00")
    p.add_argument("--end", default="2025-09-26 14:50:00")
    p.add_argument("--tz", default="America/New_York")
    p.add_argument("--use-rth", type=int, choices=[0, 1], default=0)
    p.add_argument("--ports", nargs="*", type=int, default=[7497, 7496, 4002, 4001])
    p.add_argument("--client-id", type=int, default=21001)
    p.add_argument("--halt-min-seconds", type=int, default=120)
    p.add_argument("--use-ib-halt-ticks", action="store_true", default=True,
                   help="Fetch ticks to overlay IBKR Halted/Unhalted markers (preferred)")
    p.add_argument("--no-ib-halt-ticks", dest="use_ib_halt_ticks", action="store_false",
                   help="Disable tick fetch for halt markers (fallback to zero-volume only)")

    # Feature windows and thresholds
    p.add_argument("--obv-slope-window", type=int, default=9)
    p.add_argument("--obv-z-window", type=int, default=120)
    p.add_argument("--price-slope-window", type=int, default=5)
    p.add_argument("--price-ret-z-window", type=int, default=120)
    p.add_argument("--vol-z-window", type=int, default=60)
    p.add_argument("--obv-slope-min", type=float, default=0.0)
    p.add_argument("--obv-z-min", type=float, default=0.0)
    p.add_argument("--price-slope-min", type=float, default=0.0)
    p.add_argument("--price-ret-z-min", type=float, default=0.0)
    p.add_argument("--vol-z-min", type=float, default=1.5)
    p.add_argument("--obv-ewm-span", type=int, default=9, help="Span for EWMA smoothing of OBV before slope")
    p.add_argument("--stop-loss-pct", type=float, default=0.0075, help="Hard stop loss as decimal (0.0075=0.75%)")
    p.add_argument("--max-hold-seconds", type=int, default=1500, help="Maximum holding time in seconds for Exit E (1500 = 25 minutes)")
    p.add_argument("--require-halt-lift", action="store_true", help="Require halt_lifted==True on entry (recommended)")

    # News time and entry lag
    p.add_argument("--news-time", default=None, help="Timestamp of the news in the same tz as --tz (e.g., '2025-09-26 14:34:00')")
    p.add_argument("--entry-delay-seconds", type=int, default=60, help="Minimum seconds to wait after news-time before entry is allowed")

    p.add_argument("--output", default=None, help="Optional CSV path for features & signals")
    p.add_argument("--profile", action="store_true", help="Print a timing breakdown by step")
    p.add_argument("--profile-json", default=None, help="Optional path to write JSON profiling rows")
    p.add_argument("--profile-live", action="store_true", help="Stream timing lines while running")
    args = p.parse_args(argv)

    start_et = parse_time(args.start, args.tz)
    end_et = parse_time(args.end, args.tz)
    earliest_entry_time = None
    if args.news_time:
        try:
            news_ts = parse_time(args.news_time, args.tz)
            earliest_entry_time = news_ts + pd.Timedelta(seconds=int(args.entry_delay_seconds))
        except Exception as e:
            print(f"Warning: could not parse --news-time '{args.news_time}': {e}")
    if end_et <= start_et:
        print("End must be after start")
        return 2

    ib = IB()
    prof = Profiler(live=bool(args.profile_live))
    port = connect_any(ib, tuple(args.ports), args.client_id)
    if not port:
        print("Could not connect to IBKR on any port")
        return 1

    try:
        contract = Stock(args.symbol, args.exchange, args.currency)
        # Pacing guard shared across calls
        pacer = Pacer()
        # Single 1-second bar request for price/volume
        with prof.cm("fetch_bars_1s"):
            bars = fetch_bars_1s(ib, contract, start_et, end_et, use_rth=args.use_rth)
        print(f"Fetched {len(bars)} one-second bars")

        # Zero-volume fallback halt detection
        with prof.cm("detect_halts_zero_volume"):
            bars = detect_halts(bars, min_seconds=int(args.halt_min_seconds))

        # Prefer IBKR markers: fetch just the ticks for markers and overlay
        ticks = None
        markers = []
        if args.use_ib_halt_ticks:
            with prof.cm("fetch_ticks_for_markers"):
                ticks = fetch_ticks_for_markers(ib, contract, start_et, end_et, use_rth=args.use_rth, pacer=pacer, profiler=prof)
            with prof.cm("extract_halt_markers_from_ticks"):
                markers = extract_halt_markers_from_ticks(ticks)
            if markers:
                with prof.cm("apply_halt_markers_to_bars"):
                    bars = apply_halt_markers_to_bars(bars, markers)

        with prof.cm("compute_features"):
            feats = compute_features(
            bars,
            obv_sma_window=10,
            obv_slope_window=args.obv_slope_window,
            obv_z_window=args.obv_z_window,
            price_slope_window=args.price_slope_window,
            price_ret_z_window=args.price_ret_z_window,
            vol_z_window=args.vol_z_window,
            obv_ewm_span=args.obv_ewm_span,
        )

        with prof.cm("run_strategy"):
            result = run_strategy(
            feats,
            require_halt_lift=bool(args.require_halt_lift),
            obv_slope_min=args.obv_slope_min,
            obv_z_min=args.obv_z_min,
            price_slope_min=args.price_slope_min,
            price_ret_z_min=args.price_ret_z_min,
            vol_z_min=args.vol_z_min,
            stop_loss_pct=args.stop_loss_pct,
            max_hold_seconds=args.max_hold_seconds,
            earliest_entry_time=earliest_entry_time,
        )

        out_df: pd.DataFrame = result["features"]
        trade = result["trade"]

        if args.output:
            out_path = Path(args.output)
        else:
            s_tag = start_et.strftime("%Y-%m-%d_%H%M%S")
            e_tag = end_et.strftime("%H%M%S")
            out_path = Path(__file__).with_name(f"{args.symbol}_{s_tag}-{e_tag}_halt_resume_demo.csv")
        with prof.cm("write_csv"):
            out_df.to_csv(out_path, index=True)
        print(f"Saved features & signals to {out_path}")

        if trade["entry_time"] is not None:
            best_label = trade.get("best_exit_label", trade.get("best_exit", "?"))
            trig_note = "triggered" if trade.get("best_exit_triggered", False) else "fallback"
            print(
                f"TRADE (best {best_label}, {trig_note}): ENTRY {trade['entry_time']} @ {trade['entry_price']:.2f} | "
                f"EXIT {trade['exit_time']} @ {trade['exit_price']:.2f} | "
                f"PnL {trade['pnl']:.2f} ({trade['ret_pct']:.2f}%) | "
                f"ret_A={trade.get('ret_A_pct')}% ret_B={trade.get('ret_B_pct')}% ret_C={trade.get('ret_C_pct')}% ret_D={trade.get('ret_D_pct')}% ret_E={trade.get('ret_E_pct')}%"
            )
        else:
            print("TRADE: No entry signal in the window.")

        # Print profiling breakdown if requested
        if args.profile:
            extra = {
                "bars_count": len(bars) if bars is not None else 0,
                "ticks_count": len(ticks) if ticks is not None else 0,
                "markers_count": len(markers) if markers else 0,
                "pacer_total_requests": getattr(pacer, "total", 0),
            }
            rows = prof.print_report(extra)
            if args.profile_json:
                try:
                    with open(args.profile_json, "w", encoding="utf-8") as f:
                        json.dump({"rows": rows, "extra": extra}, f, ensure_ascii=False, indent=2)
                    print(f"Wrote profiling JSON to {args.profile_json}")
                except Exception as e:
                    print(f"Failed to write profiling JSON: {e}")

        return 0
    finally:
        if ib.isConnected():
            ib.disconnect()


if __name__ == "__main__":
    sys.exit(main())
