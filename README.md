# stock-algos

Algorithmic trading tools: buy-and-hold backtests, IBKR swing strategies, MT5 integration, correlation analysis.

## Scripts

| Script | Description |
|--------|-------------|
| `Stocks-over-trading-average-returns.py` | Compares buy-and-hold vs active trading returns over multiple years via MetaTrader 5 |
| `wk-stocks-over-trading10y-claude-fix.py` | 10-year buy-and-hold analysis with split-adjusted prices via MT5 |
| `buy-hold-multiple-splitadjusted-wk.py` | Buy-and-hold return calculator with automatic split adjustment |
| `correlation.py` | Correlation matrix between multiple stocks — fetched live via MT5 |
| `crisis-checkers.py` | Tests portfolio drawdown and recovery during historical market crises |
| `halt_resume_demo_strategy.py` | IBKR swing strategy: enters on momentum, halts on volatility, resumes on signal |
| `screener.py` | Stock screener filtering by configurable fundamental and technical criteria |
| `swingfinder.py` | Identifies swing trading entry/exit signals from price action |
| `dayprice.py` | Fetches current intraday prices for a watchlist |

## Prerequisites

- Python 3.9+
- **MetaTrader 5** terminal installed (for MT5 scripts) — Admiral Markets or compatible broker
- **Interactive Brokers TWS or Gateway** running locally (for IBKR scripts)
- `ADMIRAL_PASSWORD` environment variable set

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in ADMIRAL_PASSWORD
```

## Usage

```bash
# 10-year buy-and-hold analysis
python wk-stocks-over-trading10y-claude-fix.py

# Correlation matrix
python correlation.py

# IBKR swing strategy (requires TWS running on port 7497)
python halt_resume_demo_strategy.py
```

Edit the ticker list and date range at the top of each script.

## Notes

- MT5 scripts connect to MetaTrader 5 via the `MetaTrader5` Python package — MT5 terminal must be open and logged in.
- IBKR scripts use `ib_insync` and require TWS or IB Gateway running locally.
- Credentials use `os.getenv()` — never hardcoded.

## Built with

Python · MetaTrader5 · ib_insync · pandas  
AI-assisted development (Claude, GitHub Copilot) — architecture, requirements, QA validation and debugging by me.
