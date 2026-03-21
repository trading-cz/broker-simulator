# GitHub Actions / Workflows

This directory contains automated workflows for testing, validating, and maintaining the broker-simulator project.

## Workflow Descriptions

### 1. **alpaca-historical-behavior.yml**

**Purpose**: Document exact behavior of the **real Alpaca Data API v2** (REST endpoints) across all success and error scenarios.

**What it does**:
- Calls the live Alpaca Data API with 59 different test cases
- Tests authentication: valid credentials, wrong key, wrong secret, no credentials, empty credentials
- Tests REST endpoints: bars, snapshots, latest quotes/trades/bars, historical trades/quotes, feed parameter variations
- Tests edge cases: invalid symbols, date ranges, pagination, timeframes, sort orders
- Tests how the alpaca-py SDK wraps errors (401, 404, 429, etc.)
- Outputs full HTTP responses and error details

**When to run**: Manually (`workflow_dispatch`) whenever you need to understand current Alpaca API behavior

**Output**: Downloadable artifact `alpaca-historical-behavior` (text log, 30-day retention)

**Script**: `.github/script/alpaca_historical_behavior.py`

---

### 2. **alpaca-stream-behavior.yml**

**Purpose**: Document exact protocol behavior of **Alpaca's WebSocket Streaming API** (WSS) across all success and error scenarios.

**What it does**:
- Connects to the live Alpaca streaming endpoint using raw websockets (bypasses SDK to capture exact protocol)
- Tests 38 scenarios covering:
  - Connection: valid/invalid feeds, protocol handshake
  - Authentication: valid credentials, wrong key, wrong secret, missing fields, wrong action
  - Subscription: valid symbols, invalid symbols, wildcard, empty, multiple data types (trades/quotes/bars)
  - Protocol edge cases: double auth, subscribe before auth, unsubscribe, garbage data
  - Data reception: captures actual trade/quote/bar message formats during market hours
  - SDK behavior: how alpaca-py SDK handles connection errors
- Outputs raw JSON messages and error details

**When to run**: Manually (`workflow_dispatch`) whenever you need to understand current Alpaca streaming behavior

**Output**: Downloadable artifact `alpaca-stream-behavior` (text log, 30-day retention)

**Script**: `.github/script/alpaca_stream_behavior.py`

---

### 3. **fetch-alpaca-test-data.yml**

**Purpose**: Automatically fetch real Alpaca market data and seed the broker-simulator with production-like test data.

**What it does**:
- Fetches historical daily bars from Alpaca for configured symbols (default: SPY, QQQ, MCD, KO, IBM, TSLA)
- Synthesizes "morning snapshots" from the bar data (latest trade/quote at market open, daily bar, previous daily bar)
- Writes data to `data/alpaca/bars_daily.jsonl` and `data/alpaca/snapshots.jsonl`
- Single-day mode (default): appends yesterday's data (used for daily refresh)
- Multi-day mode: truncates and backfills N trading days (used for bulk historical backfill)
- Auto-commits updated data files back to the repository

**When to run**:
- **Automatically**: Daily at 06:00 UTC (after US market close) via schedule
- **Manually**: Use workflow_dispatch with `days` parameter to backfill historical data (e.g., `--days 80` for 80 days)

**Inputs**:
- `days`: Number of calendar days to fetch (default: 1)
- `symbols`: Comma-separated symbols (default: SPY,QQQ,MCD,KO,IBM,TSLA)

**Output**: Downloadable artifact `alpaca-test-data` (JSONL files, 5-day retention) + auto-commit to repository

**Script**: `.github/script/fetch_alpaca_test_data.py`

---

## Using These Workflows

### Prerequisites
- GitHub repository secrets `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` must be set
- For the behavior probes: valid Alpaca credentials (live or paper trading account)
- For fetch-alpaca-test-data: valid Alpaca credentials with sufficient API access

### Manual Trigger (All Workflows)
1. Go to **Actions** tab in GitHub
2. Select the workflow name (e.g., "Alpaca Historical Behavior Probe")
3. Click **Run workflow** → **Run workflow** button
4. For fetch-alpaca-test-data, optionally set `days` and `symbols` inputs

### Automated Schedule
- `fetch-alpaca-test-data` runs daily at 06:00 UTC automatically
- The two behavior probes are manual-only

### Viewing Results
1. Click the workflow run
2. Scroll to **Artifacts** section
3. Download the log files to review output locally

---

## Why These Workflows Matter

### For Understanding Alpaca API Behavior
The two behavior probe workflows capture **exact HTTP responses and protocol messages** from the real Alpaca API. This is essential for:
- Building a faithful mock/simulator (broker-simulator) that can be used in CI/tests
- Understanding error codes, edge cases, and response formats
- Documenting what the real API does vs. what we expect

### For Maintaining Fresh Test Data
The fetch-alpaca-test-data workflow ensures broker-simulator always has:
- Real market data (real prices, volumes, timestamps)
- Data that can be used to replay realistic trading scenarios
- Data that stays current (daily automatic refresh)

---

## Example Workflow: Using Behavior Probes

1. **Run "Alpaca Historical Behavior Probe"** → Download log
2. Review the log to see:
   - What the API returns for valid requests (response shape, field names)
   - What HTTP status + JSON error bodies are returned for failures
   - How alpaca-py SDK exceptions map to underlying API errors
3. **Implement matching behavior in broker-simulator**:
   - Update `simulator/alpaca/rest.py` to return the correct HTTP status codes and JSON structures
   - Update `simulator/alpaca/wss.py` to send the exact same message formats
4. **Verify**: Run broker-simulator locally and test against the real patterns from the log

---

## File Structure

```
.github/
  workflows/
    fetch-alpaca-test-data.yml          ← Daily data refresh
    alpaca-historical-behavior.yml      ← REST API behavior probe
    alpaca-stream-behavior.yml          ← WSS streaming behavior probe
  script/
    fetch_alpaca_test_data.py           ← Fetches real data from Alpaca
    alpaca_historical_behavior.py       ← Tests 59 REST scenarios
    alpaca_stream_behavior.py           ← Tests 38 WSS scenarios
```
