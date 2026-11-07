# RiceQuant History Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a stable RiceQuant historical minute sync runner with day-level resume, quota protection, manifest tracking, visible logs, and Feishu-compatible notifications.

**Architecture:** Keep RiceQuant data syncing inside the existing `RiceQuantStockMinuteSyncJob`; add a separate history orchestration module/script that calls the existing pipeline one trading day at a time. Use DuckDB manifest tables for recovery and reporting, and extend `Notifier` with structured methods while preserving `send()`.

**Tech Stack:** Python 3.11, click/script entrypoint, DuckDB via existing `MetaStore` DB path, loguru, httpx, pytest, existing `Pipeline` and RiceQuant sync jobs.

---

## Implementation Update: Notification Channels

The notification design was revised after the initial plan: `zer0share.notifier` now exposes explicit `WeComNotifier`, `FeishuNotifier`, `CompositeNotifier`, and `NullNotifier` implementations. Enterprise WeChat uses the existing text webhook payload, while Feishu uses the `zer0factor`-style interactive card payload. Config supports `[notifier.wecom]` and `[notifier.feishu]`, with legacy `[notifier].wecom_webhook_url` still mapped to the WeCom channel.

## File Map

- Modify `zer0share/notifier.py`: keep `send(message)` and add `notify_start`, `notify_progress`, `notify_stage_done`, `notify_error`; change log wording from 企业微信 to webhook/飞书-compatible.
- Modify `zer0share/config.py`: support both old `[notifier].wecom_webhook_url` and new `[notifier].webhook_url`, keeping backward compatibility.
- Modify `config/settings.example.toml`: document the Feishu-compatible webhook URL and existing compatibility key.
- Create `zer0share/ricequant_history.py`: chunk planning, quota byte parsing, manifest store, parquet validation, day/month orchestration, logging.
- Create `scripts/sync_ricequant_history.py`: command-line entrypoint for long-running historical sync.
- Create `tests/test_ricequant_history.py`: tests for chunking, byte parsing, manifest, resume, quota stop, and logging callbacks.
- Modify `tests/test_notifier.py`: tests for structured notification methods and payload compatibility.
- Modify `tests/test_config.py`: tests for new notifier URL key compatibility.

## Task 1: Notifier Structured Methods And Feishu-Compatible Wording

**Files:**
- Modify: `zer0share/notifier.py`
- Test: `tests/test_notifier.py`

- [ ] **Step 1: Write failing tests**

Add tests:

```python
def test_notify_start_posts_stage_details():
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    with patch("httpx.post", return_value=mock_response) as mock_post:
        n.notify_start("ricequant_history", {"range": "20160101~20160131"})
    content = mock_post.call_args[1]["json"]["text"]["content"]
    assert "[zer0share · ricequant_history]" in content
    assert "开始" in content
    assert "range: 20160101~20160131" in content


def test_notify_progress_posts_counts_and_percentage():
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    with patch("httpx.post", return_value=mock_response) as mock_post:
        n.notify_progress("ricequant_history", 3, 12)
    content = mock_post.call_args[1]["json"]["text"]["content"]
    assert "进度 25%" in content
    assert "(3/12)" in content


def test_notify_stage_done_posts_summary_and_elapsed():
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    with patch("httpx.post", return_value=mock_response) as mock_post:
        n.notify_stage_done("2026-05", {"行数": "22,475,520", "流量": "651 MiB"}, 257.4)
    content = mock_post.call_args[1]["json"]["text"]["content"]
    assert "[zer0share · 2026-05]" in content
    assert "完成" in content
    assert "行数: 22,475,520" in content
    assert "耗时: 257.4s" in content
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_notifier.py -q
```

Expected: fail with `AttributeError` for missing `notify_start`.

- [ ] **Step 3: Implement notifier methods**

Add private `_post_text(text)` and `_prefix(stage)` helpers. Keep `send()` delegating to `_post_text(f"[zer0share] {message}")`.

- [ ] **Step 4: Run notifier tests and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_notifier.py -q
```

Expected: all notifier tests pass.

Commit:

```bash
git add zer0share/notifier.py tests/test_notifier.py
git commit -m "feat: add structured webhook notifications"
```

## Task 2: Notifier Config Compatibility

**Files:**
- Modify: `zer0share/config.py`
- Modify: `config/settings.example.toml`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write failing tests**

Add:

```python
def test_load_config_accepts_notifier_webhook_url(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML.replace(
            'wecom_webhook_url = "https://example.com/webhook"',
            'webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/example"',
        ),
        encoding="utf-8",
    )
    cfg = load_config(cfg_file)
    assert cfg.wecom_webhook_url == "https://open.feishu.cn/open-apis/bot/v2/hook/example"
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_config.py::test_load_config_accepts_notifier_webhook_url -q
```

Expected: fail because `wecom_webhook_url` is required.

- [ ] **Step 3: Implement compatibility parser**

In `load_config`, derive URL as:

```python
notifier_raw = raw["notifier"]
webhook_url = notifier_raw.get("webhook_url", notifier_raw.get("wecom_webhook_url", ""))
```

Keep the `Config.wecom_webhook_url` field name for now to avoid broad churn.

- [ ] **Step 4: Update example config and commit**

Set:

```toml
[notifier]
webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_KEY"
enabled = false
```

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_config.py -q
```

Commit:

```bash
git add zer0share/config.py config/settings.example.toml tests/test_config.py
git commit -m "feat: accept generic webhook notifier config"
```

## Task 3: RiceQuant History Manifest And Utilities

**Files:**
- Create: `zer0share/ricequant_history.py`
- Test: `tests/test_ricequant_history.py`

- [ ] **Step 1: Write failing utility and manifest tests**

Cover:

```python
def test_parse_bytes_supports_gib_suffixes():
    assert parse_bytes("50G") == 50 * 1024**3
    assert parse_bytes("512M") == 512 * 1024**2


def test_month_chunks_split_range():
    assert month_chunks("20260506", "20260621") == [
        ("20260506", "20260531"),
        ("20260601", "20260621"),
    ]


def test_manifest_records_day_success(tmp_path):
    manifest = RiceQuantHistoryManifest(tmp_path / "meta.duckdb")
    manifest.record_day_success(
        trade_date="20260506",
        rows=100,
        symbols=2,
        parquet_size=4096,
        bytes_used_before=10,
        bytes_used_after=20,
        elapsed_seconds=1.5,
    )
    row = manifest.get_day("20260506")
    assert row["status"] == "success"
    assert row["rows"] == 100
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_ricequant_history.py -q
```

Expected: fail because module does not exist.

- [ ] **Step 3: Implement utilities and manifest**

Create:

```python
def parse_bytes(value: str | None) -> int | None: ...
def month_chunks(start_date: str, end_date: str) -> list[tuple[str, str]]: ...
class RiceQuantHistoryManifest: ...
```

Manifest creates `ricequant_history_days` and `ricequant_history_chunks` tables if missing.

- [ ] **Step 4: Run tests and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_ricequant_history.py -q
```

Commit:

```bash
git add zer0share/ricequant_history.py tests/test_ricequant_history.py
git commit -m "feat: add ricequant history manifest"
```

## Task 4: Day-Level History Runner With Logs

**Files:**
- Modify: `zer0share/ricequant_history.py`
- Test: `tests/test_ricequant_history.py`

- [ ] **Step 1: Write failing runner tests**

Use fake pipeline/calendar/fetcher/notifier and verify:

```python
def test_runner_skips_existing_valid_partition(tmp_path):
    # existing parquet with rows for 20260506
    # runner should record skipped and not call pipeline.run


def test_runner_logs_each_day_start_and_finish(tmp_path, caplog):
    # runner should log "开始同步 20260506" and "完成 20260506"


def test_runner_retries_failed_day_then_records_success(tmp_path):
    # first pipeline.run raises RuntimeError, second succeeds
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_ricequant_history.py -q
```

Expected: fail because runner does not exist.

- [ ] **Step 3: Implement runner**

Add `RiceQuantHistoryRunner` with:

```python
runner.run(start_date, end_date, chunk="month", max_bytes=None, stop_remaining_below=None, retries=3)
```

Logging requirements:

- log start/end for whole run
- log start/end for every month
- log start/end/skipped/failed for every trading day
- log quota before/after each day
- use `logger.info`, `logger.warning`, `logger.error`

- [ ] **Step 4: Run tests and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_ricequant_history.py -q
```

Commit:

```bash
git add zer0share/ricequant_history.py tests/test_ricequant_history.py
git commit -m "feat: add ricequant history runner"
```

## Task 5: CLI Script

**Files:**
- Create: `scripts/sync_ricequant_history.py`
- Test: `tests/test_ricequant_history.py`

- [ ] **Step 1: Write failing script argument test**

Add a test that imports `build_parser()` and checks defaults:

```python
def test_history_script_parser_defaults():
    parser = build_parser()
    args = parser.parse_args(["--start-date", "20160101", "--end-date", "20160131"])
    assert args.chunk == "month"
    assert args.retries == 3
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_ricequant_history.py -q
```

Expected: fail because script does not exist.

- [ ] **Step 3: Implement script**

Script responsibilities:

- parse `--start-date`, `--end-date`, `--chunk month`, `--max-bytes`, `--stop-remaining-below`, `--retries`, `--config`
- call `init_logger(cfg.log_path)` before running
- build `Pipeline` with RiceQuant source
- create `RiceQuantHistoryRunner`
- print start and final summary to stdout using `print()` and loguru

- [ ] **Step 4: Run focused tests and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_ricequant_history.py tests/test_notifier.py tests/test_config.py -q
```

Commit:

```bash
git add scripts/sync_ricequant_history.py tests/test_ricequant_history.py
git commit -m "feat: add ricequant history sync script"
```

## Task 6: End-To-End Smoke Verification

**Files:**
- No code expected unless verification finds a bug.

- [ ] **Step 1: Run unit tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_ricequant_history.py tests/test_notifier.py tests/test_config.py tests/test_ricequant_sync.py tests/test_ricequant_fetcher.py -q
```

Expected: all pass.

- [ ] **Step 2: Run no-op smoke against already synced May range**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run python scripts/sync_ricequant_history.py \
  --start-date 20260506 \
  --end-date 20260508 \
  --max-bytes 1G \
  --stop-remaining-below 8G
```

Expected:

- console prints visible progress
- log file contains day/month progress
- existing partitions are skipped
- no additional RiceQuant minute data is pulled

- [ ] **Step 3: Commit final fixes if needed**

If code changed after smoke:

```bash
git add <changed-files>
git commit -m "fix: harden ricequant history smoke path"
```

## Self-Review

- Spec coverage: day-level resume, quota protection, month progress, manifest, Feishu-compatible notifications, and visible logs are covered.
- Placeholder scan: no placeholder tokens remain.
- Type consistency: `RiceQuantHistoryManifest`, `RiceQuantHistoryRunner`, `parse_bytes`, and `month_chunks` names are used consistently.
