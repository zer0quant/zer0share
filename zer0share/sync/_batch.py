"""
sync/_batch.py — 批量取数型 sync job (吸收原 bulk 引擎的能力)。

由来: 原先 zer0share 有两套同步引擎 — `sync` (job 引擎, 逐日游标+重试+通知)
与 `sync-bulk` (bulk 引擎, 多日批量拉取+预算控制+内存安全写入)。两者表名
不重叠但代码重复, 且 bulk 侧 30 张表在 catalog 无规范声明、写旧单文件路径。

本模块把 bulk 的能力吸收进 job 引擎: BatchSyncJob 与 DailySyncJob 一样是
SyncJob (有 table_name / supports_date_range / run), 由同一套 sync 命令驱动,
但取数方式分三种:

  kind="date"    按日期批量拉多天 (batch_by_date) → store.merge() 写分区
  kind="stock"   按股票批量拉多只 (batch_by_stock) → store.merge() 写分区
  kind="quarter" 按季度拉 (batch_by_quarter) → store.merge() 写分区
  kind="snapshot" 一次性接口 (batch_snapshot) → SnapshotStore 写单文件

写入统一走 storage.DailyPartitionStore.merge(): 覆盖合并 (按业务主键去重,
keep=last) + schema 对齐 + 原子替换 + row-group 控制。原先这些逻辑散在
bulk.write_partitions 里, 现已上移到 storage 层成为所有 writer 的共享原语。

5000 行截断防护: Tushare 单次上限 5000 行, 满页即静默丢数据。所有按日取数
都走自动翻页 — 首屏恰为 5000 行时继续 offset 翻页 (paginate=None 自动模式)。
实测 adj_factor 单日真实 5578 行, 不翻页只拿到 5000 (丢 578 行 ≈ 10%)。
"""
import time
import datetime as dt
from typing import Callable

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.query.repository import DailyTableSpec
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SyncJob, PROGRESS_INTERVAL

# 流式落盘阈值: 每攒够这么多天/行就 merge 一次, 避免长历史回溯
# (数千天) 时一次中断丢光全部进度。trade_cal 实测单日约 20 行,
# moneyflow 单日约 5575 行 —— 取行阈值兜住「天少但行多」的表。
# 50 天 ≈ shibor 100 秒, 中断最多损失一批。
BATCH_FLUSH_DAYS = 50
BATCH_FLUSH_ROWS = 500_000
HEARTBEAT_SECONDS = 300.0  # 长跑心跳间隔, 避免 50 天窗口内静默像卡死

PAGE_SIZE = 5000        # Tushare 单次上限
MAX_PAGES = 200         # 单日翻页硬上限 (防死循环)
RETRY_DELAYS = (5, 15, 45)


# ---------------------------------------------------------------- 纯函数工具
# (从 bulk.py 原样搬来 — 吸收后所有取数路径共用)

def _err_key(err: BaseException) -> str:
    """把异常压成一个可读的错误分类键, 供批量汇总。

    tushare 的错误码在消息里 (如 upstream_pool_exhausted / unauthorized),
    比异常类名有信息量得多: 前者说明当天数据还没发布, 后者说明凭证失效要换 key。
    """
    msg = str(err)
    low = msg.lower()
    if "[" in msg and "]" in msg:
        code = msg[msg.index("[") + 1:msg.index("]")].strip()
        if code and " " not in code:
            return code
    for kw, label in [("unauthorized", "unauthorized(凭证/Token无效)"),
                      ("token已过期", "token已过期"),
                      ("token", "token(凭证/Token)"),
                      ("timeout", "timeout(网络)"),
                      ("connection", "connection(网络)")]:
        if kw in low:
            return label
    return f"{type(err).__name__}: {msg[:60]}"


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """对象列清洗: 空串→None, 能整体转数值的列转成数值, 否则统一为字符串"""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].replace("", None)
            try:
                converted = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                converted = df[col]
            if converted.dtype != "object":
                df[col] = converted
            else:
                df[col] = df[col].astype(str)
    return df


def _fetch_paged(pro, api_name: str, params: dict, *, paginate=None,
                 page_size: int = PAGE_SIZE, max_pages: int = MAX_PAGES,
                 sleep: float = 0.15):
    """单日/单码取数 + 5000 行截断防护。返回 (df, err)。

    paginate=None (默认) 自动模式: 先普通取一页 (不带 offset), 若返回行数正好
    等于接口上限就继续 offset 翻页。"正好 5000 行" 既可能是真实满页也可能是被
    截断 — 不翻页就分不清, 误判成未截断会静默丢数据。
    paginate=True 强制翻页; paginate=False 强制不翻页。
    """
    fn = getattr(pro, api_name)
    if paginate is False:
        try:
            return fn(**params), None
        except Exception as e:  # noqa: BLE001 — 由上层按 err 分类处理
            return None, e

    if paginate is True:
        try:
            df = fn(**{**params, "offset": 0, "limit": page_size})
        except Exception as e:  # noqa: BLE001
            return None, e
    else:
        try:
            df = fn(**params)
        except Exception as e:  # noqa: BLE001
            return None, e
        if df is None or len(df) < page_size:
            return df, None

    parts, offset = [], 0
    if df is not None and len(df):
        parts.append(df)
        offset = len(df)
    while len(parts) < max_pages:
        try:
            nxt = fn(**{**params, "offset": offset, "limit": page_size})
        except Exception as e:  # noqa: BLE001
            return None, e
        if nxt is None or len(nxt) == 0:
            break
        parts.append(nxt)
        if len(nxt) < page_size:  # 取不满 → 已是最后一页
            break
        offset += len(nxt)
        time.sleep(sleep)
    if not parts:
        return None, None
    return pd.concat(parts, ignore_index=True), None


def _call_with_retry(fn: Callable, retries: int = 2, backoff: float = 3.0):
    """带重试的单次调用。返回 (result, err) — err 让上层区分「数据未就绪」
    与「凭证/网络故障」: 前者正常等待, 后者要告警。"""
    last = None
    for k in range(retries + 1):
        try:
            return fn(), None
        except Exception as e:  # noqa: BLE001
            last = e
            if k >= retries:
                return None, last
            time.sleep(backoff * (k + 1))
    return None, last


def quarter_ends(start: str, end: str) -> list[str]:
    """[start, end] 内的季末日期 (0331/0630/0930/1231), 格式 YYYYMMDD。"""
    s = dateutil.parse_date(start)
    e = dateutil.parse_date(end)
    out = []
    for year in range(s.year, e.year + 1):
        for md in ("0331", "0630", "0930", "1231"):
            d = f"{year}{md}"
            if s.strftime("%Y%m%d") <= d <= e.strftime("%Y%m%d"):
                out.append(d)
    return out


# ---------------------------------------------------------------- job 实现

class BatchSyncJob(SyncJob):
    """批量取数型 sync job — 对应原 bulk 引擎的按日/按股/按季/快照四类表。

    与 DailySyncJob 的差别只在取数粒度: DailySyncJob 逐日调 fetch(date)
    再逐日写分区; BatchSyncJob 一次拉多天/多只全量数据, 统一 merge 进分区。
    两者共用同一套 run 骨架 (游标/重试/进度/通知/日期区间校验), 因此命令行
    行为一致: 都支持 --start-date/--end-date, 都记 MetaStore 游标。
    """

    def __init__(
        self,
        table_name: str,
        spec: DailyTableSpec,
        pro,
        store: DailyPartitionStore,
        *,
        kind: str = "date",
        api_name: str | None = None,
        date_col: str = "trade_date",
        codes: list[str] | None = None,
        paginate=None,
        supports_date_range: bool = True,
        sleep: float = 0.15,
        extra_params: dict | None = None,
        trading_days_from=None,
    ):
        self.table_name = table_name
        self.spec = spec
        self._pro = pro
        self.store = store
        self.kind = kind
        self.api_name = api_name or table_name
        self.date_col = date_col
        self.codes = codes or []
        self.paginate = paginate
        self.supports_date_range = supports_date_range
        self.sleep = sleep
        self.extra_params = extra_params or {}
        # trading_days_from(exchange, start, end) -> list[str]; 默认用日历近似
        self._trading_days_from = trading_days_from

    # ---- 取数 ------------------------------------------------------------
    def _fetch_dates(self, dates: list[str], rt: "SyncRuntime | None" = None):
        """按日期批量拉取。

        流式分批落盘: 每 BATCH_FLUSH_DAYS 天 (或累计 BATCH_FLUSH_ROWS 行)
        就 merge 一次, 中断最多丢当前批次而非全部进度 —— 长历史回溯
        (数千天) 时这是必需的, 否则一次 kill 就前功尽弃。
        返回 (df | None, err_summary); df 非 None 时表示最后一批待写。
        """
        all_dfs, errs, empty = [], {}, 0
        pending: list = []          # 未落盘的 DataFrame
        pending_days: list[str] = []
        written_days, written_rows = 0, 0
        started = time.monotonic()
        last_beat = started
        for i, d in enumerate(dates):
            df, err = _fetch_paged(
                self._pro, self.api_name, {self.date_col: d, **self.extra_params},
                paginate=self.paginate, sleep=self.sleep,
            )
            if err is not None:
                errs[_err_key(err)] = errs.get(_err_key(err), 0) + 1
            elif df is not None and len(df) > 0:
                pending.append(df)
                pending_days.append(d)
            else:
                empty += 1
                pending_days.append(d)
            time.sleep(self.sleep)
            _flush_ok = (
                len(pending_days) >= BATCH_FLUSH_DAYS
                or sum(len(x) for x in pending) >= BATCH_FLUSH_ROWS
            )
            if _flush_ok and pending and rt is not None and self.store is not None:
                written_days += self._flush(pending, pending_days, rt)
                written_rows += sum(len(x) for x in pending)
                all_dfs.extend(pending)
                pending, pending_days = [], []
            if (i + 1) % PROGRESS_INTERVAL == 0:
                total = written_rows + sum(len(x) for x in pending)
                logger.info(
                    f"{self.table_name}: ...{d} ({total:,} rows, "
                    f"落盘 {written_days} 天, {i + 1}/{len(dates)}, "
                    f"{time.monotonic() - started:.0f}s)"
                )
                last_beat = time.monotonic()
            elif time.monotonic() - last_beat >= HEARTBEAT_SECONDS:
                # 长历史回溯时 50 天可能要几分钟, 加时间心跳避免看起来卡死
                logger.info(
                    f"{self.table_name}: ...{d} (进行中, {i + 1}/{len(dates)} 天, "
                    f"落盘 {written_days} 天, {time.monotonic() - started:.0f}s)"
                )
                last_beat = time.monotonic()
        summary = ""
        if errs:
            detail = ", ".join(f"{k} x{v}" for k, v in sorted(errs.items(), key=lambda kv: -kv[1]))
            summary = f"{sum(errs.values())}/{len(dates)} 天接口报错: {detail}"
        if not all_dfs and not pending:
            return None, summary or (
                f"接口全部返回空 ({empty}/{len(dates)} 天, 数据可能尚未发布)" if empty else "无日期需要下载"
            )
        if summary:
            summary += f"; 其余 {written_days + len(pending)} 天成功"
        logger.info(
            f"{self.table_name}: 取数完成 落盘 {written_days} 天 + 待写 {len(pending)} 天, "
            f"累计 {written_rows + sum(len(x) for x in pending):,} 行, "
            f"耗时 {time.monotonic() - started:.0f}s"
        )
        return pending, summary

    def _flush(self, dfs: list, days: list[str], rt: "SyncRuntime") -> int:
        """把一批 DataFrame 合并写入分区, 返回实际落盘天数。"""
        import pandas as pd
        merged = clean_df(pd.concat(dfs, ignore_index=True))
        wrote = self.store.merge(merged, date_col=self.date_col, name=self.spec.name)
        if rt.meta is not None and days:
            rt.meta.update_last_date(self.spec.name, max(days))
        logger.info(f"{self.table_name}: 落盘 {wrote} 个分区 ({len(days)} 天请求)")
        return len(days)

    def _fetch_codes(self, codes: list[str], start: str, end: str):
        """按股票批量拉取 (财报类表: 每只一次请求, 带 start/end 窗口)。"""
        all_dfs, errs, empty = [], {}, 0
        started = time.monotonic()
        for i, code in enumerate(codes):
            params = {"ts_code": code, "start_date": start, "end_date": end, **self.extra_params}
            df, err = _fetch_paged(self._pro, self.api_name, params,
                                   paginate=self.paginate, sleep=self.sleep)
            if err is not None:
                errs[_err_key(err)] = errs.get(_err_key(err), 0) + 1
            elif df is not None and len(df) > 0:
                all_dfs.append(df)
            else:
                empty += 1
            time.sleep(self.sleep)
            if (i + 1) % PROGRESS_INTERVAL == 0:
                total = sum(len(x) for x in all_dfs)
                logger.info(
                    f"{self.table_name}: {i + 1}/{len(codes)} 只 ({total:,} rows, "
                    f"elapsed={time.monotonic() - started:.0f}s)"
                )
        summary = ""
        if errs:
            detail = ", ".join(f"{k} x{v}" for k, v in sorted(errs.items(), key=lambda kv: -kv[1]))
            summary = f"{sum(errs.values())}/{len(codes)} 只接口报错: {detail}"
        if not all_dfs:
            return None, summary or (
                f"接口全部返回空 ({empty}/{len(codes)} 只)" if empty else "无股票需要下载"
            )
        if summary:
            summary += f"; 其余 {len(codes) - sum(errs.values())} 只成功"
        logger.info(
            f"{self.table_name}: 取数完成 {len(all_dfs)}/{len(codes)} 只 "
            f"{sum(len(x) for x in all_dfs):,} 行, 耗时 {time.monotonic() - started:.0f}s"
        )
        return pd.concat(all_dfs, ignore_index=True), summary

    def _fetch_quarters(self, quarters: list[str]):
        all_dfs, errs = [], {}
        for q in quarters:
            df, err = _fetch_paged(self._pro, self.api_name, {"period": q, **self.extra_params},
                                   paginate=self.paginate, sleep=self.sleep)
            if err is not None:
                errs[_err_key(err)] = errs.get(_err_key(err), 0) + 1
            elif df is not None and len(df) > 0:
                all_dfs.append(df)
            time.sleep(self.sleep)
        if not all_dfs:
            detail = ", ".join(f"{k} x{v}" for k, v in errs.items())
            return None, f"季度接口全部返回空: {detail}" if detail else "无季度需要下载"
        return pd.concat(all_dfs, ignore_index=True), ""

    # ---- window: 一次请求拿整段时间窗 (无交易日维度的公告类接口) -------
    def _run_window(self, rt: SyncRuntime, start_date: str | None,
                    end_date: str | None) -> None:
        started = time.monotonic()
        last = rt.meta.get_last_date(self.spec.name)
        start = start_date or (dateutil.add_days(last, 1) if last is not None else self.spec.first_date)
        end = end_date or rt.calendar.today()
        if start > end:
            logger.info(f"{self.table_name}: 已是最新 (last={last})")
            return
        params = {"start_date": start, "end_date": end, **self.extra_params}
        logger.info(f"{self.table_name}: 窗口取数 {start}~{end}")
        try:
            df = getattr(self._pro, self.api_name)(**params)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"{self.table_name}: 接口报错 {type(exc).__name__}: {exc}")
            return
        if df is None or len(df) == 0:
            logger.info(f"{self.table_name}: {start}~{end} 无数据")
            return
        self._flush([df], [], rt)
        logger.info(
            f"{self.table_name}: done {len(df):,} 行, {time.monotonic() - started:.0f}s"
        )

    # ---- run -------------------------------------------------------------
    def run(self, rt: SyncRuntime, start_date: str | None = None,
            end_date: str | None = None) -> None:
        started = time.monotonic()
        if self.kind == "snapshot":
            self._run_snapshot(rt)
            return
        if self.kind == "window":
            self._run_window(rt, start_date, end_date)
            return

        store = self.store

        if start_date is None:
            last = rt.meta.get_last_date(self.spec.name)
            start = dateutil.add_days(last, 1) if last is not None else self.spec.first_date
            end = rt.calendar.today()
            if start > end:
                logger.info(f"{self.spec.name}: 已是最新 (last={last})")
                return
        else:
            start, end = start_date, end_date or rt.calendar.today()
            if start > end:
                raise ValueError(f"start_date {start} is after end_date {end}")

        if self.kind == "date":
            days = self._trading_days(start, end, rt)
            if not days:
                logger.info(f"{self.spec.name}: {start}~{end} 无交易日")
                return
            # 只补缺口: 已存在分区跳过 (重跑幂等, 不重复请求)
            todo = [d for d in days if not store.exists(d)]
            if not todo:
                logger.info(f"{self.spec.name}: {start}~{end} 分区已齐全 ({len(days)} 天)")
                return
            logger.info(f"{self.spec.name}: {start}~{end} 需补 {len(todo)}/{len(days)} 天")
            df, err = self._fetch_dates(todo, rt=rt)
        elif self.kind == "stock":
            codes = self.codes
            if not codes:
                logger.info(f"{self.spec.name}: 无股票代码可拉取")
                return
            logger.info(f"{self.spec.name}: {start}~{end} 需拉 {len(codes)} 只")
            df, err = self._fetch_codes(codes, start, end)
        else:  # quarter
            qs = quarter_ends(start, end)
            if not qs:
                logger.info(f"{self.spec.name}: {start}~{end} 无季末")
                return
            logger.info(f"{self.spec.name}: {start}~{end} 需拉 {len(qs)} 个季度 {qs[0]}~{qs[-1]}")
            df, err = self._fetch_quarters(qs)

        if df is None or (hasattr(df, "__len__") and len(df) == 0):
            msg = f"{self.spec.name} 无新数据: {err}"
            logger.warning(msg)
            rt.notifier.send(msg)
            return

        if not isinstance(df, pd.DataFrame):
            # _fetch_dates 返回的是最后一批未落盘的 DataFrame 列表
            df = clean_df(pd.concat(df, ignore_index=True))
        else:
            df = clean_df(df)
        wrote = store.merge(df, date_col=self.date_col, name=self.spec.name)
        rt.meta.update_last_date(self.spec.name, end)
        elapsed = time.monotonic() - started
        msg = (f"{self.spec.name} 同步完成\n"
               f"日期：{start} ~ {end}\n"
               f"写入 {wrote} 个分区｜耗时 {elapsed:.0f}s")
        if err:
            msg += f"\n⚠️ {err}"
        logger.info(f"{self.spec.name}: done {wrote} 分区, {elapsed:.0f}s")
        rt.notifier.send(msg)

    def _trading_days(self, start: str, end: str, rt: SyncRuntime) -> list[str]:
        if self._trading_days_from is not None:
            return self._trading_days_from(start, end)
        cal = rt.calendar
        if hasattr(cal, "get_trading_days"):
            try:
                return cal.get_trading_days("SSE", start, end)
            except Exception:
                pass
        return _bday_range(start, end)

    def _run_snapshot(self, rt: SyncRuntime) -> None:
        """一次性接口 (无日期维度): 直接覆盖写单文件。"""
        store = SnapshotStore(self.store._dir / "data.parquet")
        df, err = _call_with_retry(lambda: getattr(self._pro, self.api_name)(**self.extra_params))
        if err is not None:
            msg = f"{self.spec.name} 同步失败: {err}"
            logger.error(msg)
            rt.notifier.send(msg)
            raise err
        if df is None or len(df) == 0:
            logger.info(f"{self.spec.name}: 接口返回空, 保留原文件")
            return
        store.write(clean_df(df))
        logger.info(f"{self.spec.name}: 快照写入 {len(df):,} 行")
        rt.notifier.send(f"{self.spec.name} 快照同步完成\n写入 {len(df):,} 条记录")


def _bday_range(start: str, end: str) -> list[str]:
    """交易日历不可用时的退路: pandas 工作日近似。"""
    return [d.strftime("%Y%m%d") for d in pd.bdate_range(start, end)]
