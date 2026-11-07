from __future__ import annotations

import json
import urllib.request
from typing import Protocol

import httpx
from loguru import logger

from zer0share.config import NotifierConfig


class NotifierProtocol(Protocol):
    def send(self, message: str) -> None: ...

    def send_success(self, message: str) -> None: ...

    def notify_start(self, stage: str, details: dict | None = None) -> None: ...

    def notify_progress(self, stage: str, done: int, total: int) -> None: ...

    def notify_stage_done(self, stage: str, summary: dict, elapsed: float) -> None: ...

    def notify_error(self, stage: str, error) -> None: ...


class NullNotifier:
    def send(self, message: str) -> None:
        pass

    def send_success(self, message: str) -> None:
        pass

    def notify_start(self, stage: str, details: dict | None = None) -> None:
        pass

    def notify_progress(self, stage: str, done: int, total: int) -> None:
        pass

    def notify_stage_done(self, stage: str, summary: dict, elapsed: float) -> None:
        pass

    def notify_error(self, stage: str, error) -> None:
        pass


class WeComNotifier:
    def __init__(self, webhook_url: str, enabled: bool = True):
        self._url = webhook_url
        self._enabled = enabled

    def _prefix(self, stage: str) -> str:
        return f"[zer0share · {stage}]"

    def _post_text(self, text: str) -> None:
        if not self._enabled or not self._url:
            return
        payload = {
            "msgtype": "text",
            "text": {"content": text},
        }
        try:
            resp = httpx.post(self._url, json=payload, timeout=10)
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"企业微信推送失败: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"企业微信返回错误: {e.response.status_code}")
        except Exception as e:
            logger.error(f"企业微信推送异常: {e}")

    def send(self, message: str) -> None:
        self._post_text(f"[zer0share] {message}")

    def send_success(self, message: str) -> None:
        self.send(message)

    def notify_start(self, stage: str, details: dict | None = None) -> None:
        lines = [f"{self._prefix(stage)} 开始"]
        for key, value in (details or {}).items():
            lines.append(f"{key}: {value}")
        self._post_text("\n".join(lines))

    def notify_progress(self, stage: str, done: int, total: int) -> None:
        if total == 0:
            return
        pct = round(done / total * 100)
        self._post_text(f"{self._prefix(stage)} 进度 {pct}% ({done}/{total})")

    def notify_stage_done(self, stage: str, summary: dict, elapsed: float) -> None:
        lines = [f"{self._prefix(stage)} 完成"]
        for key, value in summary.items():
            lines.append(f"{key}: {value}")
        lines.append(f"耗时: {elapsed:.1f}s")
        self._post_text("\n".join(lines))

    def notify_error(self, stage: str, error) -> None:
        if isinstance(error, BaseException):
            detail = f"{type(error).__name__}: {error}"
        else:
            detail = str(error)
        self._post_text(f"{self._prefix(stage)} 错误\n{detail}")


class FeishuNotifier:
    def __init__(self, webhook_url: str, *, app: str = "zer0share") -> None:
        self._url = webhook_url
        self._app = app

    def send(self, message: str) -> None:
        self._send(
            self._build_card(
                title=f"[{self._app}] 通知",
                color="blue",
                fields=[("内容", message)],
            )
        )

    def send_success(self, message: str) -> None:
        self.send(message)

    def notify_start(self, stage: str, details: dict | None = None) -> None:
        self._send(
            self._build_card(
                title=f"[{self._app}] `{stage}` 开始运行",
                color="orange",
                fields=[(str(key), str(value)) for key, value in (details or {}).items()],
            )
        )

    def notify_progress(self, stage: str, done: int, total: int) -> None:
        if total == 0:
            return
        pct = round(done / total * 100)
        self._send(
            self._build_card(
                title=f"[{self._app}] `{stage}` 进度 {pct}% ({done}/{total})",
                color="blue",
                fields=[],
            )
        )

    def notify_stage_done(self, stage: str, summary: dict, elapsed: float) -> None:
        fields = [(str(key), str(value)) for key, value in summary.items()]
        fields.append(("耗时", f"{elapsed:.1f}s"))
        self._send(
            self._build_card(
                title=f"[{self._app}] `{stage}` 完成",
                color="green",
                fields=fields,
            )
        )

    def notify_error(self, stage: str, error) -> None:
        if isinstance(error, BaseException):
            fields = [("异常类型", type(error).__name__), ("信息", str(error))]
        else:
            fields = [("信息", str(error))]
        self._send(
            self._build_card(
                title=f"[{self._app}] `{stage}` 出错",
                color="red",
                fields=fields,
            )
        )

    def _build_card(self, title: str, color: str, fields: list[tuple[str, str]]) -> dict:
        elements: list[dict] = []
        if fields:
            elements.append(
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**{key}**\n{value}",
                            },
                        }
                        for key, value in fields
                    ],
                }
            )
        return {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": color,
                },
                "elements": elements,
            },
        }

    def _send(self, card: dict) -> None:
        if not self._url:
            return
        payload = json.dumps(card).encode()
        req = urllib.request.Request(
            self._url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10):
                pass
        except Exception as e:
            logger.error(f"飞书推送异常: {e}")


class CompositeNotifier:
    def __init__(self, notifiers: list[NotifierProtocol]):
        self.notifiers = notifiers

    def send(self, message: str) -> None:
        for notifier in self.notifiers:
            notifier.send(message)

    def send_success(self, message: str) -> None:
        for notifier in self.notifiers:
            notifier.send_success(message)

    def notify_start(self, stage: str, details: dict | None = None) -> None:
        for notifier in self.notifiers:
            notifier.notify_start(stage, details)

    def notify_progress(self, stage: str, done: int, total: int) -> None:
        for notifier in self.notifiers:
            notifier.notify_progress(stage, done, total)

    def notify_stage_done(self, stage: str, summary: dict, elapsed: float) -> None:
        for notifier in self.notifiers:
            notifier.notify_stage_done(stage, summary, elapsed)

    def notify_error(self, stage: str, error) -> None:
        for notifier in self.notifiers:
            notifier.notify_error(stage, error)


def build_notifier(cfg: NotifierConfig) -> NotifierProtocol:
    if not cfg.enabled:
        return NullNotifier()
    notifiers: list[NotifierProtocol] = []
    if cfg.wecom.enabled and cfg.wecom.webhook_url:
        notifiers.append(WeComNotifier(cfg.wecom.webhook_url, enabled=True))
    if cfg.feishu.enabled and cfg.feishu.webhook_url:
        notifiers.append(FeishuNotifier(cfg.feishu.webhook_url, app="zer0share"))
    if not notifiers:
        return NullNotifier()
    if len(notifiers) == 1:
        return notifiers[0]
    return CompositeNotifier(notifiers)


Notifier = WeComNotifier
