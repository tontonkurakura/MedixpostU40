#!/usr/bin/env python3
import os, sys, json, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import yaml
from croniter import croniter
import urllib.request

CONFIG_PATH = os.getenv("CONFIG_PATH", "config.yml")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")

if not SLACK_TOKEN:
    print("ERROR: SLACK_BOT_TOKEN is not set.", file=sys.stderr)
    sys.exit(1)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

tzname = cfg.get("timezone") or "UTC"
tz = ZoneInfo(tzname)
window_minutes = int(cfg.get("window_minutes", 5))

now = datetime.now(tz)
window_start = now - timedelta(minutes=window_minutes)

jobs = cfg.get("jobs", [])

def due(cron_expr: str) -> bool:
    """
    「直近window_minutes分の間に発火すべき時刻があったら True」
    を返す判定。遅延や早期起動に耐性を持たせるための窓判定。
    """
    try:
        base = window_start  # 窓の開始時刻を基準に次の発火を計算
        it = croniter(cron_expr, base)
        nxt = it.get_next(datetime)  # 次の理論発火時刻（tz付き）
        # 次回発火が「窓内（window_start < t <= now）」に入っていれば送る
        return window_start < nxt <= now
    except Exception as e:
        print(f"[WARN] invalid cron '{cron_expr}': {e}", file=sys.stderr)
        return False

def post_message(channel: str, text: str):
    url = "https://slack.com/api/chat.postMessage"
    payload = {
        "channel": channel,
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {SLACK_TOKEN}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        body = resp.read()
    res = json.loads(body.decode("utf-8"))
    if not res.get("ok"):
        raise RuntimeError(f"Slack API error: {res}")

sent = 0
for j in jobs:
    name = j.get("name", "")
    channel = j.get("channel")
    text = j.get("text")
    cron_expr = j.get("cron")
    if not (channel and text and cron_expr):
        print(f"[WARN] skip job '{name}' due to missing fields.", file=sys.stderr)
        continue
    if due(cron_expr):
        try:
            post_message(channel, text)
            sent += 1
            print(f"[INFO] posted: {name} -> {channel}")
        except Exception as e:
            print(f"[ERROR] failed to post '{name}': {e}", file=sys.stderr)

print(f"[INFO] done. sent={sent}, now={now.isoformat()} window_start={window_start.isoformat()}")
