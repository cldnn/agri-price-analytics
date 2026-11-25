"""Collect the Notion 90.02 initial window. Run 'python collect_raw.py --help'."""
import argparse
import gzip
import hashlib
import http.client
import json
import os
import re
import shutil
import sqlite3
import sys
import threading
import time
import zlib
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing, contextmanager
from datetime import date, datetime, time as clock_time, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import build_opener, HTTPRedirectHandler

from ingestion.sources import CollectionError, MARKET_SOURCES, PAGE_SIZE, SETTLEMENT_OP, jobs, parse, request, require

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "data" / "raw"
META = RAW / "_meta"
KST = timezone(timedelta(hours=9))
SOURCES = ("DS-001", "DS-002", "DS-003", "DS-004", "DS-005", "DS-006")
SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (run TEXT PRIMARY KEY, snapshot TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS units (
 id INTEGER PRIMARY KEY, run TEXT, source TEXT, op TEXT, period TEXT, entity TEXT,
 status TEXT DEFAULT 'planned', revision INTEGER DEFAULT 1, error TEXT,
 UNIQUE(run, source, op, period, entity));
CREATE INDEX IF NOT EXISTS queue ON units(run,source,status,id);
CREATE TABLE IF NOT EXISTS pages (
 unit INTEGER, revision INTEGER, page INTEGER, path TEXT UNIQUE, params TEXT,
 fetched TEXT, rows INTEGER, total INTEGER, sha256 TEXT, compressed_bytes INTEGER,
 signature TEXT, PRIMARY KEY(unit,revision,page));
CREATE TABLE IF NOT EXISTS opinet_calls (kst_date TEXT PRIMARY KEY, attempts INTEGER NOT NULL);
"""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


HTTP = build_opener(NoRedirect)


class RateLimited(CollectionError):
    def __init__(self, retry_after):
        super().__init__("HTTP 429")
        self.delay = 0
        if retry_after and retry_after.isdigit():
            self.delay = int(retry_after)
        elif retry_after:
            try:
                self.delay = max(0, (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                pass


def fetch(url, *, before_request=None):
    for attempt in range(4):
        if before_request is not None:
            before_request()
        try:
            with HTTP.open(url, timeout=30) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code == 429:
                raise RateLimited(exc.headers.get("Retry-After")) from None
            if exc.code not in (500, 502, 503, 504) or attempt == 3:
                raise CollectionError(f"HTTP {exc.code}") from None
            delay = exc.headers.get("Retry-After", "")
            if delay.isdigit() and int(delay) > 120:
                raise CollectionError(f"HTTP {exc.code}, retry after {delay}s") from None
            time.sleep(max(2 ** attempt, int(delay) if delay.isdigit() else 0))
        except (URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
            if attempt == 3:
                raise CollectionError(f"network {type(exc).__name__}") from None
            time.sleep(2 ** attempt)


def credentials():
    values = {}
    if (ROOT / ".env").is_file():
        for line in (ROOT / ".env").read_text(encoding="utf-8-sig").splitlines():
            key, sep, value = line.partition("=")
            if sep and not key.lstrip().startswith("#"):
                values[key.strip()] = value.partition(" #")[0].strip().strip("'").strip('"')
    return values | os.environ


@contextmanager
def writer_lock(run, source):
    path = META / f"{run}-{source}.lock"
    with path.open("a+b") as stream:
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise CollectionError(f"{source} already running") from None
        yield


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_saved(row):
    raw = gzip.decompress((RAW / row["path"]).read_bytes())
    require(digest(raw) == row["sha256"], "checksum mismatch")
    return raw


def markets(db, run):
    rows = db.execute("""SELECT p.* FROM pages p JOIN units u ON u.id=p.unit AND u.revision=p.revision
        WHERE u.run=? AND u.op='wholesaleMarkets' AND u.status='complete' ORDER BY p.page""", (run,))
    codes = [r["whsl_mrkt_cd"] for page in rows
             for r in json.loads(read_saved(page))["response"]["body"]["items"]["item"]]
    require(codes and len(set(codes)) == len(codes), "collect DS-006 market catalog first")
    return codes


def save_page(db, unit, revision, page, raw, params, count, total, signature):
    source, op, period, entity = (unit[key] for key in ("source", "op", "period", "entity"))
    target = RAW / f"source={source}" / f"operation={op}"
    if source == "DS-006":
        target /= f"snapshot_date={period}"
    else:
        dimension = "market" if source in MARKET_SOURCES else {"DS-003": "station", "DS-004": "hs", "DS-005": "product"}[source]
        target = target / f"month={period[:7]}" / f"{dimension}={entity}"
        if len(period) == 10:
            target /= f"date={period}"
    target = target / f"run_id={unit['run']}" / f"rev={revision}"
    target.mkdir(parents=True, exist_ok=True)
    ext = "xml" if source == "DS-004" else "json"
    path = target / f"page-{page:06d}.{ext}.gz"
    tmp = path.with_suffix(".part")
    tmp.write_bytes(gzip.compress(raw, compresslevel=3, mtime=0))
    tmp.replace(path)
    with db:
        db.execute("INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   (unit["id"], revision, page, path.relative_to(RAW).as_posix(), json.dumps(params),
                    datetime.now(timezone.utc).isoformat(), count, total, digest(raw), path.stat().st_size, signature))


def page_signature(raw, source):
    body = json.loads(raw)
    if source == "DS-001":
        rows = [{k: v for k, v in row.items() if k != "ROW_NUM"} for row in body[SETTLEMENT_OP]["row"]]
    else:
        rows = body["response"]["body"]["items"]["item"]
    return digest(json.dumps(rows, sort_keys=True, ensure_ascii=False).encode())


def reserve_opinet_call(db):
    # The general API allows 300 calls per day. Count retries as calls, including after a restart.
    day = datetime.now(KST).date().isoformat()
    with db:
        result = db.execute("""INSERT INTO opinet_calls(kst_date,attempts) VALUES (?,1)
            ON CONFLICT(kst_date) DO UPDATE SET attempts=attempts+1 WHERE attempts<300""", (day,))
    if result.rowcount == 0:
        raise DailyQuota("Opinet daily call budget used")


def collect_unit(db, u, env, stop=None):
    source, period, entity = u["source"], u["period"], u["entity"]
    revision = u["revision"]
    old = db.execute("SELECT * FROM pages WHERE unit=? AND revision=? ORDER BY page",
                     (u["id"], revision)).fetchall()
    saved = {p["page"]: p for p in old}
    page_size = PAGE_SIZE.get(source)
    if old and source == "DS-002":
        page_size = int(json.loads(old[0]["params"])["numOfRows"])
    with db:
        db.execute("UPDATE units SET status='partial', error=NULL WHERE id=?", (u["id"],))
    page, received, expected = 1, 0, None
    seen = set()
    while True:
        if stop is not None and stop.is_set():
            return
        url, params = request(source, u["op"], period, entity, page, env, page_size=page_size)
        # Recheck the two boundaries of an interrupted sequence before appending.
        cached = saved.get(page)
        local = None
        if cached:
            try:
                local = read_saved(cached)
            except (OSError, EOFError, zlib.error, CollectionError):
                pass
        if local is not None and page not in (1, len(saved)):
            raw = local
        elif source == "DS-005":
            raw = fetch(url, before_request=lambda: reserve_opinet_call(db))
        else:
            raw = fetch(url)
        count, total = parse(source, period, entity, page, raw, page_size=page_size)
        changed = cached and (local is None or digest(raw) != cached["sha256"])
        if changed or (expected is not None and total != expected):
            with db:
                db.execute("UPDATE units SET revision=revision+1 WHERE id=?", (u["id"],))
            raise CollectionError("source revision changed; retry entire unit")
        signature = page_signature(raw, source) if page_size and total > page_size else None
        require(signature is None or signature not in seen, "repeated page content")
        seen.add(signature)
        if not cached:
            save_page(db, u, revision, page, raw, params, count, total, signature)
        expected = total
        received += count
        if received == expected:
            with db:
                db.execute("UPDATE units SET status=?,error=NULL WHERE id=?",
                           ("complete" if received else "empty", u["id"]))
                if source == "DS-005" and entity == "ALL":
                    db.execute("""UPDATE units SET status='superseded' WHERE run=? AND source='DS-005'
                        AND period=? AND entity='D047' AND status='complete'""", (u["run"], period))
            return
        require(count > 0 and received < expected, "incomplete page sequence")
        page += 1


def collect_one(unit_id, env, args, stop):
    require(shutil.disk_usage(RAW).free > args.reserve_gib * 1024**3, "disk reserve reached; collection paused")
    with closing(sqlite3.connect(META / "state.sqlite3", timeout=60)) as db:
        db.row_factory = sqlite3.Row
        u = db.execute("SELECT * FROM units WHERE id=?", (unit_id,)).fetchone()
        try:
            try:
                collect_unit(db, u, env, stop)
            except CollectionError:
                updated = db.execute("SELECT * FROM units WHERE id=?", (unit_id,)).fetchone()
                if updated["revision"] == u["revision"]:
                    raise
                collect_unit(db, updated, env, stop)
        except DailyQuota:
            with db:
                db.execute("UPDATE units SET status='planned' WHERE id=?", (unit_id,))
            raise
        except (ValueError, OSError, KeyError, TypeError, SyntaxError, CollectionError) as exc:
            error = str(exc) if isinstance(exc, CollectionError) else type(exc).__name__
            with db:
                db.execute("UPDATE units SET status='failed',error=? WHERE id=?", (error, unit_id))
            print(u["source"], u["op"], u["period"], u["entity"], error, flush=True)
            if isinstance(exc, RateLimited):
                raise
            return error
        time.sleep(0.15)
    return None


class DailyQuota(CollectionError):
    pass


def drain_queue(ids, workers, env, args):
    # Only the coordinator assigns ids. Workers use separate SQLite connections.
    pending_ids = iter(ids)
    stop = threading.Event()
    failures = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        active = set()
        try:
            while True:
                while len(active) < workers:
                    unit = next(pending_ids, None)
                    if unit is None:
                        break
                    active.add(pool.submit(collect_one, unit[0], env, args, stop))
                if not active:
                    return
                finished, active = wait(active, return_when=FIRST_COMPLETED)
                for future in finished:
                    error = future.result()
                    failures = failures + 1 if error else 0
                    if error in ("API ERROR-350", "API 22"):
                        raise DailyQuota(error)
                    if args.source == "DS-005" and failures >= 3 and error == "empty Opinet response":
                        raise DailyQuota("repeated empty Opinet responses")
                    if error and (failures >= 3 or error.startswith(("HTTP 401", "HTTP 403", "HTTP 429", "API INFO-100"))):
                        raise CollectionError(f"{args.source} paused: {error}")
        finally:
            stop.set()


def wait_until(start):
    while (delay := (start - datetime.now(KST)).total_seconds()) > 0:
        time.sleep(min(delay, 60))


def collect(db, args):
    source, run = args.source, args.run
    today = datetime.now(KST).date()
    snapshot = max(today, args.start_on or today).isoformat()
    with db:
        db.execute("INSERT OR IGNORE INTO runs VALUES (?,?)", (run, snapshot))
    snapshot = db.execute("SELECT snapshot FROM runs WHERE run=?", (run,)).fetchone()[0]
    targets = jobs(source, snapshot, markets(db, run) if source in MARKET_SOURCES else (),
                   args.station or ["108"], args.hs or ["0703101090"])
    with db:
        db.executemany("INSERT OR IGNORE INTO units(run,source,op,period,entity) VALUES (?,?,?,?,?)",
                       ((run, source, *target) for target in targets))
        if source == "DS-005":
            db.execute("""UPDATE units SET status='superseded' WHERE run=? AND source='DS-005'
                AND entity='D047' AND status IN ('planned','partial','failed')""", (run,))
    if args.start_on:
        start = datetime.combine(args.start_on, clock_time(0, 5), tzinfo=KST)
        print(f"{source} resumes at {start.isoformat()}", flush=True)
        wait_until(start)
    env = credentials()
    workers = args.workers or {"DS-001": 4, "DS-002": 3}.get(source, 1)
    backoff = 60
    while True:
        ids = db.execute("""SELECT id FROM units WHERE run=? AND source=? AND status IN ('planned','partial','failed')
            ORDER BY id LIMIT ?""", (run, source, args.limit or -1)).fetchall()
        if source == "DS-006" and ids:
            require(snapshot == datetime.now(KST).date().isoformat(), "new snapshot date requires a new run id")
        try:
            drain_queue(ids, workers, env, args)
            return
        except (DailyQuota, RateLimited) as exc:
            if (source not in MARKET_SOURCES and source != "DS-005") or args.limit:
                raise
            now = datetime.now(KST)
            if isinstance(exc, DailyQuota):
                start = datetime.combine(now.date() + timedelta(days=1), clock_time(0, 5), tzinfo=KST)
            else:
                start = now + timedelta(seconds=max(backoff, exc.delay))
                backoff = min(3600, 2 * backoff)
                workers = max(1, workers // 2)
            print(f"{source} {exc}; resumes at {start.isoformat()}", flush=True)
            wait_until(start)


def verify(db, run):
    db.execute("BEGIN")
    files = 0
    current = db.execute("""SELECT u.*, p.* FROM units u JOIN pages p ON p.unit=u.id AND p.revision=u.revision
        WHERE u.run=? ORDER BY u.id, p.page""", (run,))
    for r in current:
        raw = read_saved(r)
        size = json.loads(r["params"]).get("numOfRows")
        count, total = parse(r["source"], r["period"], r["entity"], r["page"], raw, page_size=size)
        require((count, total) == (r["rows"], r["total"]), f"metadata mismatch: {r['path']}")
        files += 1
    invalid = db.execute("""SELECT u.id FROM units u LEFT JOIN pages p ON p.unit=u.id AND p.revision=u.revision
        WHERE u.run=? AND u.status IN ('complete','empty') GROUP BY u.id
        HAVING count(p.page)=0 OR min(p.page)!=1 OR max(p.page)!=count(p.page)
        OR sum(p.rows)!=max(p.total) OR min(p.total)!=max(p.total)
        OR count(p.signature)!=count(DISTINCT p.signature)""", (run,)).fetchone()
    require(not invalid, "incomplete unit marked finished")
    print(f"verified {files} files")
    db.commit()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["collect", "status", "verify", "manifest"])
    parser.add_argument("--run", default="initial-20261124")
    parser.add_argument("--source", choices=SOURCES)
    parser.add_argument("--limit", type=int, help="maximum unfinished units to process in this invocation")
    parser.add_argument("--workers", type=int, help="concurrent market/date units (defaults: settlement 4, auction 3, other 1)")
    parser.add_argument("--start-on", type=date.fromisoformat, help="wait until this date in Korea before collecting")
    parser.add_argument("--reserve-gib", type=int, default=8, help="free disk space to retain (default: 8 GiB)")
    parser.add_argument("--station", action="append", help="ASOS station id (default: Seoul 108)")
    parser.add_argument("--hs", action="append", help="HS code (default: onion 0703101090)")
    args = parser.parse_args()
    if (not re.fullmatch(r"[a-zA-Z0-9_-]{1,48}", args.run) or args.reserve_gib < 0
        or (args.limit is not None and args.limit < 1) or (args.workers is not None and args.workers < 1)
        or any(not re.fullmatch(r"[0-9]{3}", s) for s in args.station or [])
        or any(not re.fullmatch(r"(?:[0-9]{2}|[0-9]{4}|[0-9]{6}|[0-9]{10})", s) for s in args.hs or [])):
        parser.error("invalid run id, limit or target code")
    if args.action == "collect" and not args.source:
        parser.error("collect requires --source")
    META.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(META / "state.sqlite3", timeout=60)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(SCHEMA)
    if args.action == "collect":
        with writer_lock(args.run, args.source):
            collect(db, args)
    elif args.action == "verify":
        verify(db, args.run)
    elif args.action == "status":
        for r in db.execute("""SELECT CASE WHEN source='DS-005' THEN source||' product='||entity ELSE source END scope,
            status,count(*) AS units FROM units WHERE run=? GROUP BY scope,status ORDER BY scope,status""", (args.run,)):
            print(*r)
        for r in db.execute("""SELECT source,count(*) AS files,sum(rows),sum(compressed_bytes)
            FROM units u JOIN pages p ON p.unit=u.id AND p.revision=u.revision
            WHERE run=? GROUP BY source ORDER BY source""", (args.run,)):
            print(*r, "(files, rows, compressed bytes)")
    else:
        # One record per page, or one per outstanding unit without a saved page.
        for row in db.execute("""SELECT u.source,u.op,u.period,u.entity,u.status,u.error,u.revision,p.*
            FROM units u LEFT JOIN pages p ON p.unit=u.id AND u.revision=p.revision
            WHERE u.run=? ORDER BY u.id,p.page""", (args.run,)):
            print(json.dumps(dict(row), ensure_ascii=True))
    db.close()


if __name__ == "__main__":
    try:
        main()
    except CollectionError as exc:
        sys.exit(str(exc))
