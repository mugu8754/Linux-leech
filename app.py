import asyncio
import hashlib
import json
import logging
import logging.handlers
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Set

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError


DEFAULT_POLL_SECONDS = 3
DEFAULT_RETRY_LIMIT = 5
DEFAULT_RETRY_DELAY = 10
DEFAULT_STABLE_SECONDS = 8
DEFAULT_KEEP_FREE_GB = 15
DEFAULT_MAX_FILE_GB = 4
DEFAULT_SCAN_DEPTH = 8
STATE_FLUSH_SECONDS = 5


@dataclass
class Config:
    api_id: int
    api_hash: str
    bot_token: str
    chat_id: str
    watch_dir: Path
    state_file: Path
    log_file: Path
    poll_seconds: int
    retry_limit: int
    retry_delay: int
    stable_seconds: int
    keep_free_gb: int
    max_file_gb: int
    scan_depth: int
    upload_workers: int
    session_name: str


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def load_config() -> Config:
    load_dotenv()
    missing = [
        name
        for name in ["API_ID", "API_HASH", "BOT_TOKEN", "CHAT_ID", "WATCH_DIR"]
        if not os.getenv(name)
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    watch_dir = Path(os.environ["WATCH_DIR"]).expanduser().resolve()
    state_file = Path(os.getenv("STATE_FILE", str(watch_dir / ".uploader_state.json"))).expanduser().resolve()
    log_file = Path(os.getenv("LOG_FILE", str(watch_dir / "uploader.log"))).expanduser().resolve()

    return Config(
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
        bot_token=os.environ["BOT_TOKEN"],
        chat_id=os.environ["CHAT_ID"],
        watch_dir=watch_dir,
        state_file=state_file,
        log_file=log_file,
        poll_seconds=env_int("POLL_SECONDS", DEFAULT_POLL_SECONDS),
        retry_limit=env_int("RETRY_LIMIT", DEFAULT_RETRY_LIMIT),
        retry_delay=env_int("RETRY_DELAY", DEFAULT_RETRY_DELAY),
        stable_seconds=env_int("STABLE_SECONDS", DEFAULT_STABLE_SECONDS),
        keep_free_gb=env_int("KEEP_FREE_GB", DEFAULT_KEEP_FREE_GB),
        max_file_gb=env_int("MAX_FILE_GB", DEFAULT_MAX_FILE_GB),
        scan_depth=env_int("SCAN_DEPTH", DEFAULT_SCAN_DEPTH),
        upload_workers=env_int("UPLOAD_WORKERS", 1),
        session_name=os.getenv("SESSION_NAME", "folder_uploader_session"),
    )


def setup_logging(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("folder_telegram_uploader")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.handlers.RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def human_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{value} B"


class StateStore:
    def __init__(self, path: Path, logger: logging.Logger):
        self.path = path
        self.logger = logger
        self.uploaded_hashes: Set[str] = set()
        self.uploaded_paths: Set[str] = set()
        self.last_save = 0.0
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.uploaded_hashes = set(data.get("uploaded_hashes", []))
            self.uploaded_paths = set(data.get("uploaded_paths", []))
            self.logger.info(
                "Loaded state: %s hashes, %s paths", len(self.uploaded_hashes), len(self.uploaded_paths)
            )
        except Exception as exc:
            self.logger.warning("Could not load state file %s: %s", self.path, exc)

    def save(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_save < STATE_FLUSH_SECONDS:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        payload = {
            "uploaded_hashes": sorted(self.uploaded_hashes),
            "uploaded_paths": sorted(self.uploaded_paths),
        }
        temp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp.replace(self.path)
        self.last_save = now

    def seen(self, file_hash: str, path: Path) -> bool:
        return file_hash in self.uploaded_hashes or str(path) in self.uploaded_paths

    def mark_uploaded(self, file_hash: str, path: Path) -> None:
        self.uploaded_hashes.add(file_hash)
        self.uploaded_paths.add(str(path))
        self.save()


class FolderScanner:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.candidate_times: Dict[str, float] = {}

    def _within_depth(self, path: Path) -> bool:
        try:
            rel = path.relative_to(self.config.watch_dir)
        except ValueError:
            return False
        return len(rel.parts) <= self.config.scan_depth

    def _is_partial_name(self, path: Path) -> bool:
        lower = path.name.lower()
        partial_suffixes = (".part", ".aria2", ".tmp", ".crdownload")
        return lower.endswith(partial_suffixes)

    def iter_files(self):
        for root, _, files in os.walk(self.config.watch_dir):
            for name in files:
                path = Path(root) / name
                if not self._within_depth(path):
                    continue
                if path == self.config.state_file or path == self.config.log_file:
                    continue
                yield path

    def stable_ready_files(self):
        ready = []
        now = time.time()
        for path in self.iter_files():
            if self._is_partial_name(path):
                continue
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            if stat.st_size <= 0:
                continue
            key = str(path)
            sig = f"{stat.st_size}:{int(stat.st_mtime)}"
            old = self.candidate_times.get(key)
            memo_key = f"{key}|{sig}"
            if old is None or memo_key not in self.candidate_times:
                self.candidate_times = {k: v for k, v in self.candidate_times.items() if not k.startswith(f"{key}|")}
                self.candidate_times[memo_key] = now
                continue
            first_seen = self.candidate_times[memo_key]
            if now - first_seen >= self.config.stable_seconds:
                ready.append(path)
        return ready


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def free_bytes(path: Path) -> int:
    usage = shutil.disk_usage(path)
    return usage.free


class TelegramUploader:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client = Client(
            config.session_name,
            api_id=config.api_id,
            api_hash=config.api_hash,
            bot_token=config.bot_token,
        )

    async def start(self):
        await self.client.start()
        self.logger.info("Telegram client started")

    async def stop(self):
        await self.client.stop()
        self.logger.info("Telegram client stopped")

    async def upload_with_retries(self, path: Path) -> bool:
        for attempt in range(1, self.config.retry_limit + 1):
            try:
                ok = await self._upload_once(path)
                if ok:
                    return True
            except FloodWait as exc:
                self.logger.warning("FloodWait for %s seconds while uploading %s", exc.value, path.name)
                await asyncio.sleep(exc.value)
            except RPCError as exc:
                self.logger.warning("Telegram RPC error on attempt %s for %s: %s", attempt, path.name, exc)
            except Exception as exc:
                self.logger.warning("Upload error on attempt %s for %s: %s", attempt, path.name, exc)
            await asyncio.sleep(self.config.retry_delay)
        return False

    async def _upload_once(self, path: Path) -> bool:
        file_size = path.stat().st_size
        last_pct = -10
        started = time.time()

        def progress(current: int, total: int):
            nonlocal last_pct
            if total <= 0:
                return
            pct = int((current / total) * 100)
            if pct >= last_pct + 10 or pct == 100:
                elapsed = max(time.time() - started, 1)
                speed = current / elapsed
                self.logger.info(
                    "UPLOAD | %s | %s%% | %s / %s | %s/s",
                    path.name,
                    pct,
                    human_bytes(current),
                    human_bytes(total),
                    human_bytes(int(speed)),
                )
                last_pct = pct

        caption = f"{path.name}\nSize: {human_bytes(file_size)}"
        msg = await self.client.send_document(
            chat_id=self.config.chat_id,
            document=str(path),
            caption=caption,
            progress=progress,
        )
        return bool(getattr(msg, "id", None))


class App:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.state = StateStore(config.state_file, logger)
        self.scanner = FolderScanner(config, logger)
        self.uploader = TelegramUploader(config, logger)
        self.queue: asyncio.Queue[Path] = asyncio.Queue()
        self.enqueued: Set[str] = set()
        self.running = True

    async def log_system_status(self):
        while self.running:
            free = free_bytes(self.config.watch_dir)
            queued = self.queue.qsize()
            self.logger.info(
                "SYSTEM | free=%s | keep_free=%s | queued=%s | watch_dir=%s",
                human_bytes(free),
                human_bytes(self.config.keep_free_gb * 1024**3),
                queued,
                self.config.watch_dir,
            )
            await asyncio.sleep(15)

    async def scanner_loop(self):
        while self.running:
            try:
                for path in self.scanner.stable_ready_files():
                    if str(path) in self.enqueued:
                        continue
                    try:
                        size = path.stat().st_size
                    except FileNotFoundError:
                        continue
                    limit = self.config.max_file_gb * 1024**3
                    if size > limit:
                        self.logger.warning(
                            "SKIP | %s | size=%s exceeds max_file_gb=%s GB",
                            path.name,
                            human_bytes(size),
                            self.config.max_file_gb,
                        )
                        continue
                    self.enqueued.add(str(path))
                    await self.queue.put(path)
                    self.logger.info("QUEUE | added=%s | size=%s | queued=%s", path.name, human_bytes(size), self.queue.qsize())
            except Exception as exc:
                self.logger.warning("Scanner loop error: %s", exc)
            await asyncio.sleep(self.config.poll_seconds)

    async def worker(self, worker_id: int):
        while self.running:
            path = await self.queue.get()
            try:
                await self.process_file(path, worker_id)
            finally:
                self.queue.task_done()
                self.enqueued.discard(str(path))

    async def process_file(self, path: Path, worker_id: int):
        if not path.exists():
            self.logger.warning("WORKER %s | missing file: %s", worker_id, path)
            return
        size = path.stat().st_size
        free_before = free_bytes(self.config.watch_dir)
        self.logger.info(
            "WORKER %s | START | %s | size=%s | free_before=%s",
            worker_id,
            path.name,
            human_bytes(size),
            human_bytes(free_before),
        )
        if free_before < self.config.keep_free_gb * 1024**3:
            self.logger.warning(
                "WORKER %s | Low disk space before upload: %s free",
                worker_id,
                human_bytes(free_before),
            )
        file_hash = sha256_file(path)
        self.logger.info("WORKER %s | HASH | %s | sha256=%s", worker_id, path.name, file_hash)
        if self.state.seen(file_hash, path):
            self.logger.info("WORKER %s | SKIP duplicate | %s", worker_id, path.name)
            return
        success = await self.uploader.upload_with_retries(path)
        if not success:
            self.logger.error("WORKER %s | FAILED | %s", worker_id, path.name)
            return
        self.state.mark_uploaded(file_hash, path)
        self.logger.info("WORKER %s | SUCCESS | %s", worker_id, path.name)
        try:
            path.unlink()
            self.logger.info(
                "WORKER %s | DELETE | %s | free_after=%s",
                worker_id,
                path.name,
                human_bytes(free_bytes(self.config.watch_dir)),
            )
        except Exception as exc:
            self.logger.warning("WORKER %s | Could not delete %s: %s", worker_id, path.name, exc)

    async def run(self):
        self.config.watch_dir.mkdir(parents=True, exist_ok=True)
        await self.uploader.start()
        tasks = [asyncio.create_task(self.scanner_loop()), asyncio.create_task(self.log_system_status())]
        for idx in range(self.config.upload_workers):
            tasks.append(asyncio.create_task(self.worker(idx + 1)))

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except NotImplementedError:
                pass

        try:
            await asyncio.gather(*tasks)
        finally:
            await self.uploader.stop()
            self.state.save(force=True)

    def request_stop(self):
        self.logger.info("Shutdown requested")
        self.running = False


def main():
    config = load_config()
    logger = setup_logging(config.log_file)
    logger.info("Starting folder Telegram uploader")
    logger.info("WATCH_DIR=%s | LOG_FILE=%s | STATE_FILE=%s", config.watch_dir, config.log_file, config.state_file)
    app = App(config, logger)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
