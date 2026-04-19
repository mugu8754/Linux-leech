# Folder Telegram Uploader

Ubuntu-ready legal file uploader that watches a folder, uploads completed files to Telegram using Pyrogram, then deletes local files after confirmed upload.

## Features

- watches a local folder continuously
- skips partial files like `.aria2`, `.part`, `.tmp`
- detailed terminal and log output
- logs file size, queue status, free storage, upload percent, upload speed, success, failure, delete status
- duplicate protection using SHA-256 and saved state
- retries failed uploads
- systemd service included

## Example log output

```text
SYSTEM | free=182.10 GB | keep_free=15.00 GB | queued=2 | watch_dir=/home/ubuntu/watch
QUEUE | added=video01.mp4 | size=1.82 GB | queued=1
WORKER 1 | START | video01.mp4 | size=1.82 GB | free_before=181.40 GB
WORKER 1 | HASH | video01.mp4 | sha256=...
UPLOAD | video01.mp4 | 10% | 186.00 MB / 1.82 GB | 5.40 MB/s
UPLOAD | video01.mp4 | 50% | 931.00 MB / 1.82 GB | 5.60 MB/s
WORKER 1 | SUCCESS | video01.mp4
WORKER 1 | DELETE | video01.mp4 | free_after=183.18 GB
```

## Quick start

```bash
cp .env.example .env
nano .env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m folder_telegram_uploader.app
```

## Important variables

- `WATCH_DIR`: local folder to monitor
- `KEEP_FREE_GB`: minimum free disk to keep available
- `MAX_FILE_GB`: skip files larger than this limit
- `UPLOAD_WORKERS`: keep `1` for safest uploads
- `LOG_FILE`: detailed log file path

## systemd

```bash
sudo cp deploy/folder-telegram-uploader.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable folder-telegram-uploader
sudo systemctl start folder-telegram-uploader
sudo journalctl -u folder-telegram-uploader -f
```
