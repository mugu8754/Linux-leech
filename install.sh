#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/opt/folder-telegram-uploader"
SERVICE_NAME="folder-telegram-uploader"

echo "Installing dependencies..."
sudo apt update
sudo apt install -y python3 python3-pip python3-venv

sudo mkdir -p "$REPO_DIR"
sudo cp -r ./* "$REPO_DIR/"
cd "$REPO_DIR"

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

sudo cp deploy/folder-telegram-uploader.service /etc/systemd/system/
sudo sed -i 's|/usr/bin/python3|/opt/folder-telegram-uploader/.venv/bin/python|' /etc/systemd/system/folder-telegram-uploader.service
sudo systemctl daemon-reload
sudo systemctl enable folder-telegram-uploader

echo "Done. Copy .env.example to .env, edit values, then run:"
echo "sudo systemctl start $SERVICE_NAME"
echo "sudo journalctl -u $SERVICE_NAME -f"
