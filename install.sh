#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/tg-pool"

echo "==> Installing system deps..."
sudo apt update
sudo apt -y install python3-venv python3-pip git rsync

echo "==> Creating app dir $APP_DIR ..."
sudo mkdir -p "$APP_DIR"
sudo chown "$USER":"$USER" "$APP_DIR"

echo "==> Sync repo files to $APP_DIR ..."
SRC_DIR="$(pwd)"
rsync -a --exclude ".git" --exclude ".venv" --exclude "sessions" "$SRC_DIR"/ "$APP_DIR"/

cd "$APP_DIR"

echo "==> Python venv & requirements..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt

echo "==> Prepare folders & env..."
mkdir -p sessions
[ -f .env ] || cp .env.example .env
chmod 600 .env || true

echo "==> Install systemd service..."
sudo cp service/tg-pool.service /etc/systemd/system/tg-pool.service
sudo systemctl daemon-reload
sudo systemctl enable --now tg-pool

echo "==> Done. Tail logs:"
sudo journalctl -u tg-pool -n 50 --no-pager
echo
echo "==> Upload your .session files now:"
echo "   scp \"C:\\\\Path\\\\to\\\\*.session\" root@<VPS_IP>:/opt/tg-pool/sessions/"
echo "   (then on VPS) chmod 600 /opt/tg-pool/sessions/*.session && sudo systemctl restart tg-pool"
echo
echo "==> API:"
echo "  Status : curl -H 'Authorization: Bearer <API_BEARER>' http://127.0.0.1:8080/status"
echo "  Add    : curl -H 'Authorization: Bearer <API_BEARER>' \"http://127.0.0.1:8080/channel?chanel=<name_or_@username>\""
echo "  Delete : curl -H 'Authorization: Bearer <API_BEARER>' \"http://127.0.0.1:8080/delete?chanel=<name>\""
