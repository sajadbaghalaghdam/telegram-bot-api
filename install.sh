#!/usr/bin/env bash
# ==============================================================================
# Telegram Bot API Server (Rust) - One-Line Production Installer & Service Setup
# Repository: https://github.com/sajadbaghalaghdam/telegram-bot-api
# ==============================================================================
set -euo pipefail

RED='\033[031m'
GREEN='\033[032m'
BLUE='\033[034m'
CYAN='\033[036m'
NC='\033[0m'

echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  Telegram Bot API Server (Pure Rust) Installer${NC}"
echo -e "${CYAN}============================================================${NC}"

# Check root privileges
if [ "$(id -u)" -ne 0 ]; then
    echo -e "${RED}[ERROR] Please run this installer as root (e.g. sudo bash install.sh)${NC}"
    exit 1
fi

REPO="sajadbaghalaghdam/telegram-bot-api"
BIN_DEST="/usr/local/bin/telegram-bot-api"
DATA_DIR="/var/lib/telegram-bot-api"
SERVICE_FILE="/etc/systemd/system/telegram-bot-api.service"

# Detect Architecture
ARCH=$(uname -m)
case "$ARCH" in
    x86_64|amd64)
        # Check if AVX2 is supported
        if grep -q avx2 /proc/cpuinfo 2>/dev/null; then
            ASSET_NAME="telegram-bot-api-rs-x86_64-unknown-linux-gnu-avx2"
        else
            ASSET_NAME="telegram-bot-api-rs-x86_64-unknown-linux-gnu"
        fi
        ;;
    aarch64|arm64)
        ASSET_NAME="telegram-bot-api-rs-aarch64-unknown-linux-gnu"
        ;;
    *)
        echo -e "${RED}[ERROR] Unsupported architecture: $ARCH${NC}"
        exit 1
        ;;
esac

echo -e "${BLUE}[INFO] Detected Architecture:${NC} $ARCH (Target Asset: $ASSET_NAME)"

# Fetch latest release binary URL
DOWNLOAD_URL="https://github.com/${REPO}/releases/latest/download/${ASSET_NAME}"
echo -e "${BLUE}[INFO] Downloading binary from:${NC} $DOWNLOAD_URL"

curl -fsSL -L "$DOWNLOAD_URL" -o "$BIN_DEST"
chmod +x "$BIN_DEST"

echo -e "${GREEN}[OK] Installed binary to:${NC} $BIN_DEST"

# Ensure Data Directory exists
mkdir -p "$DATA_DIR"
chmod 750 "$DATA_DIR"

# API ID & HASH Configuration
API_ID="${TELEGRAM_API_ID:-}"
API_HASH="${TELEGRAM_API_HASH:-}"
HTTP_PORT="${TELEGRAM_HTTP_PORT:-8080}"

if [ -z "$API_ID" ] || [ -z "$API_HASH" ]; then
    echo ""
    echo -e "${CYAN}------------------------------------------------------------${NC}"
    echo -e "${CYAN} Telegram API Credentials Setup (from my.telegram.org)${NC}"
    echo -e "${CYAN}------------------------------------------------------------${NC}"
    if [ -z "$API_ID" ]; then
        read -rp "Enter Telegram API ID: " API_ID
    fi
    if [ -z "$API_HASH" ]; then
        read -rp "Enter Telegram API Hash: " API_HASH
    fi
fi

# Create Systemd Service File
echo -e "${BLUE}[INFO] Creating systemd service file:${NC} $SERVICE_FILE"
cat <<EOF > "$SERVICE_FILE"
[Unit]
Description=Telegram Bot API Server (Pure Rust)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$DATA_DIR
ExecStart=$BIN_DEST --api-id=$API_ID --api-hash=$API_HASH --http-port=$HTTP_PORT --dir=$DATA_DIR --local
Restart=always
RestartSec=2s
KillSignal=SIGKILL
FinalKillSignal=SIGKILL
TimeoutStopSec=5s
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

# Reload & Start Service
systemctl daemon-reload
systemctl enable telegram-bot-api.service
systemctl restart telegram-bot-api.service

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  Installation Complete & Service Started!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo -e "Service Status:  systemctl status telegram-bot-api"
echo -e "Service Logs:    journalctl -u telegram-bot-api -f"
echo -e "HTTP Endpoint:   http://127.0.0.1:$HTTP_PORT"
echo -e "Data Directory:  $DATA_DIR"
echo -e "${GREEN}============================================================${NC}"
