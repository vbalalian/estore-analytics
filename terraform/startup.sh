#!/bin/bash
# Dagster VM Startup Script
# This script is run once when the VM is first created.
# Variables are injected by Terraform's templatefile function.

set -euo pipefail

# Variables injected by Terraform
DEPLOY_USER="${deploy_user}"
GCP_PROJECT="${gcp_project_id}"
SLACK_TOKEN="${slack_bot_token}"
REPO_URL="${repo_url}"

HOME_DIR="/home/$DEPLOY_USER"
PROJECT_DIR="$HOME_DIR/estore-analytics"
VENV_DIR="$PROJECT_DIR/.venv"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting Dagster VM provisioning..."

# 1. Create deploy user if it doesn't exist
if ! id "$DEPLOY_USER" &>/dev/null; then
    log "Creating user: $DEPLOY_USER"
    useradd -m -s /bin/bash "$DEPLOY_USER"
fi

# 2. Install system dependencies
log "Installing system dependencies..."
apt-get update
apt-get install -y python3 python3-venv python3-pip git

# 3. Clone repository
if [ ! -d "$PROJECT_DIR" ]; then
    log "Cloning repository..."
    sudo -u "$DEPLOY_USER" git clone "$REPO_URL" "$PROJECT_DIR"
else
    log "Repository already exists, pulling latest..."
    sudo -u "$DEPLOY_USER" git -C "$PROJECT_DIR" pull
fi

# 4. Create virtualenv and install dependencies
log "Setting up Python virtual environment..."
sudo -u "$DEPLOY_USER" python3 -m venv "$VENV_DIR"
sudo -u "$DEPLOY_USER" "$VENV_DIR/bin/pip" install --upgrade pip
sudo -u "$DEPLOY_USER" "$VENV_DIR/bin/pip" install -e "$PROJECT_DIR/dagster-project[dev]"

# 5. Create dagster.yaml
log "Creating dagster.yaml..."
cat > "$PROJECT_DIR/dagster-project/dagster.yaml" << EOF
storage:
  sqlite:
    base_dir: $PROJECT_DIR/dagster-project/dagster_home

run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher
EOF
chown "$DEPLOY_USER:$DEPLOY_USER" "$PROJECT_DIR/dagster-project/dagster.yaml"

# 6. Create .env file
log "Creating .env file..."
cat > "$PROJECT_DIR/dagster-project/.env" << EOF
GCP_PROJECT_ID=$GCP_PROJECT
EOF
chown "$DEPLOY_USER:$DEPLOY_USER" "$PROJECT_DIR/dagster-project/.env"

# 7. Set up bashrc exports
if ! grep -q "DBT_PROFILES_DIR" "$HOME_DIR/.bashrc"; then
    log "Adding environment exports to .bashrc..."
    cat >> "$HOME_DIR/.bashrc" << 'EOF'
export DBT_PROFILES_DIR=~/estore-analytics/dbt-project/
EOF
fi

# 8. Install systemd service: dagster (daemon)
log "Installing dagster.service..."
cat > /etc/systemd/system/dagster.service << EOF
[Unit]
Description=Dagster Daemon
After=network.target

[Service]
Type=simple
User=$DEPLOY_USER
WorkingDirectory=$PROJECT_DIR/dagster-project
ExecStart=$VENV_DIR/bin/dagster-daemon run
Restart=always
RestartSec=10
Environment="PATH=$VENV_DIR/bin:/usr/local/bin:/usr/bin:/bin"
Environment="DBT_PROFILES_DIR=$PROJECT_DIR/dbt-project"
Environment="DAGSTER_HOME=$PROJECT_DIR/dagster-project"

[Install]
WantedBy=multi-user.target
EOF

# 9. Install systemd service: dagster-webserver
log "Installing dagster-webserver.service..."
cat > /etc/systemd/system/dagster-webserver.service << EOF
[Unit]
Description=Dagster Webserver
After=network.target

[Service]
Type=simple
User=$DEPLOY_USER
WorkingDirectory=$PROJECT_DIR/dagster-project
ExecStart=$VENV_DIR/bin/dagster-webserver -h 127.0.0.1 -p 3000
Restart=always
RestartSec=10
Environment="PATH=$VENV_DIR/bin:/usr/local/bin:/usr/bin:/bin"
Environment="DBT_PROFILES_DIR=$PROJECT_DIR/dbt-project"

[Install]
WantedBy=multi-user.target
EOF

# 10. Create override for secrets (Slack token)
log "Creating systemd override for secrets..."
mkdir -p /etc/systemd/system/dagster.service.d
cat > /etc/systemd/system/dagster.service.d/override.conf << EOF
[Service]
Environment=SLACK_BOT_TOKEN=$SLACK_TOKEN
EOF

# 11. Enable and start services
log "Enabling and starting services..."
systemctl daemon-reload
systemctl enable dagster dagster-webserver
systemctl start dagster-webserver
systemctl start dagster

log "Dagster VM provisioning complete!"
log "Access Dagster UI via SSH tunnel: gcloud compute ssh INSTANCE -- -L 3000:localhost:3000"
