set -euo pipefail

# Docker-in-Docker setup
echo "=== Docker setup ==="
sudo chown -R vscode:vscode /var/run/docker.sock || true
# Removing credsStore from Docker config, problems with
# using docker client in python script to build images
# The problematic line looked like this:
#	"credsStore": "dev-containers-..."
mkdir -p ~/.docker && echo '{}' > ~/.docker/config.json
sudo chmod o+r+w /var/run/docker.sock 

# Fix ownership on mounted volumes (Docker volumes are usually root:root)
echo "=== AWS setup ==="
sudo chown -R vscode:vscode /home/vscode/.aws || true

echo "=== Cline persistence setup starting ==="

# Fix ownership on mounted volumes (Docker volumes are usually root:root)
sudo chown -R vscode:vscode /home/vscode/.cline || true
sudo chown -R vscode:vscode /home/vscode/.cline-history || true

# Ensure all needed subdirectories exist in the persistent volume
mkdir -p /home/vscode/.cline-history/{cache,checkpoints,settings,state,tasks}

# Ensure the target globalStorage directory exists
sudo mkdir -p /home/vscode/.vscode-server/data/User/globalStorage/saoudrizwan.claude-dev
sudo chown -R vscode:vscode /home/vscode/.vscode-server/data || true

# Create symlinks so Cline writes/reads from the persistent volume
ln -sfn /home/vscode/.cline-history/cache    /home/vscode/.vscode-server/data/User/globalStorage/saoudrizwan.claude-dev/cache || true
ln -sfn /home/vscode/.cline-history/checkpoints    /home/vscode/.vscode-server/data/User/globalStorage/saoudrizwan.claude-dev/checkpoints || true
ln -sfn /home/vscode/.cline-history/settings /home/vscode/.vscode-server/data/User/globalStorage/saoudrizwan.claude-dev/settings || true
ln -sfn /home/vscode/.cline-history/state    /home/vscode/.vscode-server/data/User/globalStorage/saoudrizwan.claude-dev/state || true
ln -sfn /home/vscode/.cline-history/tasks    /home/vscode/.vscode-server/data/User/globalStorage/saoudrizwan.claude-dev/tasks || true

echo "Persisted directories:"
ls -ld /home/vscode/.cline-history/* 2>/dev/null || true

echo "=== Cline persistence setup completed ==="

# SSH keys
echo "=== SSH setup ==="
sudo chown -R vscode:vscode /home/vscode/.ssh || true

# Kilo persistence setup
echo "=== Kilo persistence setup ==="
sudo chown -R vscode:vscode /home/vscode/.config/kilo || true
# sudo chown -R vscode:vscode /home/vscode/.local/share/kilo || true
sudo chown -R vscode:vscode /home/vscode/.local || true

# Ensure Kilo cache subdirectory exists
mkdir -p /home/vscode/.cache/kilo/bin

echo "Persisted Kilo directories:"
ls -ld /home/vscode/.config/kilo /home/vscode/.local/share/kilo /home/vscode/.cache/kilo 2>/dev/null || true
echo "=== Kilo persistence setup completed ==="

echo "=== Installing project dev dependencies ==="
pip install -e .[dev,test,deploy] -c constraints.txt
pre-commit install
pre-commit install-hooks

echo "=== Pinning linter versions to match pre-commit ==="
pip install black isort pydocstyle mypy -c constraints.txt
# TODO: include types-docker after urllib3>=2 is possible
pip install types-requests types-retry types-setuptools types-botocore types-boto3 types-boto3-account types-jsonschema types-python-dateutil -c constraints.txt