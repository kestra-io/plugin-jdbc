set -euo pipefail

echo "📦 Disk usage (before)"
df -h || true
echo

echo "🧹 Docker cleanup..."
docker system prune -af --volumes || true
docker builder prune -af || true
echo "✅ Done"

echo "🧹 APT cleanup..."
sudo apt-get clean -y >/dev/null 2>&1 || true
sudo apt-get autoremove -y >/dev/null 2>&1 || true

echo
echo "📦 Disk usage (after)"
df -h || true
