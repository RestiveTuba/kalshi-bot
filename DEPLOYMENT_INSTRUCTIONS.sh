#!/bin/bash
# Deployment instructions for validation phase tools
# Run this to copy validation phase tools to the production server

# This assumes:
# - You're running this from the repo root
# - You have SSH access to root@147.182.132.170
# - /root/kalshi-bot is your bot directory on the server

echo "Deploying validation phase tools to Kalshi bot server..."
echo ""

# Set server details
SERVER="root@147.182.132.170"
BOT_DIR="/root/kalshi-bot"

FILES=(
  audit_daily.py
  backtest_probability_model.py
  CREDENTIAL_ROTATION_CHECKLIST.md
  VALIDATION_PHASE_README.md
)

for file in "${FILES[@]}"; do
    echo "Copying ${file} to server..."
    scp "$file" "${SERVER}:${BOT_DIR}/${file}"
    if [ $? -ne 0 ]; then
        echo "   ✗ Failed to copy ${file}"
        exit 1
    fi
    echo "   ✓ ${file} deployed"
done

ssh "$SERVER" "chmod +x ${BOT_DIR}/audit_daily.py ${BOT_DIR}/backtest_probability_model.py"

echo ""

# Verify deployment
echo "Verifying deployment..."
ssh "$SERVER" "ls -lh ${BOT_DIR}/audit_daily.py ${BOT_DIR}/backtest_probability_model.py ${BOT_DIR}/CREDENTIAL_ROTATION_CHECKLIST.md ${BOT_DIR}/VALIDATION_PHASE_README.md"

echo ""
echo "✓ Deployment complete!"
echo ""
echo "Next steps:"
echo "  1. SSH to server: ssh root@147.182.132.170"
echo "  2. Test audit script: python3 ${BOT_DIR}/audit_daily.py"
echo "  3. Test backtester stub: python3 ${BOT_DIR}/backtest_probability_model.py"
echo "  4. Save audit output to log: python3 ${BOT_DIR}/audit_daily.py >> ${BOT_DIR}/audit_daily.log"
echo ""
