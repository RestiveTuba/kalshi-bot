#!/bin/bash
cd ~/kalshi-bot
pkill -f momentum_bot.py 2>/dev/null
pkill -f market_maker.py 2>/dev/null
pkill -f dashboard.py 2>/dev/null
sleep 2
tmux new-session -d -s kalshi \
  'while true; do cd ~/kalshi-bot && python3 momentum_bot.py; sleep 5; done'
tmux new-window -t kalshi
tmux send-keys -t kalshi \
  'while true; do cd ~/kalshi-bot && python3 market_maker.py; sleep 5; done' Enter
tmux new-window -t kalshi
tmux send-keys -t kalshi \
  "while true; do cd ~/kalshi-bot && DASHBOARD_PASSWORD=${DASHBOARD_PASSWORD} python3 dashboard.py; sleep 5; done" Enter
echo "All bots started"
