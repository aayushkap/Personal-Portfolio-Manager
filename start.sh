#!/bin/bash
#  Cleanup
find . -type d -name "__pycache__" -exec rm -r {} +
pkill -9 -f "app.api.__init__" 2>/dev/null
pkill -9 -f "chromium" 2>/dev/null
pkill -9 -f "playwright" 2>/dev/null


#  Activate virtualenv
source venv/bin/activate

#  Check DEBUG in .env
if grep -q '^DEBUG *= *true' .env; then
    echo "DEBUG=true, running normally..."
    python -m app.api.__init__
else
    echo "DEBUG not true, running in background..."
    nohup python -m app.api.__init__ > app.log 2>&1 &
    echo $! > app.pid  # optional: save the PID
fi
