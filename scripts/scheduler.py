import time
import schedule
import os
import sys
from pathlib import Path

# 1. Add the project root (one level up) to python path so we can import 'scripts' and 'pricedata'
# If this file is in /app/scripts, parent is /app
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 2. Add the scripts directory itself just in case
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# 3. Now try importing. 
# We try both absolute (from scripts...) and relative (direct import) to be safe.
try:
    from scripts import pricedata_update
except ImportError:
    import pricedata_update

def job():
    print("⏰ Starting scheduled scrape job...", flush=True)
    try:
        pricedata_update.main()
        print("✅ Job completed successfully.", flush=True)
    except Exception as e:
        print(f"❌ Job failed: {e}", flush=True)

# Schedule time (Container TZ should be America/Jamaica)
schedule.every().day.at("15:00").do(job)

print("🚀 Scheduler started. Waiting for 15:00...", flush=True)

while True:
    schedule.run_pending()
    time.sleep(60)