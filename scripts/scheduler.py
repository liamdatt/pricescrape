import time
import schedule
import os
from scripts import pricedata_update

def job():
    print("Starting scheduled scrape...")
    try:
        pricedata_update.main()
    except Exception as e:
        print(f"Job failed: {e}")

# Schedule the job
schedule.every().day.at("18:30").do(job) # Time is based on container TZ

print("Scheduler started. Waiting for 18:30...")
while True:
    schedule.run_pending()
    time.sleep(60)
