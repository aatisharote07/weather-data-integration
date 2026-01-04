import time
import schedule
import logging
from datetime import datetime
from manual_runner import run_pipeline

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def job():
    logger.info("⏰ Triggering Scheduled ETL Job...")
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Job failed: {e}")

def run_scheduler():
    # Schedule the job every day at a specific time (e.g., 9:00 AM)
    # OR for demonstration, every 24 hours from when script starts
    
    # schedule.every().day.at("09:00").do(job)
    
    # For now, let's just run it every 24 hours
    schedule.every(24).hours.do(job)
    
    logger.info("🚀 Scheduler Started! Data will be collected every 24 hours.")
    logger.info("Press CTRL+C to stop.")
    
    # Run once immediately so we verify it works
    job()
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    # Ensure schedule library is installed
    try:
        import schedule
    except ImportError:
        logger.error("Schedule library not found. Installing...")
        import subprocess
        subprocess.check_call(["pip", "install", "schedule"])
        import schedule

    run_scheduler()
