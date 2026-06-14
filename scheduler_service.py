import time
import logging
from datetime import datetime
import pytz
from apscheduler.schedulers.background import BackgroundScheduler

# 1. Setup structured logging to trace execution in your terminal logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Define the Target Timezone (CET)
CET_ZONE = pytz.timezone("Europe/Paris")  # Standard CET/CEST zone wrapper


# --- Define Your Tasks ---

def ten_minute_task():
    """Task that runs continuously every 2 minutes."""
    current_time = datetime.now(CET_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(f"⏰ [INTERVAL JOB]: Executing task. Current time: {current_time}")
    # Insert your processing logic here (e.g., triggering a scraper, syncing records, etc.)


def daily_cron_task():
    """Task that runs once a day at 23:45 CET."""
    current_time = datetime.now(CET_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(f"🎯 [CRON JOB]: Executing daily checklist at 23:45 CET! Current time: {current_time}")
    # Insert your critical daily logic here


# --- Initialize & Configure the Scheduler ---

def start_scheduler():
    # Initialize background scheduler explicitly tied to the CET timezone wrapper
    scheduler = BackgroundScheduler(timezone=CET_ZONE)

    # Job 1: Interval pattern (runs every 2 minutes)
    scheduler.add_job(
        id="interval_2_mins",
        func=ten_minute_task,
        trigger="interval",
        minutes=2,
        next_run_time=datetime.now(CET_ZONE)  # Forces it to run once immediately on startup
    )

    # Job 2: Cron pattern (runs every day at 23:45 / 11:45 PM CET)
    scheduler.add_job(
        id="daily_cron_2345",
        func=daily_cron_task,
        trigger="cron",
        hour=23,
        minute=45
    )

    # Start execution loop
    scheduler.start()
    logger.info("🚀 APScheduler initialized successfully in CET timezone.")
    logger.info(" -> Job 'interval_2_mins' scheduled for every 2 minutes.")
    logger.info(" -> Job 'daily_cron_2345' scheduled for 23:45 CET daily.")


if __name__ == "__main__":
    start_scheduler()

    # Since it's running as a BackgroundScheduler, your main thread must stay alive.
    # If incorporating into FastAPI, this infinite loop isn't necessary.
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler safely...")