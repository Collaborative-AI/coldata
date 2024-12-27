import schedule
import yaml
import time
from datetime import datetime
import subprocess
import sys


# Function to call the main crawling script
def run_crawler():
    try:
        print(f"[INFO] Starting crawl at {datetime.now()}")
        subprocess.run([sys.executable, "main.py"], check=True)
        print(f"[INFO] Crawl completed at {datetime.now()}")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Crawler failed: {e}")
    return


# Function to schedule crawling based on a period
def schedule_crawling(period):
    if period == "day":
        schedule.every().day.at("00:00").do(run_crawler)
    elif period == "week":
        schedule.every().week.at("00:00").do(run_crawler)
    elif period == "month":
        # Schedule on the first day of each month at midnight
        schedule.every(30).day.at("00:00").do(run_crawler)
    else:
        raise ValueError("Invalid period specified. Choose 'day', 'week', or 'month'.")

    print(f"[INFO] Crawler scheduled to run every {period}.")
    return


# Main function to parse arguments and start the scheduler
def main():
    config_path = 'config.yml'
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    if config['scheduler']['init_run']:
        run_crawler()
    # Schedule the crawling process
    schedule_crawling(config['scheduler']['period'])

    # Run the scheduler continuously
    print("[INFO] Scheduler is running. Press Ctrl+C to exit.")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
