import schedule
import time
import os


def job():
    os.system("python3 main.py --lang ro --country RO --platform tiktok --extractor keybert --start_date 2025-03-28 --nb_days 7")

schedule.every().day.at("09:36").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)