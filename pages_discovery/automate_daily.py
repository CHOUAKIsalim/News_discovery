import schedule
import time
import os


def job():
    os.system("python3 main.py --lang ro --country RO --platform tiktok --extractor keybert")

schedule.every().day.at("06:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)