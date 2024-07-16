import time
from apscheduler.schedulers.background import BackgroundScheduler,BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.code.python.el_class import ElClass



obj = ElClass()


def tick():
    obj.extract_load(batch_size=500)


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(tick, 'interval', minutes=10)
    scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()