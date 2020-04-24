import re
from fastapi import FastAPI, Query
from utils import load_fasttext_model, logger_factory
from apscheduler.schedulers.background import BackgroundScheduler
from recsys import train_job

logger = logger_factory()
# first time train
train_job()


# api
app = FastAPI()
fast_model=load_fasttext_model('model/fasttext.bin')

def prepare_process(text):
    return re.sub(r'[^\w\s]', '', text).strip().lower()


@app.get('/predict/')
async def get_predict(
    content: str,
    limit: int = 4
):
    lables, _ = fast_model.predict(prepare_process(content), k=limit+1)
    lables = list(lables)
    prefix_len = len("__label__")
    if '__label__null' in lables:
        lables.remove('__label__null')
    if len(lables) > limit:
        lables = lables[0: limit]
    return [ label[prefix_len:] for label in lables]


#  schedule train model
scheduler = BackgroundScheduler()
@scheduler.scheduled_job("cron", day_of_week='*', hour='1', minute='30', second='30')
def train():
    logger.info('start train job')
    train_job()
    global fast_model
    fast_model=load_fasttext_model('model/fasttext.bin')
    logger.info('finish train job')
try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()