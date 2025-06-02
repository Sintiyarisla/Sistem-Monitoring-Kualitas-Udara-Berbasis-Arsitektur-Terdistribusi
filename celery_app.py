from celery import Celery

celery = Celery(
    "ispu_etl",
    broker="pyamqp://guest@localhost//",
    backend="rpc://"
)
