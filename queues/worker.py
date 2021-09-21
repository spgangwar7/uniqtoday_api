from celery import Celery

# Create the celery app and get the logger
celery_app = Celery('proj',
                    broker='pyamqp://guest:guest@localhost//',
                    backend='rpc://',
                    )

# Optional configuration, see the application user guide.
celery_app.conf.update(
    result_expires=3600,)

async def initializeCelery():
    celery_app.start()

"""
class Numbers(BaseModel):
    x: float
    y: float
@app.post('/add')
def enqueue_add(n: Numbers):
    # We use celery delay method in order to enqueue the task with the given parameters
    add.delay(n.x, n.y)
"""

@celery_app.task
def add(x, y):
    return x + y


@celery_app.task
def mul(x, y):
    return x * y


@celery_app.task
def xsum(numbers):
    return sum(numbers)