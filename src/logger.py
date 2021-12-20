from functools import wraps
import datetime as dt 
import logging

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()

fmt = '%(asctime)s %(message)s'
formatter = logging.Formatter(fmt, datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def log_pipeline(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = dt.datetime.now()
        in_data = args[0].shape
        result = func(*args, **kwargs)
        end = (dt.datetime.now() - start).total_seconds()
        duration = str(dt.timedelta(seconds=end)).split('.')[0]
        logger.info(f"[{func.__name__}]  | in: {in_data}  |  out: {result.shape}  |  duration: {duration}")
        return result
    
    return wrapper