import subprocess
import json
from dateutil import parser
from datetime import datetime
from extract import Extract
from amo.entities import Statuses
from setup import amo20
from transform import Tstatuses, Transform
from load import Load
from loguru import logger


AMO = 'yastaff'
ENTITY = Tstatuses

with open("yastaff_lastdate_statuses.txt", "r") as f:
    LAST_DATE = f.read()

logger.add(
    f'logs/{AMO}_{ENTITY.truename}.log', backtrace=True,
    diagnose=True, level='DEBUG'
)


method = Statuses(AMO).created_at(from_=LAST_DATE)

if __name__ == "__main__":
    try:
        logger.info(f"starting etl: {ENTITY.truename}")
        extract = Extract(amo20, method)
        extract._all()

        with open("yastaff_lastdate_statuses.txt", "w") as f:
            f.write(str(datetime.now()))

            transform = Transform(AMO, ENTITY)
        if transform._all():
            transform.cleanup()

        load = Load(AMO, ENTITY.truename)
        if load.in_batches():
            load.cleanup()
    except Exception as e:
        subprocess.Popen(f"echo '{e}' | msmtp analytics@oddjob.ru georgy@analytics-abc.xyz")
        logger.critical(e)
