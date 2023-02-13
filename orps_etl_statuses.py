import json
from datetime import datetime #TODO
from extract import Extract
from amo.entities import Statuses
from setup import amo21
from transform import Tstatuses, Transform
from load import Load
from loguru import logger

AMO = 'orps'
ENTITY = Tstatuses

with open("yastaff_lastdate_statuses.txt", "r") as f:
    LAST_DATE = f.read()


logger.add(
    f'logs/{AMO}_{ENTITY.truename}.log', backtrace=True,
    diagnose=True, level='DEBUG'
)

method = Statuses(AMO).created_at(LAST_DATE)

if __name__ == "__main__":
    try:
        logger.info(f"starting etl: {ENTITY.truename}")
        extract = Extract(amo21, method)
        extract._all()
    
        with open("orps_lastdate_statuses.txt", "w") as f: # TODO read lastdate
            f.write(str(datetime.now()))

        transform = Transform(AMO, ENTITY)
        if transform._all():
            transform.cleanup()

        load = Load(AMO, ENTITY.truename)
        
        if load.in_batches():
            load.cleanup()
    except Exception as e:
        logger.critical(e)
