import json
from extract import Extract
from amo.entities import Users
from setup import amo20
from transform import Tusers, Transform
from load import Load
from loguru import logger

AMO = 'yastaff'
ENTITY = Tusers


logger.add(
    f'logs/{AMO}_{ENTITY.truename}.log', backtrace=True,
    diagnose=True, level='DEBUG'
)

method = Users(AMO)  # .created_at(from_=)

if __name__ == "__main__":
    try:
        logger.info(f"starting etl: {ENTITY.truename}")
        extract = Extract(amo20, method)
        extract._all()

        transform = Transform(AMO, ENTITY)
        if transform._all():
            transform.cleanup()

        load = Load(AMO, ENTITY.truename)
        load.backup()
        if load.in_batches():
            load.cleanup()
    except Exception as e:
        logger.critical(e)
