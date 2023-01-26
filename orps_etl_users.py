import json
from extract import Extract
from amo.entities import Users
from setup import amo21
from transform import Tusers, Transform
from load import Load
from loguru import logger

AMO = 'orps'
ENTITY = Tusers

method = Users(AMO)  # .created_at(from_=)

if __name__ == "__main__":
    logger.info(f"starting etl: {ENTITY.truename}")
    extract = Extract(amo21, method)
    extract._all()

    transform = Transform(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    load.backup()
    if load.in_batches():
        load.cleanup()
