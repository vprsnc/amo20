import json
from extract import Extract
from amo.entities import Calls
from setup import amo20
from transform import Tcalls, Transform
from load import Load
from loguru import logger

AMO = 'yastaff'
ENTITY = Tcalls

method = Calls('yastaff')#.created_at(from_="2022-12-15")

if __name__ == "__main__":
    logger.info(f"starting etl: {ENTITY.truename}")
    # extract = Extract(amo20, method)
    # extract._all()

    transform = Transform(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    load.backup()
    if load.in_batches():
        load.cleanup()
