import json
from extract import ExtractCalls
from amo.entities import Calls
from setup import amo20
from transform import Tcalls, Transform
from load import Load
from loguru import logger

AMO = 'yastaff'
ENTITY = Tcalls

logger.add(
    f'logs/{AMO}_{ENTITY.truename}.log', backtrace=True,
    diagnose=True, level='DEBUG'
)

method = Calls(AMO)#.created_at(from_="2022-12-15")

if __name__ == "__main__":
    logger.info(f"starting etl: {ENTITY.truename}")
    extract = ExtractCalls(amo20, method)
    extract._all()

    transform = Transform(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    load.backup()
    if load.in_batches():
        load.cleanup()
