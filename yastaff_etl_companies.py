import json
from extract import Extract
from amo.entities import Companies
from setup import amo20
from transform import Tcompanies, Transform
from load import LoadWithSchemaUpdate
from loguru import logger


AMO = 'yastaff'
ENTITY = Tcompanies

logger.add(
    f'logs/{AMO}_{ENTITY.truename}.log', backtrace=True,
    diagnose=True, level='DEBUG'
)

method = Companies(AMO)#.created_at(from_="2022-12-15") #TODO

if __name__ == "__main__":
    logger.info(f"starting etl: {ENTITY.truename}")
    extract = Extract(amo20, method)
    extract._all()

    transform = Transform(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = LoadWithSchemaUpdate(AMO, ENTITY.truename)
    load.backup()
    if load.in_batches():
        load.cleanup()
