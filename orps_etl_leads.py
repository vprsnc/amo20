import subprocess
import json
from extract import Extract
from amo.entities import Leads
from setup import amo21
from transform import Tleads, Transform
from load import LoadWithSchemaUpdate
from loguru import logger

AMO = 'orps'
ENTITY = Tleads

logger.add(
    f'logs/{AMO}_{ENTITY.truename}.log', backtrace=True,
    diagnose=True, level='DEBUG'
)

method = Leads(AMO)#.created_at(from_="2022-12-15") #TODO

if __name__ == "__main__":
    try:
        logger.info(f"starting etl: {ENTITY.truename}")
        extract = Extract(amo21, method)
        extract._all()

        transform = Transform(AMO, ENTITY)
        if transform._all():
            transform.cleanup()

        load = LoadWithSchemaUpdate(AMO, ENTITY.truename)
        load.backup()
        if load.in_batches():
            load.cleanup()
    except Exception as e:
        subprocess.Popen(f"echo '{e}' | msmtp analytics@oddjob.ru georgy@analytics-abc.xyz")
        logger.critical(e)
