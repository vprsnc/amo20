import json
from extract import Extract
from amo.entities import Pipelines
from setup import amo21
from transform import Tpipelines, TransformPipelines
from load import Load
from loguru import logger

AMO = 'orps'
ENTITY = Tpipelines

logger.add(
    f'logs/{AMO}_{ENTITY.truename}.log', backtrace=True,
    diagnose=True, level='DEBUG'
)

method = Pipelines(AMO)  # .created_at(from_=)


if __name__ == "__main__":
    logger.info(f"starting etl: {ENTITY.truename}")
    extract = Extract(amo21, method)
    extract._all()

    transform = TransformPipelines(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    load.backup()
    if load.in_batches():
        load.cleanup()
