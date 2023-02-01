import json
from extract import Extract
from amo.entities import Statuses
from setup import amo2
from transform import Tstatuses, Transform
from load import Load
from loguru import logger

AMO = 'orps'
ENTITY = Tstatuses

method = Statuses(AMO)#.created_at(from_="2022-12-15")
if __name__ == "__main__":
    logger.info(f"starting etl: {ENTITY.truename}")
    extract = Extract(amo21, method)
    extract._all()
    
    with open("yastaff_lastdate_statuses.txt", "w") as f: # TODO read lastdate
        f.write(str(datetime.now()))

    transform = Transform(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    # load.backup()
    if load.in_batches():
        load.cleanup()
