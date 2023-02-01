import datetime
import json
from dateutil import parser
from extract import Extract
from amo.entities import Statuses
from setup import amo20
from transform import Tstatuses, Transform
from load import Load
from loguru import logger


AMO = 'yastaff'
ENTITY = Tstatuses

with open("yastaff_lastdate_statuses.txt", "r") as f:
    LAST_DATE = str(
       datetime.timestamp(parser.parse(f.read()))
   ).split('.', maxsplit=1)[0]

method = Statuses(AMO).created_at(from_=LAST_DATE)
if __name__ == "__main__":
    logger.info(f"starting etl: {ENTITY.truename}")
    extract = Extract(amo20, method)
    extract._all()

    with open("yastaff_lastdate_statuses.txt", "w") as f:
        f.write(str(datetime.now()))

    transform = Transform(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    # load.backup()
    if load.in_batches():
        load.cleanup()
