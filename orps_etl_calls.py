import json
from extract import ExtractCalls
from amo.entities import Calls
from setup import amo21
from transform import Tcalls, Transform
from load import Load
from loguru import logger

AMO = 'orps'
ENTITY = Tcalls
method = Calls(AMO)#.created_at(from_="2022-12-15")

if __name__ == "__main__":
    extract = ExtractCalls(amo21, method)
    extract._all()

    transform = Transform(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    load.backup()
    if load.in_batches():
        load.cleanup()
