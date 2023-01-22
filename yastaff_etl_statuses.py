import json
from extract import Extract
from amo.entities import Statuses
from setup import amo20
from transform import Tstatuses, Transform
from load import Load

AMO = 'yastaff'
ENTITY = Tstatuses

method = Statuses('yastaff')#.created_at(from_="2022-12-15")
delete
if __name__ == "__main__":
        extract = Extract(amo20, method)
    extract._all()

    transform = TransformPipelines(AMO, ENTITY)
    if transform._all():
        transform.cleanup()

    load = Load(AMO, ENTITY.truename)
    load.backup()
    if load.in_batches():
        load.cleanup()
