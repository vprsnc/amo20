import json
from extract import Extract
from amo.entities import Pipelines
from setup import amo20
from transform import Tpipelines, TransformPipelines
from load import Load

AMO = 'yastaff'
ENTITY = Tpipelines

method = Pipelines('yastaff')  # .created_at(from_=)
extract = Extract(amo20, method)

transform = TransformPipelines(AMO, ENTITY)

load = Load(AMO, ENTITY.truename)

if __name__ == "__main__":
    extract._all()

    if transform._all():
        transform.cleanup()

    load.backup()
    if load.in_batches():
        load.cleanup()
