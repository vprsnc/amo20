import json
from extract import Extract
from amo.entities import Pipelines
from setup import amo20
from transform import Tpipelines, TransformPipelines
from load import Load

AMO = 'yastaff'
ENTITY = Tpipelines

method = Pipelines('yastaff')  # .created_at(from_=)



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
