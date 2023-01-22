import json
from extract import Extract
from amo.entities import Users
from setup import amo20
from transform import Tusers, Transform
from load import Load

AMO = 'yastaff'
ENTITY = Tusers

method = Users('yastaff')  # .created_at(from_=)
extract = Extract(amo20, method)

transform = Transform(AMO, ENTITY)

load = Load(AMO, ENTITY.truename)

if __name__ == "__main__":
    extract._all()

    if transform._all():
        transform.cleanup()

    load.backup()
    if load.in_batches():
        load.cleanup()
