import json
from extract import Extract
from amo.entities import Leads
from setup import amo20
from transform import Tleads, Transform
from load import LoadWithSchemaUpdate

AMO = 'yastaff'
ENTITY = Tleads

method = Leads('yastaff').created_at(from_="2022-12-15") #TODO
extract = Extract(amo20, method)

transform = Transform(AMO, ENTITY)

load = LoadWithSchemaUpdate(AMO, ENTITY.truename)

if __name__ == "__main__":
    extract._all()

    if transform._all():
        transform.cleanup()

    load.backup()
    if load.in_batches():
        load.cleanup()
