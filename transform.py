import csv
import json
from os import remove as rm
from pathlib import Path
from cyrtranslit import to_latin
from loguru import logger


class Base:
    truename = "base"


    def __init__(self, dict_):
        self.id_ = dict_["id"]

    def to_dict(self):
        content = self.__dict__
        return content

    def write(self, path):
        ename = self.truename
        content = self.to_dict()
        with open(
                f"{path}/{self.id_}.csv",
                'w', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(content.keys())
            writer.writerow(content.values())

class Tusers(Base):
    truename = "users"


    def __init__(self, dict_):
        self.id_ = dict_["id"]
        self.name = dict_["name"]
        self.email = dict_["email"]

class Tleads(Base):
    truename = "leads"

    def __init__(self, dict_):
        self.id_ = dict_["id"]
        self.name = dict_["name"]
        self.price = dict_["price"]
        self.responsible_user_id = dict_["responsible_user_id"]
        self.group_id = dict_["group_id"]
        self.status_id = dict_["status_id"]
        self.pipeline_id = dict_["pipeline_id"]
        self.loss_reason_id = dict_["loss_reason_id"]
        self.created_by = dict_["created_by"]
        self.updated_by = dict_["updated_by"]
        self.created_at = dict_["created_at"]
        self.updated_at = dict_["updated_at"]
        self.closed_at = dict_["closed_at"]
        self.closest_task_at = dict_["closest_task_at"]
        self.is_deleted = dict_["is_deleted"]
        self.score = dict_["score"]
        self.account_id = dict_["account_id"]
        self.labor_cost = dict_["labor_cost"]
        self.custom_fields_values = dict_["custom_fields_values"]

    def to_dict(self):
        content = self.__dict__
        if content["custom_fields_values"] is not None:
            for cf in content["custom_fields_values"]:
                field_name = ''.join(
                    c for c in to_latin(cf["field_name"], lang_code='ru')
                    if c.isalpha() or c.isdigit()
                )
                field_value = ",".join(str(v["value"]) for v in cf["values"])
                content[field_name] = field_value
                try:
                    content.pop("custom_fields_values")
                except KeyError:
                    pass
            return content
        return content

class Tevents(Base):
    truename = "events"

    def __init__(self, dict_):
        self.id_ = dict_["id"]
        self.type_ = dict_["type"]
        self.entity_id = dict_["entity_id"]
        self.entity_type = dict_["entity_type"]
        self.created_by = dict_["created_by"]
        self.created_at = dict_["created_at"]
        self.account_id = dict_["account_id"]


class Tstatuses(Tevents):
    truename = "statuses"

    def __init__(self, dict_):
        self.id_ = dict_["id"]
        self.type_ = dict_["type"]
        self.entity_id = dict_["entity_id"]
        self.entity_type = dict_["entity_type"]
        self.created_by = dict_["created_by"]
        self.created_at = dict_["created_at"]
        self.account_id = dict_["account_id"]
        self.value_after_status_id = dict_["value_after"][0]["lead_status"]["id"]
        self.value_after_pipeline_id = dict_["value_after"][0]["lead_status"]["pipeline_id"]
        self.value_before_status_id = dict_["value_before"][0]["lead_status"]["id"]
        self.value_before_pipeline_id = dict_["value_before"][0]["lead_status"]["pipeline_id"]

class Tcalls(Base):
    truename = "calls"

    def __init__(self, dict_):
        self.id_ = dict_["id"]
        self.entity_id = dict_["entity_id"]
        self.created_by = dict_["created_by"]
        self.updated_by = dict_["updated_by"]
        self.created_at = dict_["created_at"]
        self.updated_at = dict_["updated_at"]
        self.responsible_user_id = dict_["responsible_user_id"]
        self.group_id = dict_["group_id"]
        self.uniq = dict_["params"]["uniq"]
        # self.account_id = dict_["params"]["account_id"]
        self.duration = dict_["params"]["duration"]
        self.source = dict_["params"]["source"]
        self.phone = dict_["params"]["phone"]

class Tpipelines(Base):
    truename = "pipelines"

    def __init__(self, dict_):
        self.pipeline_id = dict_["id"]
        self.pipeline_name = dict_["name"]
        self.is_archive = dict_["is_archive"]
        self.statuses = dict_["_embedded"]["statuses"]

    def write_statuses(self, path):
        for status in self.statuses:
            self.id_ = status["id"]
            content = {
                "id_": status["id"],
                "pipeline_id": self.pipeline_id,
                "pipeline_name": self.pipeline_name,
                "pipeline_is_acrhive": self.is_archive,
                "name": status["name"],
                "sort": status["sort"]
            }
            with open(
                    f"{path}/{content['id_']}.csv",
                    'w', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(content.keys())
                writer.writerow(content.values())

class Transform:
    def __init__(self, amo, entity):
       self.amo = amo
       self.entity = entity
       self.name = entity.truename
       self.input_path = f"temp_data/{amo}/{self.name}/"
       self.output_path = f"temp_data/{self.amo}/{self.name}_transformed/"

    def transform_file(self, json_file):
        for entry in json_file:
            entity = self.entity(entry)
            entity.write(self.output_path)


    def cleanup(self):
        for p in Path(self.input_path).iterdir():
            rm(p)


    def _all(self):
        for p in Path(self.input_path).iterdir():
            with open(p, "r") as f:
                j = json.load(f)
                self.transform_file(j)
        logger.success("Transform successful!")
        return True

class TransformPipelines(Transform):

    def __init__(self, amo, entity):
       self.amo = amo
       self.entity = entity
       self.name = entity.truename
       self.input_path = f"temp_data/{amo}/{self.name}/"
       self.output_path = f"temp_data/{self.amo}/{self.name}_transformed/"

    def transform_file(self, json_file):
        for entry in json_file:
            entity = self.entity(entry)
            entity.write_statuses(self.output_path)
