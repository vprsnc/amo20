

# Table of contents:


## [Extract](#org5038f20)


### [Logon](#org162e2db)


### [Amo api methods](#org9986c62)

1.  [Leads](#org1f01c53)

2.  [Statuses](#org851d76e)

3.  [Notes](#orgb8fa6ae)

4.  [Pipelines](#org9cb33ce)

5.  [Users](#org41f1ffd)


### [Extraction](#orgacc3caa)


### [Test](#orge9dfab3)


## [Transform](#orgf61e92d)


### [Transform Base](#org9915987)


### [Transform Users](#orgef13f65)


### [Transform Leads](#orgd5984a9)


### [Transform Statuses](#org2e20030)


### [Transform all](#orgb3f87ef)


## [Load](#org0bb0c4e)


## [ETL&rsquo;s themselves](#orgcc85c1e)


## [Shell to rule them all](#org4fb6869)


# Extract

This is the phase we will be talking to AmoCRM api with requests calls.
The idea is to download all the entities and write them page per page to the according folder in Json format.
This will be basically a list of dictionaries, easily handled by python.


## Logon

First, we need to specify the logon information that we will use to connect to amo.
To make it reproducible for multiple amocm entities, we&rsquo;ll create a class.
We also want this class to automatically get the code from system environment, if the code was specified.

    import requests
    import json
    from os import environ, path, makedirs
    from loguru import logger
    
    
    class Tokens:
    
        def __init__(self, id_, secret, subdomain, redirect):
            self.logon_data = {
                "client_id": id_,
                "client_secret": secret,
                "redirect_uri": redirect,
                "subdomain": subdomain
            }
            self.subdomain = subdomain
            self.path = f"tokens/{subdomain}"
            self.url = f"https://{subdomain}.amocrm.ru/oauth2/access_token"
            try:
                self.code = environ["CODE"]
            except KeyError:
                self.code = None
    
    
        def check_folder(self):
            if path.isdir(self.path) == False:
                makedirs(self.path)
    
    
        def get(self):
            if self.code is not None:
                self.logon_data["code"] = self.code
                self.logon_data["grant_type"] = "authorization_code"
    
                data = self.logon_data
                r = requests.post(self.url, data=data)
                return r
            logger.critical("You need to provide code!")
    
    
        def store(self, dict_, type_):
            self.check_folder()
            with open(f"{self.path}/{type_}.txt", 'w') as f:
                f.write(dict_[type_])
    
    
        def init(self):
            r = self.get()
            dict_ = json.loads(r.text)
            self.store(dict_, 'refresh_token')
            self.store(dict_, 'access_token')
    
    
        def refresh(self):
            file_ = f"{self.path}/refresh_token.txt"
            if path.exists(file_):
                with open(file_, 'r') as f:
                    refresh_token = f.read()
                self.logon_data["grant_type"] = "refresh_token"
                self.logon_data["refresh_token"] = refresh_token
                r = requests.post(self.url, self.logon_data)
                dict_ = json.loads(r.text)
                try:
                    self.store(dict_, 'access_token')
                    self.store(dict_, 'refresh_token')
                except KeyError:
                    logger.critical(dict_)
    
            else:
                self.init()
    
    
        def provide_access(self):
            self.refresh()
            file_ = f"{self.path}/access_token.txt"
            with open(file_, 'r') as f:
                self.access_token = f.read()
            return self.access_token

Than we need to pass our information to that class.
It&rsquo;s generally a good idea to store it in a separate file that will not be synchronized with `git`.

    from setup import amo20
    from os import environ
    
    # del environ["CODE"]
    # access_token = amo20.provide_access()
    amo20.refresh()


## Amo api methods

OK, let&rsquo;s put away our **etl** file for the moment and return to setting up things.
Next thing we need to do is specify method we&rsquo;re going to call **amocrm** api.

We&rsquo;ll be using `requests` library for all our calls, all we need is to use the correct **url** for each entity.
As a rule, all the calls to amo api are done like:

<https://$SUBDOMAIN.amocrm.ru/api/v4/$ENTITY?$PARAMETERS>

Some of the entities will have *sub-entities* in them, and sub-entity can also can have sub-types.
E.g. **incoming calls** are sub-type for **notes**, which is *sub-entity* to **leads**.
Crazy stuff, isn&rsquo;t it?

Anyway, let&rsquo;s define a basic class for calling api:

    from dateutil import parser # that's because of amo datetime filters
    
    class Base:
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/"
    
    
        def to_unix_timestamp(self, timestamp):
            """This function converts any datetime string to Unix format
                required by Amo to filter"""
            time_ = parser.parse(timestamp)
            return str(int(time_.timestamp()))
    
    
        def created_at(self, at_=None, from_=None):
            if at_ is not None:
                at_ = self.to_unix_timestamp(at_)
                self.url += f"&filter[created_at]={at_}"
            elif from_ is not None:
                from_ = self.to_unix_timestamp(from_)
                self.url += f"&filter[created_at][from]={from_}"
            return self
    
    
        def updated_at(self, at_=None, from_=None):
            if at_ is not None:
                at_ = self.to_unix_timestamp(at_)
                self.url += f"&filter[updated_at]={at_}"
            elif from_ is not None:
                from_ = self.to_unix_timestamp(from_)
                self.url += f"&filter[updated_at][from]={from_}"
            return self

Now we need to specify class for each entity that we need.
The list of entities is:


### Leads

Leads are the most difficult entity because it has custom fields, which can be different for each lead.
We&rsquo;ll deal with them on the [3](#orgf61e92d) stage.

    class Leads(Base):
        truename = "leads"
        basename = "leads"
        entity = "leads"
    
    
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity + "?"
    
    
        def filter(self):
            pass


### Statuses

By statuses we mean here `lead_status_changed` entity which is sub-type for events.

    class  Events(Base):
        entity = "events"
        truename = "statuses"
        basename = "events"
    
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity + "?"

    class Statuses(Events):
        sub_type = "?filter[type]=lead_status_changed"
    
    
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity\
                + self.sub_type


### Notes

Notes are sub-entity of [2.2.1](#org1f01c53), while calls are sub-type of Notes.

    class Notes(Leads):
        truename = "notes"
        basename = "notes"
        sub_entity = "/notes"
    
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity\
                + self.sub_entity + "?"

    class Calls(Notes):
        truename = "calls"
        sub_type = "?filter[note_type]=call_in"
    
    
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity\
                + self.sub_entity + self.sub_type


### Pipelines

Pipelines are sub-entity for [2.2.1](#org1f01c53)  and contain status names in themselves

    class Pipelines(Leads):
        truename = "pipelines"
        basename = "pipelines"
        sub_entity = "/pipelines"
    
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity\
                + self.sub_entity + "?"


### Users

Users are stand-alone entity.

    class Users(Base):
        truename = "users"
        entity = "users"
        basename = "users"
        def __init__(self, subdomain):
            self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity + "?"


## Extraction

Now, when we have all entities described as API methods, we can start extracting them.
For that we&rsquo;ll create some more classes.
Basically our the only method we need is just extract all the entities from Amo, with filters applied.
Because we defined filters as a base method in previous chapter, we can now just go ahead and define extraction method.

Firstly, of course we need to log in, so this will be an attribute of our base class.

    import requests
    import json
    from requests.adapters import HTTPAdapter
    from amo.logon import Tokens
    from loguru import logger
    
    
    class Extract:
        session = requests.Session()
    
        def __init__(self, tokens, entity):
            access_token = tokens.provide_access()
            header = {"Authorization": "Bearer " + access_token}
            self.session.headers.update(header)
            # And to prevent amo from dropping connection:
            self.session.mount('https://', HTTPAdapter(max_retries=5))
            self.basename = entity.basename
            self.truename = entity.truename        # name for writing and further pr...
            self.amo = tokens.subdomain
            self.url = entity.url
    
    
        def write(self, counter, content):
            with open(f'temp_data/{self.amo}/{self.truename}/{counter}.json', 'w',
                      encoding='utf-8') as f:
                json.dump(content, f)
    
    
        def parse_page(self, req):
            """All the amo API requests have the same Json schema."""
            return json.loads(req.text)
    
    
        def get_page(self, url, counter):
            req = self.session.get(url)
            page = self.parse_page(req)
            try:
                content = page['_embedded'][f'{self.basename}']
            except KeyError:
                logger.critical(page)
            self.write(counter, content)
            try:
                next_page = page['_links']['next']['href']
                return next_page
            except KeyError:
                return None
    
    
        def _all(self):
            counter = 1
            next_page = self.get_page(self.url, counter)
            while next_page is not None:
                counter += 1
                next_page = self.get_page(next_page, counter)


## Test

    from extract import Extract
    from amo.entities import Users
    from setup import amo20
    
    entity = Users('yastaff')
    extract = Extract(amo20, entity, truename="users")
    
    extract._all()

All&rsquo;s working!


# Transform

We&rsquo;ve managed to get our entities from AmoCRM with API calls, but Amo data format is Json.
We need tabular format though to send it to the data base.
We&rsquo;ll use some classes to define fields for each entity and write it to **csv**, one file per each entity.
We do that because some of the data is to big to keep it as one file, and reading such file will cause **stack overflow**.
There is another reason for that as well: custom fields for some entities. Those custom fields can be different for each entity, and we need to deal with that, in **Load** phase we&rsquo;ll be updating our schema if new field appears.


## Transform Base

Let&rsquo;s start with defining a base class for all the entities.
The base class will contain only `id_` field, becuase that&rsquo;s the only common field for *all entities*.
We will be passing `dict` to each entity from which it will read the fields.

    import csv
    import json
    from os import remove as rm
    from pathlib import Path
    from cyrtranslit import to_latin
    
    
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

Now we&rsquo;ll define classes for all the entities that we&rsquo;ll be using.
We should keep in mind that becuase in Amo some entities are sub-entities or sub-types for others.
That&rsquo;s why we use `basename` for extraction and `truename` for further processing.

So, the entities are:


## Transform Users

    class Tusers(Base):
        truename = "users"
    
    
        def __init__(self, dict_):
            self.id_ = dict_["id"]
            self.name = dict_["name"]
            self.email = dict_["email"]


## Transform Leads

Tricky part about leads, is that it has a list of **custom fields**.
We&rsquo;ll modify `to_dict` method of the base class to comprehend those fields.
Some of their names are in Cyrillic, so we need to translate it.
Some have multiple values so we join them separated by comma.

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
            for cf in content["custom_fields_values"]:
                field_name = ''.join(
                    c for c in to_latin(cf["field_name"], lang_code='ru')
                        if c.isalpha() or c.isdigit()
                )
                field_value = ",".join(str(v["value"]) for v in cf["values"])
                content[field_name] = field_value
            content.pop("custom_fields_values")
            return content


## Transform Statuses

As we remember Statuses are sub-type of the events, so we&rsquo;ll first define Events class, and inherit from it.

    class Tevents(Base):
        truename = "events"
    
        def __init__(self, dict_):
            self.type_ = dict_["type"]
            self.entity_id = dict_["entity_id"]
            self.entity_type = dict_["entity_type"]
            self.created_by = dict_["created_by"]
            self.created_at = dict_["created_at"]
            self.account_id = dict_["account_id"]
    
    
    class Tstatuses(Tevents):
        truename = "statuses"
    
        def __init__(self, dict_):
            self.value_after_status_id = dict_["value_after"][0]["lead_status"]["id"]
            self.value_after_pipeline_id = dict_["value_after"][0]["lead_status"]["pipeline_id"]
            self.value_before_status_id = dict_["value_before"][0]["lead_status"]["id"]
            self.value_before_pipeline_id = dict_["value_before"][0]["lead_status"]["pipeline_id"]


## Transform Calls

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


## Tranform Pipelines

Pipelines are a bit tricky as well.
Though there are no custom fields different for each entry, it contains statuses inside each pipeline.

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


## Transform all

Now when we have classes for all the entities, we want a clear procedure of transforming them all at once.
We have a directory with multiple Json files, each of them contains list of dictionaries.
We want to read them all, and, if succeeded, clear the directory.

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
            return True

And since statuses have different write methods, we should inherit the other class for them:

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


# Load

We&rsquo;re using Google BigQuery as Data Warehouse, so we need first to establish a connection with the help of credentials file.

First, we create base load class for all the entities:

    import os
    from datetime import date
    import pandas as pd
    from google.cloud import bigquery as bq
    from google.api_core.exceptions import BadRequest
    from loguru import logger
    
    
    class Load:
        def __init__(self, amo, entity):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
                './tokens/oddjob-db-2007-759fe782b144.json'
            self.client = bq.Client()
            self.path = f"temp_data/{amo}/{entity}_transformed/"
            self.date = str(date.today())
            self.files_num = sum(1 for file in os.listdir(self.path))
            self.file_list = [self.path + f for f in os.listdir(self.path)]
    
            self.table_ref = self.client.dataset(
                    f"{amo}_oddjob").table(f"dw_amocrm_{entity}")
    
            self.table_backup = self.client.dataset(
                f"{amo}_oddjob").table(f"dw_amocrm_{entity}_{self.date}")
            self.job_config = bq.LoadJobConfig(autodetect=True)
    
    
        def backup(self):
            job = self.client.copy_table(
                self.table_ref, self.table_backup
            )
            logger.success(job.result())
            self.client.delete_table(self.table_ref)
    
    
        def read(self, filepath):
            return pd.read_csv(filepath)
    
    
        def load(self, df):
            job = self.client.load_table_from_dataframe(
                df, self.table_ref, job_config=self.job_config
            )
            logger.success(job.result())
    
    
        def in_batches(self, batch_size=10000):
            for i in range(0, self.files_num, batch_size):
                batch_files = self.file_list[i:i+batch_size]
                print(f for f in batch_files)
                df_list = [self.read(f) for f in batch_files]
                df = pd.concat(df_list).astype('str')
                self.load(df)
    
        def cleanup(self):
            for f in self.file_list:
                os.remove(f)

For leads we need something more advanced, because there are custom fields which are different for each entity.
So, we&rsquo;ll need to update table schema in case there are some column that is not present.

    class LoadWithSchemaUpdate(Load):
        def __init__(self, amo, entity):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
                './tokens/oddjob-db-2007-759fe782b144.json'
            self.client = bq.Client()
            self.path = f"temp_data/{amo}/{entity}_transformed/"
            self.date = str(date.today())
            self.files_num = sum(1 for file in os.listdir(self.path))
            self.file_list = [self.path + f for f in os.listdir(self.path)]
    
            self.table_ref = self.client.dataset(
                    f"{amo}_oddjob").table(f"dw_amocrm_{entity}")
    
            self.table_backup = self.client.dataset(
                f"{amo}_oddjob").table(f"dw_amocrm_{entity}_{self.date}")
            self.job_config = bq.LoadJobConfig(autodetect=True)
    
    
        def get_schema_from_dataframe(self, df, old_schema):
    
            df[df.select_dtypes(include=['object']).columns] =\
                df.select_dtypes(include=['object']).astype('string')
            old_schema_names = [field.name for field in old_schema]
            schema = []
    
            for col_name, dtype in df.dtypes.items():
                if col_name not in old_schema_names:
                    schema.append(bq.SchemaField(col_name, dtype.name))
            return schema
    
    
        def update_schema(self, df):
            table = self.client.get_table(self.table_ref)
    
            old_schema = list(table.schema)
            new_schema = get_schema_from_dataframe(df, old_schema)
    
            combined_schema = old_schema + new_schema
    
            table.schema = combined_schema
            self.client.update_table(table, ["schema"])
    
    
        def in_batches(self, batch_size=10000):
            for i in range(0, self.files_num, batch_size):
                batch_files = self.file_list[i:i+batch_size]
                print(f for f in batch_files)
                df_list = [self.read(f) for f in batch_files]
                df = pd.concat(df_list).astype('str')
                try:
                    self.load(df)
    
                except BadRequest as e:
                    logger.info(e)
                    self.update_schema(self.table_ref, df)
                    self.load(df)
                return True


# ETL&rsquo;s themselves

Finally we have everything ready, so we can write our ETL&rsquo;s script which will combine all we defined in previous steps and make it work (hopefully).


## Leads ETL


### Yastaff

    import json
    from extract import Extract
    from amo.entities import Leads
    from setup import amo20
    from transform import Tleads, Transform
    from load import LoadWithSchemaUpdate
    
    AMO = 'yastaff'
    ENTITY = Tleads
    
    method = Leads('yastaff').created_at(from_="2022-01-01")
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


## Calls ETL


### Yastaff

    import json
    from extract import Extract
    from amo.entities import Calls
    from setup import amo20
    from transform import Tcalls, Transform
    from load import Load
    
    AMO = 'yastaff'
    ENTITY = Tcalls
    
    method = Calls('yastaff').created_at(from_="2021-01-01")
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


## Statuses ETL


### Yastaff

    import json
    from extract import Extract
    from amo.entities import Statuses
    from setup import amo20
    from transform import Tstatuses, Transform
    from load import Load
    
    AMO = 'yastaff'
    ENTITY = Tstatuses
    
    method = Statuses('yastaff')  # .created_at(from_=)
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


## Users ETL


### Yastaff

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


## Pipelines ETL


### Yastaff

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


# Shell to rule them all

Now, when we have defined ETL&rsquo;s for all the entities, it is time to automate the process.
Two most common tools for that are cron jobs and Airflow.
Airflow is good when you have the full orchestra of pipelines, but since we only have few processes, we&rsquo;ll use cron.
For that we&rsquo;ll create a simple script, which then we&rsquo;ll pass to crontab.


## Yastaff ETL

    #!/home/analytics/amo20/venv/bin/python

