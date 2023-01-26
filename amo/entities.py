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

class Leads(Base):
    truename = "leads"
    basename = "leads"
    entity = "leads"


    def __init__(self, subdomain):
        self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity + "?"


    def filter(self):
        pass

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

class Notes(Base):
    truename = "notes"
    basename = "notes"
    sub_entity = "/notes"

    def __init__(self, subdomain, entity):
        self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + entity\
            + self.sub_entity + "?"

class Calls(Notes):
    truename = "calls"
    sub_type = "?filter[note_type]=call_in"
    entity1 = "leads"
    entity2 = "companies"
    entity3 = "contacts"


    def __init__(self, subdomain):
        self.url1 = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity1\
            + self.sub_entity + self.sub_type

        self.url2 = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity2\
            + self.sub_entity + self.sub_type

        self.url3 = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity3\
            + self.sub_entity + self.sub_type

class Pipelines(Leads):
    truename = "pipelines"
    basename = "pipelines"
    sub_entity = "/pipelines"

    def __init__(self, subdomain):
        self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity\
            + self.sub_entity + "?"

class Users(Base):
    truename = "users"
    entity = "users"
    basename = "users"
    def __init__(self, subdomain):
        self.url = f"https://{subdomain}.amocrm.ru/api/v4/" + self.entity + "?"
