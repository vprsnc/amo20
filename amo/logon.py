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
        logger.success("logged on!")
        return self.access_token
