# --- datasource.py
import operator
import sys
import logging
import time
import requests


class DataSource():
    def __init__(self, **kwargs):
        pass

    def query(self, **kwargs):
        pass


# --- dummy_api.py


class DummyAPI(DataSource):
    def __init__(self, **kwargs):
        super(self.__class__, self, **kwargs).__init__()
        self.name = "dummy-api"
        self.props = dict(
            target="http://dummy.restapiexample.com/api/v1/employees")

    def query(self, **kwargs):
        column = kwargs["column"]
        res = requests.get(self.props["target"])
        # TODO: handle if not res.ok
        data = res.json()['data']
        return int(max([x[column] for x in data]))


# --- notification.py
class Notification():
    def __init__(self, **kwargs):
        pass

    def send(self, message, **kwargs):
        pass

# --- log_notification.py


class LogNotification(Notification):
    def __init__(self, **kwargs):
        super(self.__class__, self, **kwargs).__init__()

    def send(self, message, **kwargs):
        logging.critical(message)


# --- user_config.py
class UserConfig():
    def __init__(self):
        self.datasource = DummyAPI()
        self.column = "employee_age"
        self.threshold = 65
        self.comparison = ">="
        self.destination = LogNotification()


# --- alert_manager.py
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
COMPARISONS = {'>': operator.gt,
               '<': operator.lt,
               '>=': operator.ge,
               '<=': operator.le,
               '=': operator.eq}


class AlertManager():
    def __init__(self):
        self.config = UserConfig()

    def check(self):
        logging.info("checking...")
        result = self.config.datasource.query(column=self.config.column)

        # TODO store result in DB/flat-file here for history

        if COMPARISONS[self.config.comparison](result, self.config.threshold):
            self.config.destination.send(f"threshold reached! {result}")


if __name__ == "__main__":
    am = AlertManager()
    logging.info("alert manager started")
    while True:
        am.check()
        logging.info("performing next check in 5 minutes.")
        time.sleep(60*5)
