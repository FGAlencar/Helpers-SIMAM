from ValidatorSIMAM import ValidatorSIMAM

class App:
    def __init__(self) -> None:
        self.dbConfigure = {
            'host':'host',
            'port':'port',
            'database':'database',
            'user':'user',
            'password':'passwd'
        }

        self.queryParams = {
            'days': '1-31',
            'months': '1',
            'years': '2023',
            'interval':'interval'
        }

        self.application = ValidatorSIMAM()

    def start(self):
        self.application.connectDatabase(self.dbConfigure)
        self.application.readQueryParams(self.queryParams)
        self.application.execute()
        exit()



if __name__ == "__main__":
    App().start()
