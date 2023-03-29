import psycopg2
import os

class ValidatorSIMAM:

    def __init__(self) -> None:
        self.__initVariable()

    def __initVariable(self):
        self.HOST = 'host'
        self.DATABASE = 'database'
        self.USER = 'user'
        self.PASSWORD = 'password'
        self.PORT = 'port'
        self.batchSQL = None
        self.conferenciaSQL = None
        self.dbConfigure = None
        self.queryParams = None
        self.queryParamsKeys = ['days', 'months', 'years', 'interval']
        self.TOTALS = {
            'batch':float(0),
            'arquivo':float(0),
            'integrado':float(0)
        }

    def __validateDBConfig(self, DBConfig) -> None:
        if DBConfig[self.HOST] is None or DBConfig[self.HOST] is None or DBConfig[self.HOST] is None or DBConfig[self.HOST] is None:
            raise "Por favor, verifique os campos da configuração"
        self.dbConfigure = DBConfig
            
    def __startDatabaseConnection(self, params) -> None:
        self.connection =  psycopg2.connect(
            host = params[self.HOST],
            database = params[self.DATABASE],
            user = params[self.USER],
            password = params[self.PASSWORD],
            port = params[self.PORT]
        )

    def connectDatabase(self, params) -> None:
        self.__validateDBConfig(params)
        try:
            self.__startDatabaseConnection(params)
        except:
            raise "Problema com a conexão com o banco de dados. Por favor, verifique!"    

    def __validateKey(self, params, key, message):
        try:
            if params[key] is None: raise 
        except:
            raise  message

    def __validateKeys(self, params):
        for key in self.queryParamsKeys: self.__validateKey(params, key, f"Falta paramatro '{key}' para pesquisa de dados")

    def __validateDelimiterInterval(self, caractere, params):
        for key in self.queryParamsKeys:
            if key == 'interval': continue
            if caractere in params[key]: raise f"Parametros '{key}' invalido. Verifique "

    def __validateIntervalIntervalCommand(self, params):
        self.__validateDelimiterInterval('/', params)
    
    def __validateSpecificIntervalCommand(self, params):
        self.__validateDelimiterInterval('-', params)

    def __validateStrictIntervalCommand(self, params):
        self.__validateDelimiterInterval('/', params)
        self.__validateDelimiterInterval('-', params)

    def __validateInterval(self, params):
        if params['interval'] == 'interval': self.__validateIntervalIntervalCommand(params)
        if params['interval'] == 'specific': self.__validateSpecificIntervalCommand(params)
        if params['interval'] == 'strict': self.__validateStrictIntervalCommand(params)

    def __validateQueryParams(self, params):
        self.__validateKeys(params)
        self.__validateInterval(params)

    def readQueryParams(self, params) -> None:
        self.__validateQueryParams(params)
        self.queryParams = params

    def __initCursor(self):
        self.__cursor = self.connection.cursor()

    def __executeQuery(self, sql):
        self.__cursor.execute(sql)
        self.connection.commit()
        return self.__cursor.fetchall()

    def __executeBatchQuery(self, date ):
        sql = open('sql-batch.sql','r').read()
        sql = sql.replace(':dataInicial', f"'{date}'")
        sql = sql.replace(':dataFinal', f"'{date}'")
        sql = sql.replace(':entidade', '1')
        return self.__executeQuery(sql)
    
    def __executeConferenciaQuery(self, date):
        sql = open('sql-conferencia.sql','r').read()
        sql = sql.replace(':dataInicial', f"'{date}'")
        sql = sql.replace(':dataFinal', f"'{date}'")
        sql = sql.replace(':entidade', '1')
        return self.__executeQuery(sql)

    def __adequateValue(self, value):
        return f'{0}{value}' if int(value) < 10 else value 

    def __getDelimeter(self):
        interval = self.queryParams['interval'] 
        if  interval == 'interval': return '-'
        elif interval == 'specific': return '/'
        else: return None
    
    def __getValues(self, value, delimeter):
        if delimeter is None or delimeter not in value: return [value]
        if delimeter == '/': return value.split(delimeter)
        elif delimeter == '-':
            values_list = []
            values = value.split(delimeter)
            for i in range(int(values[0]), int(values[1])+1): values_list.append(str(i))
            return values_list

    def __getYearsFromQueryParams(self):
        return self.__getValues(self.queryParams['years'],self.__getDelimeter()) 
    
    
    def __getMonthsFromQueryParams(self):
        return self.__getValues(self.queryParams['months'],self.__getDelimeter())
    
    def __getDaysFromQueryParams(self):
        return self.__getValues(self.queryParams['days'],self.__getDelimeter())
    
    def formatMoney(self, value):
        return "R$ {:,.2f}".format(value)\
            .replace(",","X")\
            .replace(".",",")\
            .replace("X",".")
    
    def __getFileName(self):
        return f"resultado-{self.dbConfigure['database']}.txt"
    
    def __updateTotals(self, batch, arquivo, integrado):
        self.TOTALS['batch'] = self.TOTALS['batch'] + float(batch)
        self.TOTALS['arquivo'] = self.TOTALS['arquivo'] + float(arquivo)
        self.TOTALS['integrado'] = self.TOTALS['integrado'] + float(integrado)


    def __registerResponse(self, date, batchResponse, conferenciaResponse):
        with open(self.__getFileName(), 'a') as file:
            batchValue = batchResponse[0][0]
            arquivoValue = conferenciaResponse[0][3]
            integradoValue = conferenciaResponse[0][4]
            isValuesOk = 'OK' if batchValue == arquivoValue and arquivoValue == integradoValue else 'PROBLEMAS'
            self.__updateTotals(batchValue, arquivoValue, integradoValue)
            file.write(f'Data = {date} ; BATCH = {self.formatMoney(batchValue)} ; ARQUIVO = {self.formatMoney(arquivoValue)} ; INTEGRADO = {self.formatMoney(integradoValue)} ; STATUS = {isValuesOk} \n')

    def __prepareFile(self):
        try:
            os.remove(self.__getFileName())
        except:
            print("Arquivo inexistente!")

    def __closeConnection(self):
        self.connection.close()

    def __registerTotals(self):
        with open(self.__getFileName(), 'a') as file:
            totalsLine = '\n \n TOTAIS: '
            for key in self.TOTALS: totalsLine += f"{key} = {self.formatMoney(self.TOTALS[key])} ;  "
            file.write(totalsLine)

    def execute(self):
        self.__initCursor()
        self.__prepareFile()
        for year in self.__getYearsFromQueryParams():
            for month in self.__getMonthsFromQueryParams():
                for day in self.__getDaysFromQueryParams():
                    date = f'{year}-{self.__adequateValue(month)}-{self.__adequateValue(day)}'
                    batchResponse = self.__executeBatchQuery(date)
                    conferenciaResponse = self.__executeConferenciaQuery(date)
                    self.__registerResponse(date, batchResponse, conferenciaResponse)
        self.__registerTotals()
        self.__closeConnection()