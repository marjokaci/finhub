import datetime
import requests
import json
import sqlite3
from sqlite3 import Error
import os
import time
import plotille


class FinnhubAPIException(Exception):
    def __init__(self, response):
        super(FinnhubAPIException, self).__init__()
        self.code = 0
        try:
            json_response = response.json()
        except ValueError:
            self.message = "JSON error message from Finnhub: {}".format(response.text)
        else:
            if "error" not in json_response:
                self.message = "Wrong json format from FinnhubAPI"
            else:
                self.message = json_response["error"]
        self.status_code = response.status_code
        self.response = response

    def __str__(self):
        return "FinnhubAPIException(status_code: {}): {}".format(self.status_code, self.message)


class FinnhubRequestException(Exception):
    def __init__(self, message):
        super(FinnhubRequestException, self).__init__()
        self.message = message

    def __str__(self):
        return "FinnhubRequestException: {}".format(self.message)


class Finnhub:
    def __init__(self, api_key):
        self._session = self._init_session(api_key)
        self.API_URL = "https://finnhub.io/api/v1"
        self.DEFAULT_TIMEOUT = 60

    @staticmethod
    def _init_session(api_key):
        session = requests.session()
        session.headers.update({"Accept": "application/json",
                                "User-Agent": "finnhub/python"})
        session.params["token"] = api_key
        return session

    def stock_candles(self, symbol, resolution, _from, to, **kwargs):
        params = self._merge_two_dicts({
            "symbol": symbol,
            "resolution": resolution,
            "from": _from,
            "to": to
        }, kwargs)
        return self._get("/stock/candle", params=params)

    @staticmethod
    def _merge_two_dicts(first, second):
        result = first.copy()
        result.update(second)
        return result

    def _get(self, path, **kwargs):
        return self._request("get", path, **kwargs)

    def _request(self, method, path, **kwargs):
        uri = "{}/{}".format(self.API_URL, path)
        kwargs["timeout"] = kwargs.get("timeout", self.DEFAULT_TIMEOUT)
        kwargs["params"] = self._format_params(kwargs.get("params", {}))

        response = getattr(self._session, method)(uri, **kwargs)
        return self._handle_response(response)

    @staticmethod
    def _format_params(params):
        return {k: json.dumps(v) if isinstance(v, bool) else v for k, v in params.items()}

    @staticmethod
    def _handle_response(response):
        if not response.ok:
            raise FinnhubAPIException(response)
        try:
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                return response.json()
            if 'text/csv' in content_type:
                return response.text
            if 'text/plain' in content_type:
                return response.text
            raise FinnhubRequestException("Invalid Response: {}".format(response.text))
        except ValueError:
            raise FinnhubRequestException("Invalid Response: {}".format(response.text))

    def index_constituents(self, symbol, **kwargs):
        params = self._merge_two_dicts({
            "symbol": symbol,
        }, kwargs)
        return self._get("/index/constituents", params=params)

    def stock_profile(self, symbol, **kwargs):
        params = self._merge_two_dicts({
            "symbol": symbol,
        }, kwargs)
        return self._get("/stock/profile2", params=params)

    def forex_exchanges(self, **kwargs):
        params = self._merge_two_dicts({
            "symbol": 'symbol',
        }, kwargs)
        return self._get("/forex/exchange", params=params)

    def fx_symbols(self, exchange, **kwargs):
        params = self._merge_two_dicts({
            "exchange": exchange,
        }, kwargs)
        return self._get("/forex/symbol", params=params)

    def fx_candles(self, symbol, resolution, _from, to, **kwargs):
        params = self._merge_two_dicts({
            "symbol": symbol,
            "resolution": resolution,
            "from": _from,
            "to": to
        }, kwargs)
        return self._get("/forex/candle", params=params)

    def store_data(self, database, stockmarket, fxcurr, from_date, to_date):
        start_time = time.time()
        from_date_ux = int(time.mktime(datetime.datetime.strptime(from_date, "%d/%m/%Y").timetuple()))
        to_date_ux = int(time.mktime(datetime.datetime.strptime(to_date, "%d/%m/%Y").timetuple()))

        # Storing stock anagraphic data in db
        indx_const = self.index_constituents(f'^{stockmarket}')
        stockprofile = []
        for i in indx_const['constituents']:
            try:
                sp = self.stock_profile(i)
                if sp == {}:
                    pass
                else:
                    stockprofile.append((sp['country'], sp['currency'], sp['exchange'], sp['finnhubIndustry'],
                                         sp['ipo'], sp['logo'], sp['marketCapitalization'], sp['name'], sp['phone'],
                                         sp['shareOutstanding'], sp['ticker'], sp['weburl']))
            except:
                # API limit reached
                time.sleep(60)
                print('api limit ')
                sp = self.stock_profile(i)
                if sp == {}:
                    pass
                else:
                    stockprofile.append((sp['country'], sp['currency'], sp['exchange'], sp['finnhubIndustry'],
                                         sp['ipo'], sp['logo'], sp['marketCapitalization'], sp['name'], sp['phone'],
                                         sp['shareOutstanding'], sp['ticker'], sp['weburl']))
        database.insert_stock_anagraphic(stockprofile)

        # # Storing stock historical data in db
        prices = []
        for i in indx_const['constituents']:
            try:
                stock = self.stock_candles(i, 'D', int(time.mktime(datetime.datetime.strptime(from_date, "%d/%m/%Y").timetuple()))
                                           , int(time.mktime(datetime.datetime.strptime(to_date, "%d/%m/%Y").timetuple())))

            except:
                print('api limit ')
                time.sleep(60)
                stock = self.stock_candles(i, 'D', int(time.mktime(datetime.datetime.strptime(from_date, "%d/%m/%Y").timetuple()))
                                           , int(time.mktime(datetime.datetime.strptime(to_date, "%d/%m/%Y").timetuple())))

            for k in range(0, len(stock['c'])):
                prices.append((i, stock['c'][k], stock['h'][k], stock['l'][k], stock['o'][k],
                               time.strftime("%Y-%m-%d", time.localtime(int(stock['t'][k]))), stock['v'][k]))

        database.insert_stock_prices(prices)

        # # Storing fx anagraphic data in db
        exchanges = self.forex_exchanges()
        fxanagraphic_data = []
        for exch in exchanges:
            currencies = self.fx_symbols(exch)
            for fx in currencies:
                fxanagraphic_data.append((fx['description'], fx['displaySymbol'], fx['symbol']))
        database.insert_fxanagraphic_data(fxanagraphic_data)

        # Storing fx historical data in db
        fxprices = []
        for f in fxcurr:
            for j in fxanagraphic_data:
                if j[1] == f and 'FOREX' in j[2]:
                    historical_fx_data = self.fx_candles(symbol=f'{j[2]}', resolution='D',
                                                         _from=from_date_ux, to=to_date_ux)
                    for k in range(0, len(historical_fx_data['c'])):
                        fxprices.append((f, historical_fx_data['c'][k], historical_fx_data['h'][k],
                                         historical_fx_data['l'][k], historical_fx_data['o'][k],
                                         time.strftime("%Y-%m-%d", time.localtime(int(historical_fx_data['t'][k])))))

        database.insert_historical_data(fxprices)

        print("Storing execution time: %s seconds ---" % (time.time() - start_time))


class FinnhubDB:
    def __init__(self, dbname):
        self.create_connection(r"{}".format(os.getcwd() + f'\\{dbname}'))
        self.con = sqlite3.connect(dbname)
        self.cursor = self.con.cursor()
        self.create_tables()

    def __del__(self):
        self.cursor.close()

    @staticmethod
    def create_connection(db_file):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            # print('sqlite3 version', sqlite3.version)
        except Error as e:
            print(e)
        finally:
            if conn:
                conn.close()

    def create_tables(self):
        create_anagraphic_table = """CREATE TABLE IF NOT EXISTS anagraphic_table (
                                    id INTEGER PRIMARY KEY,
                                    country TEXT,
                                    currency TEXT,
                                    exchange TEXT,
                                    finnhubIndustry TEXT,
                                    ipo DATETIME default (DATETIME(CURRENT_DATE)),
                                    logo TEXT,
                                    marketCapitalization REAL,
                                    name TEXT,
                                    phone TEXT,
                                    shareOutstanding REAL,
                                    ticker TEXT,
                                    weburl TEXT
                                );"""

        create_historical_data = """CREATE TABLE IF NOT EXISTS stock_historical_data (
                                    id INTEGER PRIMARY KEY,
                                    ticker TEXT,
                                    Close REAL,
                                    High REAL,
                                    Low REAL,
                                    Open REAL,
                                    time DATETIME default (DATETIME(CURRENT_DATE)),
                                    Volum REAL
                                );"""

        create_fxanagraphic_data = """CREATE TABLE IF NOT EXISTS fxanagraphic_data (
                                    id INTEGER PRIMARY KEY,
                                    description TEXT,
                                    displaySymbol TEXT,
                                    symbol TEXT
                                );"""

        create_fxhistorical_data = """CREATE TABLE IF NOT EXISTS fxhistorical_data (
                                    id INTEGER PRIMARY KEY,
                                    fx TEXT,
                                    Close REAL,
                                    High REAL,
                                    Low REAL,
                                    Open REAL,
                                    time DATETIME default (DATETIME(CURRENT_DATE))
                                );"""

        self.cursor.execute(create_anagraphic_table)
        self.cursor.execute(create_historical_data)
        self.cursor.execute(create_fxanagraphic_data)
        self.cursor.execute(create_fxhistorical_data)
        # Clean old data
        self.cursor.execute("""delete from anagraphic_table""")
        self.cursor.execute("""delete from stock_historical_data""")
        self.cursor.execute("""delete from fxanagraphic_data""")
        self.cursor.execute("""delete from fxhistorical_data""")
        self.con.commit()

    def insert_stock_anagraphic(self, stockprofile):
        table_name = 'anagraphic_table'
        attrib_names = "country, currency, exchange, finnhubIndustry, ipo, logo, marketCapitalization," \
                       " name, phone, shareOutstanding, ticker, weburl"
        attrib_values = '?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?'
        sql = f"INSERT INTO {table_name} ({attrib_names}) VALUES ({attrib_values})"
        self.cursor.executemany(sql, stockprofile)
        self.con.commit()

    def insert_stock_prices(self, prices):
        table_name = 'stock_historical_data'
        attrib_names = "ticker, Close, High, Low, Open, time, Volum"
        attrib_values = '?, ?, ?, ?, ?, ?, ?'
        sql = f"INSERT INTO {table_name} ({attrib_names}) VALUES ({attrib_values})"
        self.cursor.executemany(sql, prices)
        self.con.commit()

    def insert_fxanagraphic_data(self, fxdata):
        table_name = 'fxanagraphic_data'
        attrib_names = "description, displaySymbol, symbol"
        attrib_values = '?, ?, ?'
        sql = f"INSERT INTO {table_name} ({attrib_names}) VALUES ({attrib_values})"
        self.cursor.executemany(sql, fxdata)
        self.con.commit()

    def insert_historical_data(self, fxhistorical_data):
        table_name = 'fxhistorical_data'
        attrib_names = "fx, Close, High, Low, Open, time"
        attrib_values = '?, ?, ?, ?, ?, ?'
        sql = f"INSERT INTO {table_name} ({attrib_names}) VALUES ({attrib_values})"
        self.cursor.executemany(sql, fxhistorical_data)
        self.con.commit()


def plot(inst, x, y):
    fig = plotille.Figure()
    fig.width = 90
    fig.height = 30
    fig.color_mode = 'byte'
    fig.plot(x, y, lc=200, label=f'Close prices of {inst}')
    print(fig.show(legend=True))


class Functionality:
    def __init__(self, db_file):
        self.conn = self.create_connection(db_file)
        self.present_stock = {'ticker': [],
                              'name': []}
        cur = self.conn.cursor()
        cur.execute(""" 
            SELECT DISTINCT stock_historical_data.ticker, anagraphic_table.name FROM stock_historical_data
            LEFT JOIN anagraphic_table
            on stock_historical_data.ticker = anagraphic_table.ticker  
        """)
        rows = cur.fetchall()
        for row in rows:
            self.present_stock['ticker'].append(row[0])
            self.present_stock['name'].append(row[1])

    @staticmethod
    def create_connection(db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)
        return conn

    def funct1(self, ticker, fx_toprint):
        if ticker == '':
            print('\nThis is functionality nr: 1.\n '
                  '\tConverts and prints historical stock quotes in any foreign currency.')
        input('Press enter to execute: ')
        cur = self.conn.cursor()
        for i in fx_toprint:
            i = i[0:3]
            print(f"\n\tPrinting historical prices in {i}...")
            time.sleep(2)
            cur.execute(f"""
                SELECT stock.id as id, 
                stock.ticker as ticker,
                stock.Close*forex.Close as Close_{i},
                stock.High*forex.Close as High_{i},
                stock.Low*forex.Close as Low_{i},	
                stock.Open*forex.Close as Open_{i},
                stock.time as time,
                stock.Volum as Volum
                FROM stock_historical_data as stock
                LEFT JOIN fxhistorical_data as forex
                on stock.time = forex.time
                WHERE forex.fx = '{i}/USD'
                {ticker}
                ORDER BY stock.id
            """)
            rows = cur.fetchall()
            for row in rows:
                print(f'Stock {row[1]}: Close in {i}: {row[2]};'
                      f' High in {i}: {row[3]}; Low in {i}: {row[4]}; Open in {i}: {row[5]}; Time: {row[6]}', end='\n')

    def funct2(self):
        print('\nThis is functionality nr: 2.\n '
              '\tPrints latest stock quotes for a given asset with the option to apply a currency exchange')
        cur = self.conn.cursor()
        present_fx = []
        cur.execute("""SELECT DISTINCT fx FROM fxhistorical_data""")
        rows = cur.fetchall()
        for row in rows:
            present_fx.append(row[0])

        stock = self._stock_selection(self.present_stock)
        curr = self._curr_selection(present_fx)
        self.funct1(ticker=f"and stock.ticker='{stock}'", fx_toprint=[curr])

    def _stock_selection(self, stk_present):
        stk = input("\nInsert a stock ticker (es:  TSLA, press enter to see options): ")
        if stk not in stk_present['ticker']:
            print(f'Ticker "{stk}" quotes are not present.\n Please insert one of the following: \n')
            for i in range(0, len(stk_present['ticker'])):
                print('ticker:', stk_present['ticker'][i], 'for:', stk_present['name'][i], end=",")
            return self._stock_selection(stk_present)
        return stk

    def _curr_selection(self, pres_fx):
        curr = input('Insert a currency(es: AUD/USD, press enter to see other options.): ')
        if curr in pres_fx:
            return curr
        else:
            print(f'Only the following currencies are present: {pres_fx}')
            return self._curr_selection(pres_fx)

    def funct3(self):
        print('\nThis is functionality nr: 3.\n '
              '\tDraw a graph based on whole historical data for an exchange from/to currency inputs')
        curr = self._curr_selection(['AUD/USD', 'EUR/USD', 'GBP/USD'])
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT * FROM fxhistorical_data
            WHERE fx = '{curr}'
        """)
        rows = cur.fetchall()
        x = []
        y_close = []

        for row in rows:
            x.append(datetime.date(*(int(s) for s in row[6].split('-'))))
            y_close.append(row[2])

        plot(curr, x, y_close)

    def funct4(self, **args):
        if args == {}:
            date_q = ''
            print('\nThis is functionality nr: 4.\n '
                  '\tDraw a graph based on whole historical data for a stock (close prices)')
        else:
            date_q = args['date']
        stock = self._stock_selection(self.present_stock)
        cur = self.conn.cursor()
        cur.execute(f""" SELECT * FROM stock_historical_data where ticker = '{stock}' {date_q}""")
        rows = cur.fetchall()
        x = []
        y_close = []
        for row in rows:
            x.append(datetime.date(*(int(s) for s in row[6].split('-'))))
            y_close.append(row[2])

        plot(stock, x, y_close)

    def funct5(self, _from, to):
        print('\nThis is functionality nr: 5.\n '
              '\tDraw a graph based on time inverval and a stock (close prices)')
        self.funct4(date=f"AND time BETWEEN '{_from}' AND '{to}'")
        print(f'Prices from {_from} to {to}')


if __name__ == "__main__":
    mytest = Finnhub(api_key="c0d9tiv48v6vf7f7iorg")
    db = FinnhubDB('finnhub.db')
    print('Storing data in sqlite database...\n(it will taker around 4min due to API limited nr of calls per min.)')
    # Currently support stock market GSPC (S&P 500), NDX (Nasdaq 100), DJI (Dow Jones)
    mytest.store_data(db, stockmarket='NDX', from_date="10/01/2020", to_date="10/01/2021",
                      fxcurr=['AUD/USD', 'EUR/USD', 'GBP/USD'])
    print('All necessary data have been stored in DB.')

    input("Executing functionalities, press Enter to continue...")

    fct = Functionality('finnhub.db')
    fct.funct1(ticker='', fx_toprint=['AUD', 'EUR', 'GBP'])
    fct.funct2()
    fct.funct3()
    fct.funct4()
    fct.funct5(_from='2020-02-09', to='2020-09-14')
