
CLI APP DOCUMENTATION

The application has two phases. The first phase is the storage of the data. This is achieved using mainly two classes, the Finnhub and FinnhubDB. The first class, the Finnhub uses the API to make calls and get the data, then it passes these data to FinnhubDB class which does the storing in DB.

The Finnhub downloads all the stocks present in a particular stock market, in my presentation I used NDX (Nasdaq 100) since it has sufficient data as requested. It first looks at the index constituents and then gets historical data for each of the stocks. My main issue here was the limit of the API of 30 API calls/ second, I managed solve it with time.sleep(60) as there is no way to overcome the issue other then waiting. Another choice that I had to make was the market to get FX data, I noticed that with OANDA exchange there was a time shift in the downloaded FX data, hence I opted for the forex.com. As requested, the only FX historical data that I downloaded where AUD/USD, EUR/USD, GBP/USD.

Once Finnhub gets the data it passes them to FinnhubDB to store them respectively in the following tables: anagraphic_table, stock_historical_data for the stock data and fxanagraphic_data, fxhistorical_data for the FX data. I used sqlite as it does not require additional libraries, its easy and fast to store data. Storing data in a DB gave me a lot of flexibility in executing the requested functionalities.

The second phase of the application is that of executing some functionalities. This is achieved using Functionality class, the class ha 5 defined methods that execute the 5 requested functionalities. Functionality 1 prints all the historical stock quotes by applying the exchange rate of the available FX historical data: AUD/USD, EUR/USD, GBP/USD (close price is applied) while functionality 2 allows to pick the stock and one of the available FX historical data and print them. Functionality nr 3 and 4 plot a graph using plotille as suggested. Functionality 5 prints for a stock only one interval of data not the whole historical data (since it was let to my discretion, I chose from='2020-02-09', to='2020-09-14').

The script was tested in my Windows Subsystem for Linux.
