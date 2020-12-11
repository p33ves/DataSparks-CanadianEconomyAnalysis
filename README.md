# DataSparks - Pre and Post Covid-19 Canadian Economy Analysis

## BIG DATA LAB PROJECT PROPOSAL

### TITLE : Pre and Post Covid-19 analysis of Canadian Economy  
### TEAM - DataSparks : Raksha Harish, Vignesh Perumal, Anjali Thomas  

### Description : 
In this project we aim to perform a comparative study of economic activity in Canada pre and post-covid19. We would be looking towards www150.statcan.gc.ca for our base datasets 
of 11 specific industries (and their provincial splits for those available) in Canada and studying their performance based on the historical data extending as far back as 20 years if required. We would also attempt to set an international context to the observations based on data taken from IMF datasets on the Canadian GDP and Global economic indicators. We plan to use AWS in this study for ETL and data processing. 

As a use-case from this study, we would further like to utilize the conclusions gathered to validate if the trends observed for the different industries correlate with the stock 
prices of major companies belonging to their respective industries in Canada during the time period. For this effort, we would be utilizing and integrating TSX data to our base
datasets from websites mentioned below.

### Datasets :  
#### 1. Statistics Canada Website  
a)Gross Domestic Product (GDP) at basic prices, by industry, monthly :   
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610043401  
b) Consumer Price Index (CPI), monthly, for different industries in the 13 provinces :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000401  
c) International Merchandise Trade by commodity, monthly :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1210012101  
d) Retail trade sales by province and territory :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010000801  
e) Detailed information on confirmed cases of COVID-19, Public Health Agency of Canada  
https://open.canada.ca/data/en/dataset/287bf238-cc87-41e7-a5e2-78bc5d5ae97a  
f) Actual hours worked at main job, by industry, monthly :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410028901    
g) TSX Statistics  
https://open.canada.ca/data/en/dataset/0e1e57aa-e664-41b5-a69f-d814d4407d62  
h) Detailed household final consumption expenditure, Canada, quarterly :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610012401  

#### 2. International Monetary  Fund (IMF) Website  
a) Canadian GDP and inflation rate data :  
https://www.imf.org/en/Countries/CAN  
b) Principal Global Economic Indicators, by sector/industry (selected for Canada) :  
https://www.principalglobalindicators.org/?sk=E30FAADE-77D0-4F8E-953C-C48DD9D14735  
c) IMF Macroeconomic and Financial Data (selected for Canada) :  
https://data.imf.org/?sk=388DFA60-1D26-4ADE-B505-A05A558D9A42&sId=1479331931186  

#### 3. Toronto Stock Exchange (TSX) Data  
a) TSX Statistics - Government of Canada :  
https://open.canada.ca/data/en/dataset/0e1e57aa-e664-41b5-a69f-d814d4407d62  
b) Business leading indicators for Canada - Government of Canada  
https://open.canada.ca/data/en/dataset/a0ac9f2f-b993-4653-9798-8871d7b1db3e  
c) Yahoo finance TSX Data for stocks (2000 - 2020)  
https://finance.yahoo.com/quote/%5EGSPTSE/history?period1=948672000&period2=1603497600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true  

#### 4. Economic Data, Federal Reserve Bank of St. Louis
GDP data of 10 major countries in the world:  
a) Canada  
https://fred.stlouisfed.org/series/MKTGDPCAA646NWDB  
b) US  
https://fred.stlouisfed.org/series/FYGDP  
c) India  
https://fred.stlouisfed.org/series/MKTGDPINA646NWDB  
d) France  
https://fred.stlouisfed.org/series/MKTGDPFRA646NWDB  
e) Japan  
https://fred.stlouisfed.org/series/MKTGDPJPA646NWDB  
f) Germany  
https://fred.stlouisfed.org/series/MKTGDPDEA646NWDB  
g) UK  
https://fred.stlouisfed.org/series/MKTGDPGBA646NWDB  
h) Brazil  
https://fred.stlouisfed.org/series/MKTGDPBRA646NWDB  
i) Italy  
https://fred.stlouisfed.org/series/MKTGDPITA646NWDB  
j) China  
https://fred.stlouisfed.org/series/MKTGDPCNA646NWDB  

### Technologies : Spark (RDD/Dataframes) with Amazon Web Services :    
-> AWS-EMR for ETL and processing  
-> AWS-S3 for storage  
-> AWS-QuickSight for data visualization and dashboard creation  
-> Pandas and Scikit-learn libraries for GDP prediction  
