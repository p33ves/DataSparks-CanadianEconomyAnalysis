# DataSparks-CanadianEconomyAnalysis
BIG DATA LAB PROJECT PROPOSAL

TITLE : Pre and Post Covid-19 analysis of Canadian Economy  
TEAM "DataSparks" : Raksha Harish, Vignesh Perumal, Anjali Thomas  

Description : 
In this project we aim to perform a comparative study of economic activity in Canada pre and post-covid19. We would be looking towards www150.statcan.gc.ca for our base datasets 
of 11 specific industries (and their provincial splits for those available) in Canada and studying their performance based on the historical data extending as far back as 20 years if required. We would also attempt to set an international context to the observations based on data taken from IMF datasets on the Canadian GDP and Global economic indicators. We plan to use AWS in this study for ETL and data processing. 

As a use-case from this study, we would further like to utilize the conclusions gathered to validate if the trends observed for the different industries correlate with the stock 
prices of major companies belonging to their respective industries in Canada during the time period. For this effort, we would be utilizing and integrating TSX data to our base
datasets from websites mentioned below.

Datasets :  
1. Statistics Canada Website  
a)Gross Domestic Product (GDP) at basic prices, by industry, monthly :   
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610043401  
b) Consumer Price Index (CPI), monthly, for different industries in the 13 provinces :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000401  
c) International Merchandise Trade by commodity, monthly :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1210012101  
d) Retail trade sales by province and territory :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010000801  
e) Manufacturers’ sales, inventories, and orders, by industry :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1610004701  
f) Actual hours worked at main job, by industry, monthly :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410028901  
g) Average weekly earnings by industry, annually :  
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410020401  
h) Detailed household final consumption expenditure, Canada, quarterly :   
https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610012401  

2. International Monetary  Fund (IMF) Website  
a) Canadian GDP and inflation rate data :  
https://www.imf.org/en/Countries/CAN  
b) Principal Global Economic Indicators, by sector/industry (selected for Canada) :  
https://www.principalglobalindicators.org/?sk=E30FAADE-77D0-4F8E-953C-C48DD9D14735  
c) IMF Macroeconomic and Financial Data (selected for Canada) :  
https://data.imf.org/?sk=388DFA60-1D26-4ADE-B505-A05A558D9A42&sId=1479331931186  

3. Toronto Stock Exchange (TSX) Data  
a) TSX Statistics - Government of Canada :  
https://open.canada.ca/data/en/dataset/0e1e57aa-e664-41b5-a69f-d814d4407d62  
b) Business leading indicators for Canada - Government of Canada  
https://open.canada.ca/data/en/dataset/a0ac9f2f-b993-4653-9798-8871d7b1db3e  
c) Yahoo finance TSX Data for stocks (2000 - 2020)  
https://finance.yahoo.com/quote/%5EGSPTSE/history?period1=948672000&period2=1603497600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true  

Technologies : Spark (RDD/Dataframes) with Amazon Web Services :  
-> AWS-EMR for ETL and processing  
-> AWS-S3 for storage  
-> AWS-QuickSight for data visualization and dashboard creation  

STEPS FOR BIG DATA PROJECT EXECUTION:
=====================================

1. python 1.get_statcan_data.py  ---> Data is downloaded at "../data/raw/statcan/..."  
2. spark-submit 2.cleanse_statcan_data.py  ---> Cleaning the data and storing at "../data/clean/statcan/...", Storing the schema at "../schema/statcan/.."  
 
Below code files are stored in the "src" folder in GitHub and in "ProjectCode" in AWS and their output will be in "OUTPUT-Folder"   

3. spark-submit 3.covid_cases.py  
4. spark-submit 4.GDP+MT-analysis.py  
5. spark-submit 5.RetailTradeSales+Yahoo.py  
6. spark-submit 6.IMF+FDindex---pending  
7. spark-submit 7.CPI-analysis.py  
8. spark-submit 8.GDP-HoursWorked.py  
9. spark-submit 9.TSX+Business.py  
10. spark-submit 10.RetailTrade-GDPvsSales.py  
11. spark-submit 11.healthcare_analysis.py  
12. spark-submit 12.householdconsumption.py  


STEPS FOR AWS CLUSTER SETUP :  
==============================  

1. AWS Educate Account : [Pipeline : EMR Spark Cluster --> S3 storage --> QuickSight Visualization]  
a) To setup AWS EMR-Spark Cluster session : Key Pair Created - "datasparks"  
   Files for key pairs :    
   -> "DataSparks.pem" --- for linux/mac  
   -> "dataKey.ppk" --- converted by PuttyGen for Windows  
b) An S3 storage bucket called "mysparks" has been created which contains the input data folder, Project code folder and the output folder  
c) The files from the output folder in S3 bucket "mysparks" is used for data visualization in AWS QuickSight Tool
