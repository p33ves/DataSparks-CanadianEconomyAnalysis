STEPS FOR PROJECT EXECUTION :
==============================

1. spark-submit 1.get_statcan_data.py ---> Data is downloaded at "../data/raw/statcan/..."
2. spark-submit 2.cleanse_statcan_data.py ---> Cleaning the data and storing at "../data/clean/statcan/...", Storing the schema at "../schema/statcan/.."

Below code files are stored in the "src" folder in GitHub and in "ProjectCode" in AWS and their output will be in "OUTPUT-Folder"

3. spark-submit 3.covid_cases.py
4. spark-submit 4.GDP+MT-analysis.py
5. spark-submit 5.RetailTradeSales+Yahoo.py
6. spark-submit 6.IMF+FDindex
7. spark-submit 7.CPI_analysis.py
8. spark-submit 8.GDP-HoursWorked.py
9. spark-submit 9.TSX+Business.py
10. spark-submit 10.RetailTrade-GDPvsSales.py
11. spark-submit 11.healthcare_analysis.py
12. spark-submit 12.householdconsumption.py
13. spark-submit 13.GDP_countries.py
14. spark-submit 14.GDP_Pred.py


STEPS FOR AWS CLUSTER SETUP :
==============================

AWS Student Edition - Educate Account : [Pipeline : EMR Spark Cluster --> S3 storage --> QuickSight Visualization]  
a) To setup AWS EMR-Spark Cluster session : Key Pair Created - "datasparks"  
Files for key pairs :  
-> "DataSparks.pem" --- for linux/mac  
-> "dataKey.ppk" --- converted by PuttyGen for Windows  
b) An S3 storage bucket called "mysparks" has been created which contains the input data folder, Project code folder and the output folder
c) To allow an EMR cluster node to access the AWS S3 objects (here, the input json schema files), the AWS boto3 client for python is used. The following command should be used after SSHing into the cluster node:
sudo pip-3.7 install boto3

where 3.7 is the python version in the node.

d) The files from the output folder in S3 bucket "mysparks" is loaded into a different S3 bucket called "datasparks-output". This new S3 bucket is linked with the AWS QuickSight Tool for data visualization and Dashboard creation.
e) The GDP prediction code in Jupyter Notebook of EMR cluster is automatically created as S3 object and pushed to "notebooks" folder in Git repo.