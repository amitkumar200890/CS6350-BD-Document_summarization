##################################  Project Topic: Document summarization using NLP techniques #################################################

#                        TEAM MEMBERS

#  1. Amit Kumar          (Net-Id: axk210047)
#  2. Kirthi Menon        (Net-Id: kxm190036)
#  3. Manpreet Sandhu     (Net-Id: mxs200009)
#  4. Neha Ann John       (Net-Id: naj210000)


Steps to install/ Run the project:

For this project to run/execute PySpark enabled databricks community version account login is needed. 
Steps to execute:
1. Downlaod the attached IPynb notebook file: [CS6350-Project]_Text_Summarization.ipynb
2. Login to databricks cloud communinity version account.
3. Create a cluster on databricks with Databricks Runtime Version selected as 10.4 LTS (includes Apache Spark 3.2.1, Scala 2.12)
4. Once cluster is created click on Workspace -> select Users -> Click on Import
5. It will give an option to Import the source code: a. File b. URL
6. Select File (the radio button) and upload the ipynb file downloaded in step 1.
7. Click on Import button
8. Source code will be opened in databricks notebook.
9. Attach it to a cluster created in step: 3
10. Execute each shell one at a time.

During the execution step, the output will be dispalyed in the notebook and also an output file of summary report will be uploaded to S3.
Data set Link: https://drive.google.com/file/d/1xGcaWDiCkoWI_Iu8IK41CdRkHjw-Ku1a/view
Note: *It is also uploaded to S3 account. The program will directly access it from S3.

Also the link to the IPynb file is: 
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3188649968092718/2647635177611498/8267238645877833/latest.html