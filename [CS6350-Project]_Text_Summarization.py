# Databricks notebook source
#                        TEAM MEMBERS

#  1. Amit Kumar          (Net-Id: axk210047)
#  2. Kirthi Menon        (Net-Id: kxm190036)
#  3. Manpreet Sandhu     (Net-Id: mxs200009)
#  4. Neha Ann John       (Net-Id: naj210000)

# COMMAND ----------

pip install nltk

# COMMAND ----------

import boto3
import pandas as pd
from io import StringIO

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords 
import string as st
import re
import math
from nltk.tokenize import sent_tokenize
import nltk
nltk.download('punkt')

# COMMAND ----------

def clean_str(strData):
    '''
        This function will remove the punctuation and stop words from the input string
    '''
    stpWrds = set(stopwords.words('english'))
    strData = ''.join([wrd for wrd in strData if wrd not in st.punctuation])
    strData = strData.lower().split(' ')
    strData = ' '.join([wrd for wrd in strData if wrd not in stpWrds])

    return strData
    

def convert_to_vector_form(str1, str2):
    '''
        This function will convert string into vector form and set value if overlapping words found
    '''
    
    str1 = [wrd.lower() for wrd in str1.replace("[^a-zA-Z0-9]"," ").split(" ")]
    str2 = [wrd.lower() for wrd in str2.replace("[^a-zA-Z0-9]"," ").split(" ")]

    unqWrds = list(set(str1 + str2))
    cmnWrds = list(set(set(str1) & set(str2)))

    frstVec  = [0] * len(unqWrds)
    secVec = [0] * len(unqWrds)

    #str1 vctr
    for wrd in str1:
        frstVec[unqWrds.index(wrd)] += 1

    #str2 vctr
    for wrd in str2:
        secVec[unqWrds.index(wrd)] += 1

    return cmnWrds, frstVec, secVec    
    
    
def func_text_similarity_val(text1, text2):
    '''
        this function will calculate the similarity of two string based on the formula:
        similarity = sum(common words)/(log(sentence1 length) + log(sentence2 length))
    '''
    text1 = clean_str(text1)
    text2 = clean_str(text2)
    
    if text1.__eq__(text2):
        return 0.0
    cmnWrds, frstVec, secVec = convert_to_vector_form(text1, text2)

    num_val = len(cmnWrds)

    denum_val1 = sum([valX != 0 for valX in list(frstVec)])
    denum_val2 = sum([valX != 0 for valX in list(secVec)])

    denum_val = math.log(denum_val1) + math.log(denum_val2)
    if not denum_val:
        return 0.0
    else:
        return float(num_val) / denum_val

# COMMAND ----------

class cs6350_text_summarization:
    '''
        TextRank extractive summary approach is implemented to find the summary of the document.
        In this project, program will loop through the list of documents and find out the summary.
        Result will be dispalyed on console or uploaded to S3 based on output_format flag in output_format function
    '''
    
    def __init__(self, awsAccessKeyID = "AKIA2VKC2K3DSWIUF55G", 
                 awsSecretAccessKey = "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", 
                 bucketName = "axk210047bucket", 
                 inptFile = "datafile/tennis_articles.csv",
                 outptFile = "datafile/[CS6350-Project]_Text_Summarization_output.txt",
                 summary_percent = 0.20
                ):
        
        self.awsAccessKeyID = awsAccessKeyID
        self.awsSecretAccessKey = awsSecretAccessKey
        self.bucketName = bucketName
        self.inptFile = inptFile
        self.outptFile = outptFile
        self.summary_percent = summary_percent
    
    
    def read_file_data(self):
        '''
            This function will be used to read the file content form s3 and data will be loaded and returned to caller in spark dataframe format
        '''
        s3AWS = boto3.client("s3", 
                          aws_access_key_id = self.awsAccessKeyID, 
                          aws_secret_access_key = self.awsSecretAccessKey)
        
        aws_s3_obj = s3AWS.get_object(Bucket = self.bucketName, Key = self.inptFile)
        fileData = aws_s3_obj["Body"].read().decode('ISO-8859-1')
        input_document = pd.read_csv(StringIO(fileData), encoding = 'unicode_escape', delimiter = ',', header=[0])
        input_doc_sdf = spark.createDataFrame(input_document)
        
        return input_doc_sdf
    
    
    def convert_doc_to_sent_list(self, doc):
        sent_list =[]
        sent_list = sent_tokenize(doc)
        for sent in sent_list:
            sent.replace("[^a-zA-Z0-9]"," ")

        return sent_list
    
    
    def func_document_summary_finder(self, input_docs_df):
        '''
            This function is used to calculste the sentence similarity and find the summary n=based on highest rank sentences. It will return the complete information about the summary of each article
        '''
        input_docs = input_docs_df.rdd.map(lambda x: (x[0], x[2], x[1])).collect()
        document_gist = []
        plot_data = []
        tablular_report_data = []
        
        for input_doc in input_docs:
            document_id = input_doc[0]
            document_title = input_doc[2]
            sent_list = self.convert_doc_to_sent_list(input_doc[1])
            doc_contnt = sc.parallelize(sent_list)
            
            summary_sent_count = int(math.ceil(len(sent_list)*self.summary_percent))
            
            final_result = []
            summary_string = ""
            
            for input_sent in doc_contnt.collect():
                try:
                    connectivity = sum([x for x in doc_contnt.map(lambda x: func_text_similarity_val(x, input_sent)).collect()]) 
                except:
                    connectivity = sum([x for x in doc_contnt.map(lambda x: func_text_similarity_val(x, input_sent)).collect()]) 
            
                final_result.append((input_sent, connectivity))
                
            summary_string = ' '.join(sc.parallelize(final_result).sortBy(lambda xVal: -xVal[1]).map(lambda xVal: xVal[0]).take(summary_sent_count))
            document_gist.append((document_id.__str__(), document_title, summary_string))
            
            tablular_report_data.append((document_title, len(sent_list), summary_sent_count))
            plot_data.append((document_title, final_result, summary_string))
          
        return document_gist, plot_data, tablular_report_data
    
    
    def output_format(self, report_data, output_mode):
        '''
            The purpose of this function is to control output based on input parameter output_mode as follows:
                1: Console, 2: File, 3: Console and File
        '''
        
        file_input = "article_id|article_title|article_summary\n"
        for data in report_data:
            row_data = ""
            if output_mode == 1 or output_mode == 3:
                print("article_id: ", data[0])
                print("article_title: ", data[1].encode("ascii", "replace").decode("utf-8"))
                print("article_summary: ", data[2].encode("ascii", "replace").decode("utf-8"))
                print("\n")

            if output_mode == 2 or output_mode == 3:
                row_data = '|'.join(data) + "\n"
            
            file_input = file_input + row_data.encode("ascii", "replace").decode()
            
        if output_mode == 2 or output_mode == 3:
            sessionS3 = boto3.Session(aws_access_key_id = self.awsAccessKeyID,
                                      aws_secret_access_key = self.awsSecretAccessKey)

            s3_session = sessionS3.resource('s3')
            object = s3_session.Object(self.bucketName, self.outptFile)

            s = object.put(Body=file_input)
            print("File {} is uploaded to S3 bucket".format(self.outptFile))


# COMMAND ----------

# All input parameter is define here
# based on provide inpu parameter, class object will be created to further use
awsAccessKeyID = 'AKIA2VKC2K3DSWIUF55G'
awsSecretAccessKey = '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'
bucketName = 'axk210047bucket'
inptFile = 'datafile/tennis_articles.csv'
outptFile = 'datafile/[CS6350-Project]_Text_Summarization_output.txt'
summary_percent = 0.20 # ceil(no. of sentence count * summary_percent) will be summary sentences

text_summarization_obj = cs6350_text_summarization(awsAccessKeyID, awsSecretAccessKey, bucketName, inptFile, outptFile, summary_percent)

# COMMAND ----------

# input file data
input_doc_df = text_summarization_obj.read_file_data()

# COMMAND ----------

# input file content
display(input_doc_df)

# COMMAND ----------

# funtion call to find the summary of documet
document_gist, plot_data, tablular_report_data = text_summarization_obj.func_document_summary_finder(input_doc_df)

# COMMAND ----------

# summary report
summary_report = pd.DataFrame(document_gist, columns = ["article_id", "article_title", "article_summary"])
display(summary_report)

# COMMAND ----------

#output_mode: 1: Console, 2: File, 3: Console and File
output_mode = 2
text_summarization_obj.output_format(document_gist, output_mode)


# COMMAND ----------

# Below line of code will download the output file from S3 and display the result
check_data = boto3.client("s3",
                  aws_access_key_id = text_summarization_obj.awsAccessKeyID,
                  aws_secret_access_key = text_summarization_obj.awsSecretAccessKey)
        
output = check_data.get_object(Bucket = text_summarization_obj.bucketName, Key = text_summarization_obj.outptFile)

output = pd.read_csv(output['Body'], delimiter="|")
display(output)

# COMMAND ----------

# Article specific Sentence count and Summary sentence count
report = pd.DataFrame(tablular_report_data, columns = ["Artcle title", "Sentence Count", "Summary sentence count"])
display(report)

# COMMAND ----------

# plot of sentences and its weight for each article
for x in range(len(plot_data)):
    print("Article: ", plot_data[x][0])
    print("Summary: ", plot_data[x][2])
    display(pd.DataFrame(plot_data[x][1], columns = ["sentence", "weight"]))
    

# COMMAND ----------


