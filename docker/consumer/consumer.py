#!/usr/bin/python3

#local files(same folder)
from modelFunctions import *
from kafkavar import *
from hbasevar import *

#python libraries
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio
from kafka import KafkaConsumer
import happybase as hb
import re
from string import punctuation
import json

#TODO: manage args
#import sys
#import argparse


#TODO: use ConnectionPool to improve paralelism?(hb.ConnectionPool(size=3,host=hbHost,port=hbPort,timeout=None)) 
#TODO: raise error if arrays have different len
#Store set of values using the key
def storeHB(key,columns, values):
    if len(columns) == len(values):
        dic={}
        for i in range(len(columns)):
            dic[str.encode(columns[i])]=str.encode(values[i])
        conn = hb.Connection(hbHost,hbPort)
        conn.table(hbTableName).put(key, dic)
        conn.close()

# Load model from file
joblib_model = loadSpamModel()

#Classify mail using machine learning model
#Store label and content
def classifyMail(contentDic):
    content= contentDic['content']
    pred = joblib_model.predict([content])[0]
    #content['prediction'] = pred #No need to add to stream??
    storeHB(contentDic['path'],["content:raw","metadata:label"], [content,pred])
    return contentDic

#Extract mail content from stream Tuple
def extract_mailContent(mailTuple):
    contentDivided= mailTuple[1].split('\n\n', 1)
    if(len(contentDivided)<=1):
        return { 'path': mailTuple[0], 'content': ''}
    return { 'path': mailTuple[0], 'content': contentDivided[1]}

#Check if mail content is not empty in the stream
def is_ContentNotEmpty(contentDic):
    return contentDic['content'] != ''

#Clean mail content in the stream
def cleanContent(contentDic):
    #Remove special characters
    clean = re.sub(r'\\r\\n', ' ', str(contentDic['content']))
    clean = re.sub(r'\W', ' ', clean)
    clean = re.sub(r'http\S+', ' ', clean)
    clean = re.sub(r'\s+[a-zA-Z]\s+', ' ', clean)
    clean = re.sub(r'\^[a-zA-Z]\s+', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean, flags=re.I)
    clean = re.sub("\d+", " ", clean)
    clean = clean.replace('\n', ' ')
    clean = clean.translate(str.maketrans("", "", punctuation))
    clean = clean.lower()
    
    contentDic['content'] = clean
    return contentDic

#Word count of content inside the stream
#Store word count
def countWordsContent(contentDic):
    text = contentDic['content']
    wordC={}
    for word in text.split(" "):
        if (word != '' and word != ' '):
            if word in wordC:
                wordC[word] += 1
            else:
                wordC[word] =1
    #contentDic['wordCount'] = wordC  #No need to add to stream??
    storeHB(contentDic['path'],["metadata:wordCount"], [json.dumps(wordC)])
    return contentDic

#Extract mail metadata from stream Tuple
def extract_mailMetadata(mailTuple):
    contentDivided= mailTuple[1].split('\n\n', 1)
    return { 'path': mailTuple[0], 'metadata': contentDivided[0]}

#Extract subject from metadata in the stream
#Store metadata and subject
def extract_subjectMetadata(metadataDic):
    #TODO: leave more open to use for other information
    metadata = metadataDic['metadata']
    metadata = metadata.replace(' Subject2: ',' Subject: ').replace(' (Subject- ',' (Subject: ')
    subjectDiv = metadata.split('Subject: ' ,1)
    subject = ''
    if(len(subjectDiv)>1):
        subject = subjectDiv[1].split("\n" ,1)[0]
    #metadata['subject'] = subject #No need to add to stream??
    storeHB(metadataDic['path'],["metadata:raw","metadata:subject"], [metadata,subject])
    return metadataDic

def main():

    #TODO: Fix this wait
    #sleep until hbase and kafka are up
    time.sleep(60)
    
    useBeam = True

    #TODO: check first if kafka topic exist?
    #TODO: check kafka connection
    #TODO: include kafka group ID
    if useBeam:
        #TODO: Check HBase connection
        #Check if table exist and create it otherwise
        conn = hb.Connection(hbHost,hbPort)
        if not hbTableName.encode('utf-8') in conn.tables():
            conn.create_table(hbTableName, hbFamilies)
        conn.close()

        #Define kafka configuration
        kafka_config = {"topic": kafkaTopic, "bootstrap_servers": kafkaServers}#,"group_id":kafkaGrId

        #Streaming pipelines
        with beam.Pipeline(options=PipelineOptions()) as p:
            #3 pipelines: Metadata&Subject, Content&Label, WordCount

            inputTuples = p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(kafka_config)

            content = (inputTuples | "Extract content" >> beam.Map(extract_mailContent))
            #TODO: filter empty content mails????  | "filter empy content" >> beam.Filter(is_ContentNotEmpty))

            classifiedContent = content | "Classify as SPAM/HAM and store"  >> beam.Map(classifyMail)

            wordC = (content | "Clean content" >> beam.Map(cleanContent)
                #TODO: word count exploiting beam(window strategy?)
                #| 'Fixed-size windows' >> beam.WindowInto()
                #| "Word" >> .....
                #| "Count" >> beam.combiners.Count.PerElement()
                | "Count and store" >> beam.Map(countWordsContent))

            metadata = (inputTuples | "Extract metadata" >> beam.Map(extract_mailMetadata)
               | "Extract subject and store" >> beam.Map(extract_subjectMetadata))

            #| 'Writing to stdout' >> beam.Map(print))

    else:
        #Create Kafka consumer
        consumer = KafkaConsumer(kafkaTopic, bootstrap_servers = kafkaServers)#group_id = kafkaGrId

        #Receive and store kafka data
        dataCollected = []
        for message in consumer:
            dataCollected.append((message.key,message.value))
            print(message.key)
            #print(message.value)
            #print('')
            #print('')



if __name__ == "__main__":
   main()
