#!/usr/bin/python3

#local files(same folder)
from readingFunctions import *
from kafkavar import *

#python libraries
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio
from kafka import KafkaProducer

#TODO: manage args
#import sys
#import argparse


def main():

	#TODO: Fix this wait
	#sleep until kafka is up
	time.sleep(60)
	
	datapath= 'Data'
	useBeam = True

	#filesPath = obtain_mailPathList('Data', untar=True)
	filesPath = obtain_mailPathList(datapath)

	#TODO: Check first if kafka topic exist?
	#TODO: Check kafka connection?
	if useBeam:
		#Create streaming pipeline with beam
		with beam.Pipeline(options=PipelineOptions()) as p:
			publishKafka = (p
				| "Creating data" >> beam.Create(filesPath)
				#TODO: Obtain file list inside Pipeline
				#| fileio.MatchFiles()
				#| watchForNewFiles()
				#| fileio.ReadMatches()
				| "Read mails" >> beam.Map(lambda path: (path,readFilepath(path)))
				| "Pushing messages to Kafka" >> kafkaio.KafkaProduce(
					topic= kafkaTopic,
					servers= kafkaServers
					)
				)

			#publishKafka | 'Writing to stdout' >> beam.Map(print)

	else:
		#Create kafka producer
		producer = KafkaProducer(bootstrap_servers = kafkaServers)

		#TODO:one loop could be avoided(obtain_mailPathList)
		#iterate and send
		for file_path in filesPath:
			content = readFilepath(file_path)
			producer.send(kafkaTopic,key=str.encode(file_path),value=str.encode(content))
			print("File sent: " + file_path)
   

if __name__ == "__main__":
   main()