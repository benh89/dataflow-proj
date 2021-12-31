import json
import apache_beam as beam
import ast
from apache_beam.io import WriteToText, fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime
from google.cloud import pubsub_v1

#element = '4365 Kenneth Green Apt. 712, East Ashleystad, AR 03333'
element = 'USNS Dominguez, FPO, AP 69504'     # military address

num = element.split()[0]
st = (element.split(',')[0]).split()[1:]
street = ' '.join(st)
city = element.split(',')[1]
state = (element.split(',')[-1]).split()[-2]
zip = int(element.split()[-1])

print(city)

