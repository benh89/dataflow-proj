
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam import io
from apache_beam.dataframe import frame_base
from apache_beam.io import fileio
import pandas as pd
# pytype: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.pvalue import AsSingleton


if __name__ == '__main__':

    with beam.Pipeline() as p:
        data = p | beam.io.ReadFromText(
                  'gs://york-project-bucket/b_huang_files/2021-12-17-01-03/part-r-*')

        data | 'Filter header' >> beam.Filter(lambda df: df[0] == 'author')
        data | 'Write' >> WriteToText('./output1.txt')

        cnt = data | 'Count all element' >> beam.combiners.Count.Globally()
        cnt | beam.Map(print)





