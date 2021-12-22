
import apache_beam as beam
from apache_beam.io import WriteToText

# class MergeCSVFn(beam.DoFn):
#     def process(self, element):
#         print(element)
#         return [len(element)]

if __name__ == '__main__':

    with beam.Pipeline() as pipeline:

          ## C. Read from multiple files:
        files = pipeline | beam.io.ReadFromText(
              'gs://york-project-bucket/b_huang_files/2021-12-17-01-03/part-r-*')
        #mergedData = files | beam.Map(print)
          ## OR: using the Class:
        #mergedData = files | beam.ParDo(MergeCSVFn())

          ## do count:
        count = files | 'Count all elements' >> beam.combiners.Count.Globally()
        count | beam.Map(print)

        files | beam.io.WriteToText('gs://york-project-bucket/b_huang_files/2021-12-20-16-35/textFileCombined-test1')

          ## 1st task "read" file from bucket:
          # lines | "Write" >> WriteToText('./output.txt')

          ## 2nd task "write" it back to bucket:
        #lines | beam.io.WriteToText('gs://york-project-bucket/b_huang_files/2021-12-20-16-35')

        pass





