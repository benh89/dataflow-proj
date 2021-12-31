import json
import apache_beam as beam
import ast
from apache_beam.io import WriteToText, fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime
from google.cloud import pubsub_v1


# pub/sub values
PROJECT_ID = "york-cdf-start"
TOPIC = "dataflow-order-stock-update"
#SUBID = 'projects/york-cdf-start/subscriptions/bh-blackfriday-orders-sub-update
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

blackfriday_sub2 = "projects/york-cdf-start/subscriptions/dataflow-project-orders-sub"
blackfriday_sub1 = "projects/york-cdf-start/subscriptions/bh-blackfriday-orders-sub"
# table_spec1 = 'york-cdf-start:bhuang_project_1.usd_order_payment_history'
# table_spec2 = 'york-cdf-start:bhuang_project_1.eur_order_payment_history'
# table_spec3 = 'york-cdf-start:bhuang_project_1.gbp_order_payment_history'

## formatting
class decode_json(beam.DoFn):
    def process(self, element):
        dat = json.loads(element.decode("utf-8"))
        return [dat]


class price_calcu(beam.DoFn):
    def process(self, element):
        _sum_ = 0
        for k, val in element.items():
            if k == 'order_items':
                for order_item_elements in val:
                    for key, value in order_item_elements.items():
                        if key == 'price':
                            _sum_ += value
        if k == 'cost_shipping' or k == 'cost_tax':
            _sum_ += val
        element['cost_total'] = round(_sum_, 2)
        yield element


class add_namecol(beam.DoFn):
    def process(self, element):
        element['customer_first_name'] = element['customer_name'].split()[0]
        element['customer_last_name'] = element['customer_name'].split()[1]
        yield element


class parsing_address(beam.DoFn):
    def process(self, element):
        new_dict = {}
        new_dict["order_building_number"] = element['order_address'].split()[0]
        order_street = (element['order_address'].split(',')[0]).split()[1:]
        new_dict["order_street_name"] = ' '.join(order_street)
        new_dict["order_city"] = element['order_address'].split(',')[1]
        new_dict["order_state"] = (element['order_address'].split(',')[-1]).split()[-2]
        new_dict["order_zipcode"] = int(element['order_address'].split()[-1])
        element["order_address"] = [new_dict]
        yield element


def is_usd(element1):
    return element1['order_currency'] == 'USD'

def is_eur(element2):
    return element2["order_currency"] == "EUR"

def is_gbp(element3):
    return element3['order_currency'] == 'GBP'

class filter_columns(beam.DoFn):
    def process(self, element):
        new_dict = {}
        new_dict["order_id"] = element['order_id']
        new_dict["order_address"] = element['order_address']
        new_dict["customer_first_name"] = element['customer_first_name']
        new_dict["customer_last_name"] = element['customer_last_name']
        new_dict["customer_ip"] = element['customer_ip']
        new_dict["cost_total"] = element['cost_total']
        element = new_dict
        yield element


## the purpose to break out the pipeline and then yield a new set
class order_details(beam.DoFn):
    def process(self, element):
        columns = ["order_id", "order_items"]
        details = {key: element[key] for key in columns}
        yield details



def publish(publisher, topic, message):
    data = json.dumps(message).encode('utf-8')
    return publisher.publish(topic_path, data=data)

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=20):
        print('Publishing message on {} threw an Exception {}.'.format(message_future.exception()))
    else:
        print(message_future.result())


## program run:
if __name__ == '__main__':

    # table_schema for BigQuery
    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'Nullable'},
            {'name': 'order_address', 'type': 'RECORD', 'mode': 'REPEATED',
             'fields': [
                 {'name': 'order_building_number', 'type': 'STRING', 'mode': 'Nullable'},
                 {'name': 'order_street_name', 'type': 'STRING', 'mode': 'Nullable'},
                 {'name': 'order_city', 'type': 'STRING', 'mode': 'Nullable'},
                 {'name': 'order_state', 'type': 'STRING', 'mode': 'Nullable'},
                 {'name': 'order_zipcode', 'type': 'STRING', 'mode': 'Nullable'},
             ],},
            {'name': 'customer_first_name', 'type': 'STRING', 'mode': 'Nullable'},
            {'name': 'customer_last_name', 'type': 'STRING', 'mode': 'Nullable'},
            {'name': 'customer_ip', 'type': 'STRING', 'mode': 'Nullable'},
            {'name': 'cost_total', 'type': 'FLOAT', 'mode': 'Nullable'}
        ]}

    table_spec1 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='bhuang_project_1',
        tableId='usd_order_payment_history1')

    table_spec2 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='bhuang_project_1',
        tableId='eur_order_payment_history1')

    table_spec3 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='bhuang_project_1',
        tableId='gbp_order_payment_history1')


    pipeline_options = beam.options.pipeline_options.PipelineOptions(streaming=True)
    #new_options = pipeline_options.view_as(beam.options.pipeline_options.StandardOptions)
    #new_options.streaming = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        blackfriday_orders_original = (pipeline | 'Reading from pubsub' >> beam.io.ReadFromPubSub(
                                                              subscription=blackfriday_sub1
                                                                        )
                                                | 'transforming' >> beam.ParDo(decode_json())
                                       )

        blackfriday_orders_sum = blackfriday_orders_original | beam.ParDo(price_calcu())
        blackfriday_orders_addname = blackfriday_orders_sum | beam.ParDo(add_namecol())
        # transforming the address as required
        blackfriday_orders = blackfriday_orders_addname | beam.ParDo(parsing_address())

        ## orders divided
        usd_blackfriday_orders = blackfriday_orders | beam.Filter(is_usd)
        eur_blackfriday_orders = blackfriday_orders | beam.Filter(is_eur)
        #eur_blackfriday_orders = (blackfriday_orders | beam.Filter(is_eur) | beam.Map(print))
        gbp_blackfriday_orders = blackfriday_orders | beam.Filter(is_gbp)

        ## order details
        order_detail_info = blackfriday_orders_original | beam.ParDo(order_details())

        USD_orders = usd_blackfriday_orders | "finalizing_USD" >> beam.ParDo(filter_columns()) | beam.Map(print)
        EUR_orders = eur_blackfriday_orders | "inalizing_EUR" >> beam.ParDo(filter_columns())
        GBP_orders = gbp_blackfriday_orders | "inalizing_GBP" >> beam.ParDo(filter_columns())

        # USD_orders | "Write-USD" >> beam.io.WriteToBigQuery(
        #     table_spec1,
        #     schema=table_schema,
        #     #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        #
        #
        # EUR_orders | "Write-EUR" >> beam.io.WriteToBigQuery(
        #     table_spec2,
        #     schema=table_schema,
        #     #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        #
        #
        # GBP_orders | "Write-GBP" >> beam.io.WriteToBigQuery(
        #     table_spec3,
        #     schema=table_schema,
        #     #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    while True:
        message_future = publish(publisher, topic_path, order_detail_info)
        message_future.add_done_callback(callback)



    result = pipeline.run()
    result.wait_until_finish()



