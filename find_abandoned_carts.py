from __future__ import absolute_import

import argparse
import logging
import re
import json
import os
import glob
import pandas as pd

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
      
class Format_Result(beam.DoFn):
    """formats the results by filtering the desired keys."""
    def process(self, element):
        """Returns an json with desired keys.
        The element is a line of json.
        Args:
          element: the element being processed
        Returns:
          The processed json.
    """
        data_json = json.loads(element[1])
        data = {k: data_json[k] for k in ("timestamp","customer","page","product")}
        return [json.dumps(data)]

def create_json_wrk_file(filepath):
    """ Create page_views_wrk.json with new (keys,values)."""
    df = pd.read_json(os.path.join(os.getcwd(), filepath), lines=True)
    df.timestamp = df.timestamp.astype('string')
    df['same_customer'] = df.customer.eq(df.customer.shift(-1))
    df['next_page'] = df.page.shift(-1)
    df.to_json('input/page_views_wrk.json', orient='records', lines=True)
    
def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the find_abondoned_carts pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    # Create page_views_wrk.json with new (keys,values).
    create_json_wrk_file(known_args.input)
    
    # Filter abandoned carts.
    def is_abandoned_carts(element):
        row=json.loads(element[1])
        return (row['page'] == 'basket') & (row['same_customer'] == False) & (row['next_page'] != 'checkout')
    
    # The pipeline will be run on exiting the with block.
    with beam.Pipeline() as p:
        readable_file = (
             p
             | 'Read json file' >> beam.io.textio.ReadFromTextWithFilename('input/page_views_wrk.json')
             | 'Filter abandoned carts' >> beam.Filter(is_abandoned_carts)
             | 'Format result' >> beam.ParDo(Format_Result())
             | 'Write json file output' >> beam.io.WriteToText(known_args.output, shard_name_template=''))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()