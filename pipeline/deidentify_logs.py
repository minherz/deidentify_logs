# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A log deidentifying workflow."""

import argparse
import base64
import json
import logging

import apache_beam as beam
from google.cloud import dlp_v2
import google.cloud.logging

from apache_beam.io import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints.typehints import Dict, List, Any


INSPECT_CONFIG = {
  "info_types": [
    {"name": "US_SOCIAL_SECURITY_NUMBER" },
    {"name": "EMAIL_ADDRESS"}
  ]
}
DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [
            {
                "primitive_transformation": {
                    "character_mask_config": {
                        "masking_character": '#'
                    }
                }
            }
        ]
    }
}

class OneListFn(beam.CombineFn):
  """Combines all elements into the single list."""
  def create_accumulator(self):
      return []

  def add_input(self, accumulator, input):
      accumulator.append(input)
      return accumulator

  def merge_accumulators(self, accumulators):
      merged = []
      for accumulator in accumulators:
          for item in accumulator:
              merged.append(item)
      return merged

  def extract_output(self, accumulator):
      return accumulator


class SendLogsFn(beam.DoFn):
  """Send the list of log entries to Google Logging."""
  def __init__(self, log_name: str):
    self.log_name = log_name
    self.logger = None

  def change_log_name(self, log):
    log['logName'] = self.log_name
    return log

  def process(self, logs):
    logs = list(map(self.change_log_name, logs))
    if not self.logger:
      gcp_logging_client = google.cloud.logging.Client()
      if not gcp_logging_client:
        logging.error('FAILED to initialize GCP logger')
      self.logger = gcp_logging_client.logger(self.log_name)
    if self.logger:
      self.logger.client.logging_api.write_entries(logs)
    yield logs

class DeidentifyLogsFn(beam.DoFn):
  """Executes tokenization request to DLP"""
  def __init__(self, project: str):
    self.project = project
    self.dlp = None

  def map_headers(self, header):
    return {"name": header}
  
  def map_rows(self, log_entry):
    return {"values": [{"string_value": log_entry['textPayload']}]}

  def process(self, logs):
    if not self.dlp:
      self.dlp = dlp_v2.DlpServiceClient()
    if not self.dlp:
        logging.error('FAILED to initialize DLP client')
    else:
      headers = map(self.map_headers, ["textPayload"])
      rows = map(self.map_rows, logs)

      table_item = {
        "table": {
          "headers": headers,
          "rows": rows
        }
      }
      response = self.dlp.deidentify_content(
        request={
            "parent": self.project,
            "deidentify_config": DEIDENTIFY_CONFIG,
            "inspect_config": INSPECT_CONFIG,
            "item": table_item,
        }
      )
      modified_logs = []
      for i in range(len(logs)):
        row = response.item.table.rows[i]
        copied_log = dict(logs[i])
        copied_log['insertId'] = 'deid-' + copied_log['insertId']
        copied_log['textPayload'] = row.values[0].string_value
        modified_logs.append(copied_log)

    yield modified_logs

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the log deidentifying pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--project",
      help="Project id for subscription and output log. The project will be used to deidentify the logs.",
  )
  parser.add_argument(
      "--output_logname",
      help="Output log name where the deidentified logs will be stored",
  )
  parser.add_argument(
      "--input_subscription",
      help="Input PubSub subscription where logs will be pushed",
  )
  parser.add_argument(
      "--window_interval_sec",
      default=60,
      type=int,
      help="Window interval in seconds for grouping incoming messages.",
  )
  known_args, pipeline_args = parser.parse_known_args(argv)

  project = f"projects/{known_args.project}"
  input_subscription = f"{project}/subscriptions/{known_args.input_subscription}"
  output_logname = f"{project}/logs/{known_args.output_logname}"
  window_interval_sec = known_args.window_interval_sec

  pipeline_options = PipelineOptions(pipeline_args, project=known_args.project, save_main_session=save_main_session, streaming=True)

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    logs_with_pii = (
      p 
      | 'Read exports from Pub/Sub sink' >> ReadFromPubSub(subscription=input_subscription).with_output_types(bytes)
      | 'Convert to UTF-8' >> beam.Map(lambda data: data.decode("utf-8")).with_output_types(str)
      | 'Convert string to Json' >> beam.Map(lambda msg: json.loads(msg)).with_output_types(Dict[str, Any])
    )

    batch = (
      logs_with_pii
      | 'Collect log entries into batches received within ' + str(window_interval_sec) + 's windows' >> beam.WindowInto(FixedWindows(window_interval_sec, 0))
      | 'Convert multiple items into single array' >> beam.CombineGlobally(OneListFn()).with_output_types(List[Dict[str, Any]]).without_defaults()
    )

    _ = (
      batch
      | "De-identify using primitive transformation" >> beam.ParDo(DeidentifyLogsFn(project))
      | "Send to Cloud Logging" >> beam.ParDo(SendLogsFn(output_logname))
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()