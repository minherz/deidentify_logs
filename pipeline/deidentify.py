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
import logging
import json
import os

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.typehints.typehints import Any, Dict, List

from google.cloud import dlp_v2
from google.cloud import logging_v2


# identify payloads
INSPECT_CONFIG = {
    "info_types": [
        {"name": "US_SOCIAL_SECURITY_NUMBER" }
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



def print_row(row):
    logging.info("received row: %s", row)


class OneListFn(beam.CombineFn):
    """Combines all elements into a single list."""
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


class DeidentifyLogsFn(beam.DoFn):
    """Deidentify SSN from textPayload field"""
    def __init__(self, project: str):
        self.project = project
        self.dlp_client = None

    def _map_headers(self, header):
        """Convert header to a dictionary {'name': HEADER_NAME}"""
        return {"name": header}

    def _map_rows(self, log_entry):
        """Convert textPayload from log entry to string value"""
        return {"values": [{"string_value": log_entry['textPayload']}]}

    def process(self, logs):
        # initiate dlp client on first demand
        modified_logs = []
        if not self.dlp_client:
            self.dlp_client = dlp_v2.DlpServiceClient()
        if not self.dlp_client:
            logging.error('FAILED to initialize DLP client')
        else:
            headers = map(self._map_headers, ["textPayload"])
            rows = map(self._map_rows, logs)

            table_item = {
                "table": {
                    "headers": headers,
                    "rows": rows
                }
            }
            response = self.dlp_client.deidentify_content(
                request={
                    "parent": self.project,
                    "deidentify_config": DEIDENTIFY_CONFIG,
                    "inspect_config": INSPECT_CONFIG,
                    "item": table_item,
                }
            )
            for i in range(len(logs)):
                copied_log = dict(logs[i])
                copied_log['insertId'] = 'deid-' + copied_log['insertId']
                copied_log['textPayload'] = response.item.table.rows[i].values[0].string_value
                modified_logs.append(copied_log)

        yield modified_logs


class SendLogsFn(beam.DoFn):
    """Ingest array of log entries to designated log"""
    def __init__(self, log_name: str):
        self.log_name = log_name
        self.logger = None

    def _set_log_name(self, log):
        log['logName'] = self.log_name
        return log

    def process(self, logs):
        logs = list(map(self._set_log_name, logs))
        if not self.logger:
            gcp_logging_client = logging_v2.Client()
        if gcp_logging_client:
            self.logger = gcp_logging_client.logger(self.log_name)
            if self.logger:
                self.logger.client.logging_api.write_entries(logs)
        else:
            logging.error('FAILED to initialize GCP logger')
        yield logs


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the deidentification pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input-pubsub',
        dest='pubsub_subscription',
        required=True,
        help="Input PubSub subscription where logs will be pushed")
    parser.add_argument(
        '--log-id',
        dest='log_id',
        required=True,
        help='Output log Id to ingest de-identified log entries to.')
    parser.add_argument(
        "--interval",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    try:
        project_id = pipeline_options.view_as(GoogleCloudOptions).project
    except AttributeError:
        pass
    if not project_id:
        raise "Project id is not provided. Pass it using '--project' argument or setup as GOOGLE_CLOUD_PROJECT env var"

    project_qualified_name = f"projects/{project_id}"
    input_subscription = f"{project_qualified_name}/subscriptions/{known_args.pubsub_subscription}"
    window_interval_sec = known_args.interval
    output_logname = f"{project_qualified_name}/logs/{known_args.log_id}"

    logging.info(f"starting the pipeline to read from:{input_subscription}")

    # The pipeline will be run on exiting the with block.
    p = beam.Pipeline(options=pipeline_options)
    logs_with_pii = (
        p
        | 'Read exports from Pub/Sub sink' >> ReadFromPubSub(subscription=input_subscription).with_output_types(bytes)
        | 'Convert to UTF-8' >> beam.Map(lambda data: data.decode("utf-8")).with_output_types(str)
        | 'Convert string to Json' >> beam.Map(lambda msg: json.loads(msg)).with_output_types(Dict[str, Any])
    )
    batch = (
        logs_with_pii
        | f'Collect logs into batches received within last ${window_interval_sec} secs' >> beam.WindowInto(FixedWindows(window_interval_sec, 0))
        | 'Convert batched logs into one log array' >> beam.CombineGlobally(OneListFn()).with_output_types(List[Dict[str, Any]]).without_defaults()
    )
    _ = (
        batch
        | "Deidentify using primitive transformation" >> beam.ParDo(DeidentifyLogsFn(project_qualified_name))
        | "Send to Cloud Logging" >> beam.ParDo(SendLogsFn(output_logname))
        # | 'Print' >> beam.Map(print_row)
    )
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
