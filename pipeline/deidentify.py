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
from apache_beam.typehints.typehints import Dict, Any


def print_row(row):
    logging.info("received row: %s", row)


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
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    try:
        project_id = pipeline_options.project
    except AttributeError:
        pass

    project_qualified_name = f"projects/{project_id}"
    input_subscription = f"{project_qualified_name}/subscriptions/{known_args.pubsub_subscription}"
    output_logname = f"{project_qualified_name}/logs/{known_args.log_id}"

    logging.info(f"starting the pipeline to read from:{input_subscription}")

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        _ = (
            p
            | 'Read exports from Pub/Sub sink' >> ReadFromPubSub(subscription=input_subscription).with_output_types(bytes)
            | 'Convert to UTF-8' >> beam.Map(lambda data: data.decode("utf-8")).with_output_types(str)
            | 'Convert string to Json' >> beam.Map(lambda msg: json.loads(msg)).with_output_types(Dict[str, Any])
            | 'Print' >> beam.Map(print_row)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
