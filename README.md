# Handle sensitive data in logs using Google Cloud services

Logs often contain sensitive or proprietary information that should be obscured or de-identified.
This proof of concept demonstrates how you can use DLP service in Google Cloud to de-identify log entries that are ingested into Google Cloud Logging. More about it you can read in the [Medium post][1].

_Information in this readme will reflect blog post in Media_

## How to use

The basic Dataflow worker does not include Cloud Logging library. You will need to build the [custom Dataflow image][2] or to use one that I've already created for you: `gcr.io/minherz/log-deidentification-custom-image:with_sdk`.

### Create custom Dataflow container image

To build the image you will need to [install][3] gcloud CLI and then authenticate using `gcloud auth login`.
After that run `./build.sh` from the `custom_image` folder. The script accepts the following parameters (order is important):

1. Project id where the image will be built. If you do not provide it, the script will try to take it from gcloud CLI configuration.
1. URI of the container image. The URI should point to the repository where the built container image should be pushed to. If you do not provide it then the image will be stored in the Container Repository of the project with the project id (set in the first argument).
1. Container image label. The `latest` will be used by default.

### Run Dataflow pipeline

Run the pipeline from `pipeline` folder using `./install.sh` script. The script accepts the following argument (order is important):

1. Project id where the pipeline will run. If you do not provide it, the script will try to take it from gcloud CLI configuration.
2. URI to the custom Dataflow container image that you built. If it is not provided the default URI `gcr.io/minherz/log-deidentification-custom-image:with_sdk` will be used.

Once the pipeline is launched you can test it by writing logs to the same project where the pipeline is running with the log name "logs-with-pii":

```bash
gcloud --project=${PROJECT_ID} logging write logs-with-pii "test with SSN 543-23-4321"
```

The de-identified logs will be ingested into the same project but with the different logname: "no-pii-logs". To see the de-identified logs run the following command. Use the project id where you pipeline is running:

```bash
gcloud --project=${PROJECT_ID} logging read "logName:no-pii-logs"
```

### A couple of things about this PoC

This is a very simple proof of concept. As such it operates with the following assumptions:

* No performance optimizations for the pipeline is done
* The payload of the logs is expected to be the `textPayload`. If you ingest logs with `jsonPayload` the pipeline will throw an error
* The de-identification supports **ONLY** SSN


[1]: https://medium.com/google-cloud/protect-sensitive-info-in-logs-using-google-cloud-4548211d4654
[2]: https://cloud.google.com/dataflow/docs/guides/using-custom-containers
[3]: https://cloud.google.com/sdk/docs/install
