# Handle sensitive data in logs using Google Cloud services

_Information in this readme will reflect blog post in Media_

## TL;DR

Use `install.sh` to provision the resources for the example environment.

Post logs for de-identification using Cloud SDK command:

```bash
gcloud --project=${PROJECT_ID} logging write logs-with-pii "test with SSN 543-23-4321"
```

See the logs with masked data using Cloud SDK command:

```bash
gcloud --project=${PROJECT_ID} logging logs list --filter="logName:no-pii-logs"
```
