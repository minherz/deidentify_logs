#!/bin/bash
set -e

project_id=${1:-`gcloud config get-value core/project --quiet`}
if [ -z "${project_id}" ]; then
    echo "Pass project id as first argument or set project using 'gcloud config set project'"
    exit 1
fi
repo=${2}
tag=${3:-"latest"}

if [ -z "${repo}" ]; then
    echo "Repository path is not set"
    repo="gcr.io/$project_id/log-deidentification-custom-image"
fi

image_uri="$repo:$tag"

gcloud --project=$project_id builds submit . --tag $image_uri --timeout=15m

echo "Custom container image URI will be:"
echo "${repo}"
