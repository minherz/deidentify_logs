# Minimize base image
FROM python:3.8-slim

# Install dependencies
COPY requirements.txt /tmp
RUN pip3 install --upgrade pip && pip3 install -r /tmp/requirements.txt && pip3 check

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.8_sdk:2.36.0 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]