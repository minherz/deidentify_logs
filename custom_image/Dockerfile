# Minimize base image
FROM apache/beam_python3.8_sdk:2.36.0

# Install dependencies
COPY requirements.txt /tmp
RUN pip3 install --upgrade pip && pip3 install -r /tmp/requirements.txt && pip3 check
