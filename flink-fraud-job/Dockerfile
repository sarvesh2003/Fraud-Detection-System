FROM apache/flink:1.18-scala_2.12-java11

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip && \
    ln -s /usr/bin/python3 /usr/bin/python  # 👈 fix: make `python` point to `python3`

# Install PyFlink and any other dependencies
COPY requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt

# Add Kafka connector JARs
COPY flink-connector-kafka.jar /opt/flink/lib/
COPY kafka-clients-3.2.0.jar /opt/flink/lib/

# Copy your Flink Python job
COPY fraud_detection.py /opt/flink/job/

# Default run command
CMD ["python3", "/opt/flink/job/fraud_detection.py"]
