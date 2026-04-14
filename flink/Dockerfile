FROM flink:1.18.0

# Install Python
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Download Kafka Connector JAR
# We'll put it in the lib folder so Flink finds it automatically
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar

WORKDIR /opt/flink

COPY job.py .

USER flink
