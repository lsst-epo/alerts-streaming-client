FROM gcr.io/google_appengine/python

# Kafka configurations
RUN apt-get update && apt-get install -y apt-transport-https
RUN apt-get install -y software-properties-common
RUN apt update
RUN wget -qO - https://packages.confluent.io/deb/7.1/archive.key | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/7.1 stable main"
RUN add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main"
RUN apt-get update && apt-get -y install confluent-platform

# Install the fortunes binary from the debian repositories.
RUN apt-get update && apt-get install -y fortunes librdkafka-dev confluent-platform
#default-jdk 

# Change the -p argument to use Python 2.7 if desired.
RUN virtualenv /env -p python3.6

# Install Kafka
# RUN  -t -v "$PWD:/v" quay.io/pypa/$distro /v/packaging/tools/build-manylinux.sh /v /v/artifacts/librdkafka-${distro}.tgz
# WORKDIR /tmp
# RUN wget https://downloads.apache.org/kafka/2.8.0/kafka_2.12-2.8.0.tgz
# RUN tar xzf kafka_2.12-2.8.0.tgz
# RUN mv kafka_2.12-2.8.0 /usr/local/kafka

# Set virtualenv environment variables. This is equivalent to running
# source /env/bin/activate.
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

ADD requirements.txt /app/
RUN pip install -r requirements.txt
ADD . /app/

ENV API_KEY ""
ENV API_SECRET ""

# CMD gunicorn -b :$PORT main:app
# the tls handshake occurs over port 9092
EXPOSE 9092/tcp
WORKDIR /app
CMD [ "python", "./main.py"]