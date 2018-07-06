FROM sd2e/python3 as langbuilder

RUN mkdir -p /app/src
WORKDIR /app
COPY ./setup.py .

RUN python3 setup.py develop

COPY ./src /app



FROM langbuilder as s3builder
RUN apt-get update && \
    apt-get -y install ruby2.3 && \
    apt-get clean
RUN gem install fakes3


