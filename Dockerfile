FROM sd2e/python3 as langbuilder

RUN mkdir -p /app/src
WORKDIR /app
COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY ./src /app



FROM langbuilder as s3builder
RUN apt-get update && \
    apt-get -y install ruby2.3 && \
    apt-get clean
RUN gem install fakes3


