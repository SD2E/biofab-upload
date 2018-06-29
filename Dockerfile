FROM sd2e/python3 as langbuilder

RUN mkdir -p /app
WORKDIR /app
COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY ./biofab-upload /app/biofab-upload
COPY ./agave /app/agave
COPY ./upload /app/upload


FROM langbuilder as s3builder
RUN apt-get update && \
    apt-get -y install ruby2.3 && \
    apt-get clean
RUN gem install fakes3


