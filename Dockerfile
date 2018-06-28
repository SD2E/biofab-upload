FROM sd2e/python3 as langbuilder

RUN mkdir -p /app
WORKDIR /app
COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY ./biofab-upload /app/biofab-upload
COPY ./agave /app/agave
