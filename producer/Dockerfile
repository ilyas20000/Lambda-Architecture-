FROM python:3.9

WORKDIR /producer

COPY requirements.txt ./
COPY Producer.py /producer
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "/producer/Producer.py" ]