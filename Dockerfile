FROM python:3.10

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY app/ .

ENTRYPOINT [ "python", "meshtastic2traccar.py" ]
