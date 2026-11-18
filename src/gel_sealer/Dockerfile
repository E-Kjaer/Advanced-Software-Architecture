FROM python:3.14-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV MACHINE_ID=1
ENV PYTHONUNBUFFERED=1

CMD [ "python", "./main.py" ]
