FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y \
    google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver
ENV GOOGLE_CHROME_BIN=/usr/bin/google-chrome

COPY . .

CMD ["gunicorn", "-w", "4", "-k", "gevent", "run:app"]