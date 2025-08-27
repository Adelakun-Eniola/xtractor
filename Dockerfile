FROM python:3.13-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    wget \
    gnupg \
    unzip \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxrandr2 \
    libgbm1 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Env vars for Selenium
ENV CHROMIUM_PATH=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["python", "run.py"]
