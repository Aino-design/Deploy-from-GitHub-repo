# Use official Python 3.11 image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy files
COPY . /app

# Install system dependencies for aiohttp (Linux)
RUN apt-get update && \
    apt-get install -y build-essential libssl-dev && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Run the bot
CMD ["python", "main.py"]