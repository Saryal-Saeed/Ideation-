# Use Python 3.9 as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Create directories for output files (if not already in code)
RUN mkdir -p /app/data /app/separated_csvs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command to run your pipeline
CMD ["python", "pipeline.py"]
