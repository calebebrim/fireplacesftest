FROM python:alpine

# Upgrade pip and system packages to address vulnerabilities
RUN apk update && \
	apk upgrade && \
	apk add --no-cache \
	gcc \
	musl-dev \
	librdkafka-dev \
	openssl-dev \
	cyrus-sasl-dev && \
	pip install --upgrade pip

# Set working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Default command (adjust to your app entry point)
# CMD ["python", "-m", "src.services.fire_event_source"]
