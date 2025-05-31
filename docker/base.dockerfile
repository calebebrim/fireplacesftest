FROM python:alpine

# Upgrade pip and system packages to address vulnerabilities
RUN apk update && apk upgrade && apk add --no-cache gcc musl-dev && \
	pip install --upgrade pip

# Set working directory
WORKDIR /app

# Copy C source folder
COPY src/ ./src/

