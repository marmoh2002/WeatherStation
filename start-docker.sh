#!/bin/bash
set -e

echo "=== Starting WeatherStation (Docker Compose) ==="

echo "1. Building Maven project..."
mvn clean package -Dmaven.test.skip=true

echo "2. Starting Docker Compose environment..."
docker-compose up -d --build

echo ""
echo "=== Success! ==="
echo "All services are starting in the background."
echo "To view logs, run: docker-compose logs -f"
echo "To shut down, run: docker-compose down -v"
