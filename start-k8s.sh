#!/bin/bash
set -e

echo "=== Starting WeatherStation (Kubernetes / Minikube) ==="

echo "1. Ensuring Minikube is running with sufficient resources..."
minikube start --memory=8192 --cpus=4 --driver=docker

echo "2. Building Maven project..."
mvn clean package -Dmaven.test.skip=true

echo "3. Pointing Docker to Minikube's daemon..."
eval $(minikube docker-env)

echo "4. Building Docker images inside Minikube..."
docker build -f Dockerfile.producer.minimal -t weather-producer:latest .
docker build -f Dockerfile.stream.minimal   -t weather-stream:latest .
docker build -f Dockerfile.consumer.minimal  -t weather-consumer:latest .
docker build -f DockerFile.indexer           -t weather-indexer:latest .

echo "5. Deploying all Kubernetes manifests..."
chmod +x k8s/deploy-all.sh
./k8s/deploy-all.sh

echo ""
echo "=== Deployment Triggered! ==="
echo "Kubernetes is now spinning up your pods."
echo "Run 'kubectl get pods -w' to monitor the progress."
echo "Once Kibana is ready, run 'minikube service kibana --url' to access the dashboard."
