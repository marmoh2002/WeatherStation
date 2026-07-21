#!/bin/bash
set -e

echo "=== Deploying WeatherStation to Minikube ==="

echo "1/7 Applying volumes and configmap..."
kubectl apply -f k8s/volumes.yaml
kubectl apply -f k8s/configmap.yaml

echo "2/7 Deploying Kafka..."
kubectl apply -f k8s/kafka-deployment.yaml
kubectl wait --for=condition=ready pod -l app=kafka --timeout=120s

echo "3/7 Deploying Schema Registry..."
kubectl apply -f k8s/schema-registry-deployment.yaml
kubectl wait --for=condition=ready pod -l app=schema-registry --timeout=120s

echo "4/7 Deploying Producer and Stream..."
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/stream-deployment.yaml

echo "5/7 Deploying Consumer..."
kubectl apply -f k8s/consumer-deployment.yaml

echo "6/7 Deploying Elasticsearch..."
kubectl apply -f k8s/elasticsearch-deployment.yaml
kubectl wait --for=condition=ready pod -l app=elastic-search --timeout=180s

echo "7/7 Deploying Kibana and Indexer..."
kubectl apply -f k8s/kibana-deployment.yaml
kubectl apply -f k8s/indexer-deployment.yaml

echo ""
echo "=== All deployed! ==="
echo "Run 'kubectl get pods' to check status"
echo "Run 'minikube service kibana --url' to access Kibana"
