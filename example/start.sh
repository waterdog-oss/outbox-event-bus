# !/bin/bash

# Copy dependencies
cp -f ../build/libs/ktor-event-bus-0.10.jar ./consumer/libs/ktor-event-bus-0.10.jar
cp -f ../build/libs/ktor-event-bus-0.10.jar ./producer/libs/ktor-event-bus-0.10.jar

# Build images
cd consumer && docker build -t consumer:dev .
cd producer && docker build -t producer:dev .

# deploy
kubectl apply -f k8s/zookeeper/zookeeper.yaml
kubectl apply -f k8s/kafka/kafka.yaml
kubectl apply -f k8s/manager/kafka-manager.yaml
kubectl apply -f k8s/producer/producer.yaml
kubectl apply -f k8s/consumer/consumer.yaml

