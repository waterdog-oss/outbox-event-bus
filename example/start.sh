# !/bin/bash

# Copy dependencies
cp -f ../build/libs/outbox-event-bus-0.11.jar ./consumer/libs/outbox-event-bus-0.11.jar
cp -f ../build/libs/outbox-event-bus-0.11.jar ./producer/libs/outbox-event-bus-0.11.jar

# Build images
cd consumer && docker build -t consumer:dev .
cd producer && docker build -t producer:dev .

# deploy
kubectl apply -f k8s/zookeeper/zookeeper.yaml
kubectl apply -f k8s/kafka/kafka.yaml
kubectl apply -f k8s/manager/kafka-manager.yaml
kubectl apply -f k8s/producer/producer.yaml
kubectl apply -f k8s/consumer/consumer.yaml

