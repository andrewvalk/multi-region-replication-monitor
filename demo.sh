#!/bin/bash
echo "Starting Multi-Region Replication Monitor..."
docker-compose up -d
sleep 15
python src/replication_monitor.py
