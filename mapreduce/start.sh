#!/usr/bin/env bash


while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    start)
      REPLICAS="${2}"
      docker-compose --compatibility up -d --scale workers=${REPLICAS:-1}
      shift
      shift
      ;;
    stop)
      docker-compose down --rmi local --volumes --remove-orphans
      shift
      ;;
    *)
      echo "Usage $0 start <number of replicas> - starts cluster for map reduce"
      echo "Usage $0 stop - wipes everything"
      echo "Requirements: docker, docker-compose"
      exit
      ;;
esac
done