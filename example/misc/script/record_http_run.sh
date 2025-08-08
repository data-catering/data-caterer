#!/usr/bin/env bash
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}Generating data for OpenAPI/Swagger doc: docker/mount/http/petstore.json${NC}"
sleep 4
./run.sh AdvancedHttpPlanRun

echo -e "${GREEN}Let's check what hit our http-bin server${NC}"
sleep 4
docker logs docker-http-1

echo -e "${GREEN}Cool! We can see the same ID values are used across the GET and DELETE HTTP calls${NC}"
echo -e "${GREEN}Try with your own OpenAPI/Swagger doc now.${NC}"
