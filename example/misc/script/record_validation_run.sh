#!/usr/bin/env bash
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}Generating data for a JSON file and validate the consumed data in another JSON file${NC}"
sleep 4
./run.sh ValidationPlanRun

echo -e "${GREEN}Very fast feedback loop!${NC}"
echo -e "${GREEN}Try now with your own data sources and consumers to validate${NC}"
echo -e "${GREEN}Shift left data quality${NC}"
