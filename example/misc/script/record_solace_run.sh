#!/usr/bin/env bash
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}Generating data for Solace${NC}"
sleep 4
./run.sh AdvancedSolacePlanRun

echo -e "${GREEN}Let's check how many messages are queued${NC}"
sleep 4
curl -s -u admin:admin localhost:8080/SEMP/v2/monitor/msgVpns/default/queues/rest_test_queue/msgs | jq '.meta'

echo -e "${GREEN}Try with your own Solace messages now.${NC}"
