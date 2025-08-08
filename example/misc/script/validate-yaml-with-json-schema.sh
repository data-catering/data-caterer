#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
LIGHT_BLUE='\033[1;34m'
NC='\033[0m'

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
json_schema_version=${JSON_SCHEMA_VERSION:-latest}
num_failed_validation=0

echo "Checking if $json_schema_version JSON schema is valid"
if ajv compile --spec=draft2019 -c ajv-formats -s "${script_dir}/../../schema/data-caterer-${json_schema_version}.json"; then
  echo -e "${GREEN}Valid JSON schema${NC}"
else
  echo -e "${RED}Invalid JSON schema, exiting${NC}"
  exit 1
fi

echo -e "Validating YAML files based on ${json_schema_version} JSON schema"
for file in docker/data/custom/task/*/*/*.yaml docker/data/custom/task/*/*.yaml docker/data/custom/validation/*.yaml docker/data/custom/validation/*/*.yaml; do
  if ajv validate --all-errors --spec=draft2019 -c ajv-formats -s "${script_dir}/../../schema/data-caterer-${json_schema_version}.json" -d "${script_dir}/../../${file}"; then
    echo -e "${GREEN}Passed validation, file=${file}${NC}"
  else
    num_failed_validation=$((num_failed_validation+1))
  fi
done

echo -e "${YELLOW}Total failed=${num_failed_validation}${NC}"
if [ "${num_failed_validation}" -gt 0 ]; then
  exit 1
else
  exit 0
fi
