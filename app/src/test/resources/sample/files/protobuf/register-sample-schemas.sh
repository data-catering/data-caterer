#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
registry_host=${REGISTRY_HOST:-http://localhost:8081}
echo "Creating JSON for posting schemas to confluent-schema-registry"

my_import_proto=$(cat "${SCRIPT_DIR}/my-import.proto" | tr -d "\n")
example_proto=$(cat "${SCRIPT_DIR}/example.proto" | tr -d "\n")
escaped_my_import=${my_import_proto//\"/\\\"}
escaped_example=${example_proto//\"/\\\"}

cat >"${SCRIPT_DIR}/my-import.json" <<EOF
{
  "schemaType": "PROTOBUF",
  "schema": "${escaped_my_import}"
}
EOF
cat >"${SCRIPT_DIR}/example.json" <<EOF
{
  "schemaType": "PROTOBUF",
  "references": [
    {
      "name": "my-import",
      "subject": "my-import",
      "version": 1
    }
  ],
  "schema": "${escaped_example}"
}
EOF

echo "Posting sample protobuf schema to confluent-schema-registry on url=$registry_host"

curl -X POST "${registry_host}/subjects/my-import/versions" -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @"${SCRIPT_DIR}/my-import.json"
curl -X POST "${registry_host}/subjects/example/versions" -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @"${SCRIPT_DIR}/example.json"

echo
echo "Finished posting proto schemas to confluent-schema-registry!"
echo

curl -X GET "${registry_host}/subjects/my-import/versions/1"
echo
curl -X GET "${registry_host}/subjects/example/versions/1"

rm "${SCRIPT_DIR}/my-import.json"
rm "${SCRIPT_DIR}/example.json"
