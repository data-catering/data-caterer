#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Checking for vulnerabilities in JAR"

trivy rootfs "${SCRIPT_DIR}/../../app/build/libs"

#gradle :app:dependencyInsight --dependency
