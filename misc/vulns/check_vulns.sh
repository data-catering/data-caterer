#!/usr/bin/env bash

echo "Checking for vulnerabilities in JAR"

trivy rootfs ../../app/build/libs

#gradle dependencyInsight --dependency
