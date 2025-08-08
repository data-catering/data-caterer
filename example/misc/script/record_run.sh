#!/usr/bin/env bash

if [[ $# -eq 0 ]]; then
  read -p "Data source name to run [http]: " data_source
else
  data_source=$1
fi

asciinema rec -c "./misc/script/record_${data_source}_run.sh" --idle-time-limit=2 --overwrite "${data_source}_generation_run.cast"
agg --no-loop "${data_source}_generation_run.cast" "${data_source}_generation_run.gif"
