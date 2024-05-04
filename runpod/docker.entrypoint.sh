#!/bin/bash

set -Eeuo pipefail
shopt -s nullglob

# RunPod instances do not self-terminate after the main process exits, so we could get stuck in a bug loop and waste expensive GPU rental.
self_terminate() {
  if [[ ${NO_SELF_TERMINATE:-} == "1" ]]; then
    echo 'Not terminating!'
    sleep infinity
  fi

  # Wait for any pending Telegraf exports. NOTE: 5 seconds is not long enough.
  # Ensure newline at end of log file, or else exporter may not export last (unterminated) line. This often happens on crashes and panics.
  echo '' >>/app.log
  sleep 10

  echo 'Self terminating...'
  runpodctl remove pod $RUNPOD_POD_ID

  exit 1
}

trap 'self_terminate' ERR EXIT

# Set up SSH for debugging.
mkdir -p ~/.ssh
echo $PUBLIC_KEY >~/.ssh/authorized_keys
ssh-keygen -A
service ssh start

/telegraf --config /telegraf.conf &
/promtail -config.file /promtail.yaml -config.expand-env=true &
# Wait for log collector to start, as it won't export existing log entries before it starts.
sleep 5

if [[ -d /workspace ]]; then
  ln -s /workspace/hndr-data /hndr-data
fi

if [[ -f /app/$MAIN/main.py ]]; then
  python3 -m cudf.pandas /app/$MAIN/main.py |& tee /app.log
else
  # We cannot use ts-node as it doesn't support node:worker.
  node /app/$MAIN/main.js |& tee /app.log
fi
