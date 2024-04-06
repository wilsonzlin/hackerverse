#!/bin/bash

set -Eeuo pipefail
shopt -s nullglob

# We could be running:
# - locally on a dev machine.
# - on bare metal, with host networking, where the host already has metrics and logs collection, and a StatsD server.
# - on an isolated individual VM.
# - on a container service like RunPod or Oracle Cloud Container Instances.

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

export HF_DATASETS_OFFLINE=1
export NODE_NO_WARNINGS=1
export NODE_OPTIONS='--max-old-space-size=16384 --stack-trace-limit=1024'
export TRANSFORMERS_OFFLINE=1
# We cannot use ts-node as it doesn't support node:worker.
node /app/hn-crawler/main.js |& tee /app.log
