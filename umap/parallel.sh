#!/usr/bin/env bash

set -Eeuo pipefail

sample_sizes=(100000 500000 1000000 4000000)
n_neighbors=(20 50 100 300)
min_dist=(0.1 0.25 0.5 0.85)

process_data() {
  SAMPLE_SIZE=$1
  N_NEIGHBORS=$2
  MIN_DIST=$3
  SAMPLE_SIZE=$SAMPLE_SIZE N_NEIGHBORS=$N_NEIGHBORS MIN_DIST=$MIN_DIST python main.py
}

export -f process_data
parallel --linebuffer process_data ::: "${sample_sizes[@]}" ::: "${n_neighbors[@]}" ::: "${min_dist[@]}"
