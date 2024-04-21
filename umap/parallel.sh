#!/usr/bin/env bash

set -Eeuo pipefail

n_neighbors=(20 50 100)
min_dist=(0.1 0.25 0.5 0.85)

process_data() {
  N_NEIGHBORS=$1
  MIN_DIST=$2
  SAMPLE_SIZE=$SAMPLE_SIZE N_NEIGHBORS=$N_NEIGHBORS MIN_DIST=$MIN_DIST python main.py
}

export -f process_data
parallel -j 8 --linebuffer process_data ::: "${sample_sizes[@]}" ::: "${n_neighbors[@]}" ::: "${min_dist[@]}"