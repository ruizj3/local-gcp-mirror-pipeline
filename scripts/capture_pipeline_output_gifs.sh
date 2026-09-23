#!/usr/bin/env bash

set -euo pipefail

output_dir="${1:-artifacts/service-gifs}"
mkdir -p "$output_dir"

capture_text_gif() {
  local name="$1"
  local title="$2"
  local source="$3"
  local frames="$output_dir/.${name}-frames"
  local clean_source="$frames/source.txt"
  rm -rf "$frames"
  mkdir -p "$frames"
  LC_ALL=C sed 's/[^ -~]//g' "$source" > "$clean_source"

  awk -v title="$title" -v output_frames="$frames" '
    { lines[NR] = $0 }
    END {
      total = NR
      if (total == 0) lines[++total] = "No output captured"
      for (frame = 1; frame <= 8; frame++) {
        if (frame == 1) {
          start = total - 10
          if (start < 1) start = 1
          finish = total
        } else {
          start = int((frame - 1) * total / 8) + 1
          finish = int(frame * total / 8)
        }
        if (finish < start) finish = start
        file = sprintf("%s/%02d.srt", output_frames, frame)
        printf "1\n00:00:00,000 --> 00:00:01,000\nlocal-gcp-mirror-pipeline\n%s\n\n", title > file
        for (i = start; i <= finish && i <= total; i++) print lines[i] >> file
        close(file)
      }
    }
  ' "$clean_source"

  for subtitle in "$frames"/*.srt; do
    frame="${subtitle%.srt}.png"
    ffmpeg -hide_banner -loglevel error -f lavfi -i "color=c=0x111827:s=1280x720" \
      -vf "subtitles='$subtitle':force_style='FontName=Menlo,FontSize=18,PrimaryColour=&H00FFFFFF,OutlineColour=&H00111827,BorderStyle=1,Outline=2,Alignment=7,MarginL=64,MarginV=48'" \
      -frames:v 1 -y "$frame"
  done
  ffmpeg -hide_banner -loglevel error -framerate 2 -i "$frames/%02d.png" \
    -vf "split[s0][s1];[s0]palettegen=max_colors=128[p];[s1][p]paletteuse" \
    -loop 0 -y "$output_dir/$name.gif"
  rm -rf "$frames"
}

PYTHONUNBUFFERED=1 timeout 12 .venv/bin/python kafka/mock_producer.py > "$output_dir/producer-output.log" 2>&1 || true

docker exec local-gcp-mirror-pipeline-postgres-1 psql -U pipeline_user -d analytics_warehouse -P pager=off -c \
  "SELECT event_id, event_timestamp, user_id, amount, merchant_category, is_fraud, fraud_scenario FROM fraud_events ORDER BY event_timestamp DESC LIMIT 12;" \
  > "$output_dir/worker-postgres-output.log"

capture_text_gif "producer-live" "Kafka producer -> transactions" "$output_dir/producer-output.log"
capture_text_gif "worker-postgres-live" "Spark worker -> PostgreSQL fraud_events" "$output_dir/worker-postgres-output.log"
rm -f "$output_dir/producer-output.log" "$output_dir/worker-postgres-output.log"

if [[ -f "$output_dir/adminer-fraud-events.png" ]]; then
  ffmpeg -hide_banner -loglevel error -framerate 1 -i "$output_dir/adminer-fraud-events.png" \
    -vf "scale=1280:-1" -loop 0 -y "$output_dir/adminer-fraud-events.gif"
  rm -f "$output_dir/adminer-fraud-events.png"
fi
if [[ -f "$output_dir/airflow-dag.png" ]]; then
  ffmpeg -hide_banner -loglevel error -framerate 1 -i "$output_dir/airflow-dag.png" \
    -vf "scale=1280:-1" -loop 0 -y "$output_dir/airflow-dag.gif"
  rm -f "$output_dir/airflow-dag.png"
fi

printf 'Created live output and UI GIFs in %s\n' "$output_dir"