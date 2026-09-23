#!/usr/bin/env bash

set -euo pipefail

duration_seconds="${1:-18}"
output_dir="${2:-artifacts/service-gifs}"
compose_services=(zookeeper kafka postgres adminer airflow)

if ! [[ "$duration_seconds" =~ ^[0-9]+$ ]] || (( duration_seconds < 6 )); then
  printf 'Usage: %s [duration-seconds>=6] [output-directory]\n' "$0" >&2
  exit 2
fi

mkdir -p "$output_dir"
docker compose up -d

printf 'Capturing service startup activity for %s seconds...\n' "$duration_seconds"
for service in "${compose_services[@]}"; do
  service_dir="$output_dir/$service"
  mkdir -p "$service_dir"
  : > "$service_dir/startup.log"

  for ((second = 1; second <= duration_seconds; second++)); do
    {
      printf '[%02ss] %s\n' "$second" "$(docker compose ps --status running --services | grep -Fx "$service" >/dev/null && printf 'RUNNING' || printf 'STARTING')"
      docker compose logs --no-color --tail 5 "$service" 2>/dev/null \
        | sed -E 's/[[:space:]]+$//' \
        | tail -n 3 \
        | cut -c1-100
    } >> "$service_dir/startup.log"
    sleep 1
  done

  # SRT lets ffmpeg animate real log snapshots without requiring a screen recorder.
  awk '
    function emit() {
      if (!started) return
      start = second - 1
      finish = second
      printf "%d\n00:00:%02d,000 --> 00:00:%02d,000\n%s\n\n", second, start, finish, text
    }
    /^\[[0-9]+s\]/ {
      emit()
      timestamp = $0
      sub(/^\[/, "", timestamp)
      sub(/s\].*$/, "", timestamp)
      second = timestamp + 0
      text = $0
      started = 1
      next
    }
    { text = text "\n" $0 }
    END { emit() }
  ' "$service_dir/startup.log" > "$service_dir/captions.srt"

  # Build a compact title card with the captured service timeline underneath.
  ffmpeg -hide_banner -loglevel error \
    -f lavfi -i "color=c=0x111827:s=1280x720:r=1:d=$((duration_seconds + 1))" \
    -vf "drawtext=text='local-gcp-mirror-pipeline':fontcolor=0x93c5fd:fontsize=24:x=64:y=48,drawtext=text='$service':fontcolor=white:fontsize=56:x=64:y=88,subtitles=$service_dir/captions.srt:force_style='FontName=Menlo,FontSize=14,PrimaryColour=&H00FFFFFF,OutlineColour=&H00111827,BorderStyle=1,Outline=2,Alignment=1,MarginL=64,MarginR=64,MarginV=48'" \
    -loop 0 -y "$service_dir.gif"
  rm -f "$service_dir/captions.srt"
  rm -rf "$service_dir"
  printf 'Created %s\n' "$service_dir.gif"
done

cat > "$output_dir/README.md" <<EOF
# Service startup GIFs

Generated with:


    ./scripts/capture_service_gifs.sh $duration_seconds $output_dir

Each GIF shows real Docker Compose status and log output sampled during startup.
EOF

printf 'GIFs are ready in %s\n' "$output_dir"