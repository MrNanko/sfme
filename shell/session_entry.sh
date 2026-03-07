#!/bin/sh
set -eu

copy_root_sessions_to_project() {
  copied=0
  for session_file in /*.session; do
    [ -f "$session_file" ] || continue
    target_file="/sfme/$(basename "$session_file")"
    cp "$session_file" "$target_file"
    echo "session copied to: $target_file"
    copied=1
  done

  if [ "$copied" -eq 0 ]; then
    echo "no session file found under / (pattern: /*.session)"
  fi
}

copy_project_sessions_to_root() {
  copied=0
  for session_file in /sfme/*.session; do
    [ -f "$session_file" ] || continue
    target_file="/$(basename "$session_file")"
    cp "$session_file" "$target_file"
    echo "session copied to: $target_file"
    copied=1
  done

  if [ "$copied" -eq 0 ]; then
    echo "no session file found under /sfme (pattern: /sfme/*.session)"
  fi
}

run_login() {
  trap copy_root_sessions_to_project EXIT
  python3 -m sfme
}

run_start() {
  copy_project_sessions_to_root
  exec python3 -m sfme
}

mode="${1:-}"
case "$mode" in
  login)
    run_login
    ;;
  start)
    run_start
    ;;
  *)
    echo "usage: /bin/sh /sfme/shell/session_entry.sh [login|start]"
    exit 1
    ;;
esac
