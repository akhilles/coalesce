#!/bin/sh
# test.sh -- functional suite for coalesce(1)
#
#   ./test.sh              run all checks
#   ./test.sh -v           verbose (print a failed check's output)
#   ./test.sh NAME         run only checks whose id contains NAME
#   COALESCE_TIMEOUT=10 ./test.sh   per-check watchdog seconds (default 5)
#
# Checks run concurrently, each under a hard SIGINT->SIGKILL watchdog, so the
# whole suite is bounded by roughly one timeout no matter how many checks hang.
# Workers run in an isolated XDG_RUNTIME_DIR that is removed on exit.
#
# Requires: the ./coalesce binary (build with `make`) and lsof or fuser.

set -u

HERE=$(cd "$(dirname "$0")" && pwd)
COALESCE=${COALESCE:-"$HERE/coalesce"}
TIMEOUT=${COALESCE_TIMEOUT:-5}
VERBOSE=0
FILTER=""

RTDIR=""
cleanup() {
  [ -n "${RTDIR:-}" ] || return 0
  cleanup_leftovers 2>/dev/null   # reap any worker still bound in RTDIR
  rm -rf "$RTDIR"
}
trap cleanup EXIT INT TERM

# --- args ---
while [ $# -gt 0 ]; do
  case "$1" in
    -v) VERBOSE=1; shift ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) FILTER="$1"; shift ;;
  esac
done

# --- setup ---
if [ ! -x "$COALESCE" ]; then
  echo "test.sh: binary not found: $COALESCE (run 'make' first)" >&2
  exit 2
fi

RTDIR=$(mktemp -d 2>/dev/null || mktemp -d -t coalesce)
export XDG_RUNTIME_DIR="$RTDIR"
mkdir -p "$RTDIR/coalesce"

PASS=0; FAIL=0

# --- worker lifecycle helpers ---

# PID of the process bound to a unix socket, or empty.
sock_pid() {
  s=$1
  if command -v lsof >/dev/null 2>&1; then
    # Filter by the socket path itself; do NOT pass -U, which makes lsof
    # ignore the path and list the owner of every unix socket on the system.
    lsof -t -- "$s" 2>/dev/null | head -n1
  else
    fuser "$s" 2>/dev/null | tr -d ' \t' | head -n1
  fi
}

# Send SIGTERM to the worker bound to <name>'s socket, then SIGKILL if needed.
kill_worker() {
  s="$RTDIR/coalesce/$1.sock"
  [ -e "$s" ] || return 0
  pid=$(sock_pid "$s")
  if [ -n "$pid" ]; then
    kill -TERM "$pid" 2>/dev/null
    i=0
    while [ "$i" -lt 20 ]; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.05
      i=$((i+1))
    done
    kill -KILL "$pid" 2>/dev/null
  fi
  rm -f "$s"
}

# Reap any worker still holding a socket in the runtime dir (timeout safety).
cleanup_leftovers() {
  for s in "$RTDIR"/coalesce/*.sock; do
    [ -e "$s" ] || continue
    pid=$(sock_pid "$s")
    if [ -n "$pid" ]; then
      kill -TERM "$pid" 2>/dev/null
      sleep 0.2
      kill -KILL "$pid" 2>/dev/null
    fi
    rm -f "$s"
  done
}

# --- polling + assertion helpers ---

# Poll status until the worker answers (i.e. is listening), or give up.
wait_up() {
  i=0
  while [ "$i" -lt 120 ]; do
    "$COALESCE" status "$1" >/dev/null 2>&1 && return 0
    sleep 0.05
    i=$((i+1))
  done
  return 1
}

# Poll status until it equals <want>.
wait_state() {
  i=0
  while [ "$i" -lt 120 ]; do
    [ "$("$COALESCE" status "$1" 2>/dev/null)" = "$2" ] && return 0
    sleep 0.05
    i=$((i+1))
  done
  return 1
}

# Poll until <name>'s socket disappears (worker exited and unlinked it).
wait_gone() {
  i=0
  while [ "$i" -lt 120 ]; do
    [ -e "$RTDIR/coalesce/$1.sock" ] || return 0
    sleep 0.05
    i=$((i+1))
  done
  return 1
}

# Poll until <file> contains <pattern>.
wait_grep() {
  i=0
  while [ "$i" -lt 120 ]; do
    grep -q "$1" "$2" 2>/dev/null && return 0
    sleep 0.05
    i=$((i+1))
  done
  return 1
}

is_state() { [ "$("$COALESCE" status "$1" 2>/dev/null)" = "$2" ]; }
count()    { wc -l < "$1" | tr -d ' '; }
# Abort the running check with a message (captured, shown under -v).
fail()     { echo "$@"; exit 1; }

# --- harness ---

# Launch one check in the background under its own watchdog. Checks are
# independent (each owns a uniquely named worker) and so run concurrently; the
# verdict goes to a per-check result file the parent tallies once all finish.
# Files are keyed by id to avoid clobbering. A check registers its worker with
# `trap 'kill_worker NAME' EXIT` so teardown happens on every exit path.
LAUNCHED=""
launch_check() {
  id=$1 fn=$2
  case "$id" in *"$FILTER"*) ;; *) return 0 ;; esac
  LAUNCHED="$LAUNCHED $id"
  (
    out="$RTDIR/out.$id"
    flag="$RTDIR/flag.$id"
    ( $fn ) >"$out" 2>&1 &
    cpid=$!
    # On overrun: mark the flag, SIGINT for a clean exit, then SIGKILL after a
    # 1s grace so a wedged check can never block the wait below or the suite.
    (
      sleep "$TIMEOUT"
      : > "$flag"
      kill -INT "$cpid" 2>/dev/null
      sleep 1
      kill -KILL "$cpid" 2>/dev/null
    ) &
    wpid=$!
    wait "$cpid" 2>/dev/null; rc=$?
    kill "$wpid" 2>/dev/null; wait "$wpid" 2>/dev/null  # stop the watchdog
    if [ -e "$flag" ];   then echo "timeout"
    elif [ "$rc" = 0 ];  then echo "ok"
    else                      echo "fail $rc"
    fi > "$RTDIR/res.$id"
  ) &
}

# Print verdicts in launch order and accumulate PASS/FAIL. A missing result
# file (launcher died unexpectedly) counts as a failure rather than vanishing.
tally() {
  for id in $LAUNCHED; do
    set -- $(cat "$RTDIR/res.$id" 2>/dev/null)
    case "${1:-}" in
      ok)      printf 'ok - %s\n' "$id"; PASS=$((PASS+1)) ;;
      timeout) printf 'not ok - %s (timed out after %ss)\n' "$id" "$TIMEOUT"
               [ "$VERBOSE" = 1 ] && sed 's/^/    | /' "$RTDIR/out.$id" 2>/dev/null
               FAIL=$((FAIL+1)) ;;
      fail)    printf 'not ok - %s (exit %s)\n' "$id" "${2:-?}"
               [ "$VERBOSE" = 1 ] && sed 's/^/    | /' "$RTDIR/out.$id" 2>/dev/null
               FAIL=$((FAIL+1)) ;;
      *)       printf 'not ok - %s (no result)\n' "$id"; FAIL=$((FAIL+1)) ;;
    esac
  done
}

# --- checks ---

# idle + trigger -> run; trigger during run -> dirty; finish + dirty -> one more.
check_basic_state() {
  NAME=t_basic; trap 'kill_worker "$NAME"' EXIT
  cnt="$RTDIR/c_basic"; : > "$cnt"
  "$COALESCE" run "$NAME" -- sh -c 'echo x >>"$1"; sleep 0.4' _ "$cnt" &
  wait_up "$NAME"                  || fail "worker not up"
  is_state "$NAME" idle            || fail "expected idle"
  "$COALESCE" trigger "$NAME"      || fail "trigger failed"
  wait_state "$NAME" running       || fail "trigger did not start a run"
  "$COALESCE" trigger "$NAME"      # during run -> dirty
  is_state "$NAME" "running dirty" || fail "expected running dirty"
  wait_state "$NAME" idle          || fail "never returned to idle"
  n=$(count "$cnt"); [ "$n" = 2 ]  || fail "expected 2 runs, got $n"
}

# cancel clears a pending dirty bit; the in-flight run is left alone.
check_cancel() {
  NAME=t_cancel; trap 'kill_worker "$NAME"' EXIT
  cnt="$RTDIR/c_cancel"; : > "$cnt"
  "$COALESCE" run "$NAME" -- sh -c 'echo x >>"$1"; sleep 0.5' _ "$cnt" &
  wait_up "$NAME"                  || fail "worker not up"
  "$COALESCE" trigger "$NAME"      || fail "trigger failed"
  wait_state "$NAME" running       || fail "run did not start"
  "$COALESCE" trigger "$NAME"      # dirty
  is_state "$NAME" "running dirty" || fail "expected running dirty"
  "$COALESCE" cancel "$NAME"       # clear dirty
  is_state "$NAME" running         || fail "cancel did not clear dirty"
  wait_state "$NAME" idle          || fail "never returned to idle"
  n=$(count "$cnt"); [ "$n" = 1 ]  || fail "expected 1 run, got $n"
}

# a leftover .sock with no listener is reclaimed by `run`.
check_stale_socket() {
  NAME=t_stale; trap 'kill_worker "$NAME"' EXIT
  : > "$RTDIR/coalesce/$NAME.sock"
  "$COALESCE" run "$NAME" -- true &
  wait_up "$NAME"       || fail "stale socket not reclaimed"
  is_state "$NAME" idle || fail "expected idle"
}

# exec failure on first trigger: worker exits 2, removes socket, logs error.
check_exec_fail() {
  NAME=t_execfail; trap 'kill_worker "$NAME"' EXIT
  "$COALESCE" run "$NAME" -- /no/such/binary 2>"$RTDIR/e_execfail" &
  wpid=$!
  wait_up "$NAME"             || fail "worker not up"
  "$COALESCE" trigger "$NAME" # attempts exec -> fails
  wait_gone "$NAME"           || fail "socket should be removed after exec fail"
  wait "$wpid"; rc=$?
  [ "$rc" = 2 ]               || fail "expected exit 2, got $rc"
  grep -q "exec '/no/such/binary' failed" "$RTDIR/e_execfail" || fail "missing exec error"
}

# a second `run` against a live worker is rejected with exit 1 + message.
check_double_run() {
  NAME=t_dbl; trap 'kill_worker "$NAME"' EXIT
  "$COALESCE" run "$NAME" -- sleep 30 &
  wait_up "$NAME" || fail "worker not up"
  "$COALESCE" run "$NAME" -- sleep 30 2>"$RTDIR/e_dbl"; rc=$?
  [ "$rc" = 1 ]   || fail "expected second run to exit 1, got $rc"
  grep -q "already running" "$RTDIR/e_dbl" || fail "missing 'already running' message"
}

# poke spawns a worker and triggers it; a second poke reuses the worker.
check_poke() {
  NAME=t_poke; trap 'kill_worker "$NAME"' EXIT
  cnt="$RTDIR/c_poke"; : > "$cnt"
  "$COALESCE" poke "$NAME" -- sh -c 'echo x >>"$1"; sleep 0.6' _ "$cnt" || fail "first poke failed"
  wait_up "$NAME"                 || fail "poke did not spawn a worker"
  "$COALESCE" poke "$NAME" -- sh -c 'echo x >>"$1"; sleep 0.6' _ "$cnt" || fail "second poke failed"
  wait_state "$NAME" idle         || fail "never returned to idle"
  n=$(count "$cnt"); [ "$n" = 2 ] || fail "expected 2 runs, got $n"
}

# 20 triggers fired during a single run collapse into exactly one follow-up.
check_stress() {
  NAME=t_stress; trap 'kill_worker "$NAME"' EXIT
  cnt="$RTDIR/c_stress"; : > "$cnt"
  "$COALESCE" run "$NAME" -- sh -c 'echo x >>"$1"; sleep 0.6' _ "$cnt" &
  wait_up "$NAME"             || fail "worker not up"
  "$COALESCE" trigger "$NAME" # start run 1
  wait_state "$NAME" running  || fail "run did not start"
  i=0
  while [ "$i" -lt 20 ]; do
    "$COALESCE" trigger "$NAME" >/dev/null 2>&1
    i=$((i+1))
  done
  wait_state "$NAME" idle         || fail "never returned to idle"
  n=$(count "$cnt"); [ "$n" = 2 ] || fail "expected 2 runs, got $n"
}

# SIGTERM to a busy worker terminates the in-flight command and exits cleanly.
check_sigterm() {
  NAME=t_sigterm; trap 'kill_worker "$NAME"' EXIT
  m="$RTDIR/m_sigterm"; : > "$m"
  "$COALESCE" run "$NAME" -- sh -c 'echo started >>"$1"; sleep 2; echo done >>"$1"' _ "$m" &
  wpid=$!
  wait_up "$NAME"             || fail "worker not up"
  "$COALESCE" trigger "$NAME" || fail "trigger failed"
  wait_grep started "$m"      || fail "command never started"
  kill -TERM "$wpid"          # signal the worker mid-run
  wait "$wpid"; rc=$?
  [ "$rc" = 0 ]               || fail "worker should exit 0 on SIGTERM, got $rc"
  wait_gone "$NAME"           || fail "socket should be removed on shutdown"
  grep -q done "$m"           && fail "in-flight command should have been terminated"
  return 0
}

# while a worker is live, poke delivers only a trigger -- its command is ignored.
check_poke_ignores_cmd() {
  NAME=t_pokeign; trap 'kill_worker "$NAME"' EXIT
  a="$RTDIR/a_pokeign"; b="$RTDIR/b_pokeign"; : > "$a"; : > "$b"
  "$COALESCE" poke "$NAME" -- sh -c 'echo A >>"$1"; sleep 0.5' _ "$a" || fail "first poke failed"
  wait_up "$NAME"            || fail "poke did not spawn a worker"
  wait_state "$NAME" running || fail "command A did not start"
  "$COALESCE" poke "$NAME" -- sh -c 'echo B >>"$1"; sleep 0.1' _ "$b" || fail "second poke failed"
  wait_state "$NAME" idle    || fail "never returned to idle"
  grep -q A "$a"             || fail "command A should have run"
  [ -s "$b" ]                && fail "command B should have been ignored"
  return 0
}

# status/trigger against a name with no worker fail with exit 1 + message.
check_no_worker() {
  "$COALESCE" status t_noworker 2>"$RTDIR/e_nw"; rc=$?
  [ "$rc" = 1 ] || fail "expected status exit 1, got $rc"
  grep -q "no worker" "$RTDIR/e_nw" || fail "missing 'no worker' message"
  "$COALESCE" trigger t_noworker 2>/dev/null; rc=$?
  [ "$rc" = 1 ] || fail "trigger should fail without a worker"
}

# an invalid name is rejected before any socket work, with exit 2.
check_invalid_name() {
  "$COALESCE" status 'bad name' 2>"$RTDIR/e_inv"; rc=$?
  [ "$rc" = 2 ] || fail "expected exit 2, got $rc"
  grep -q "invalid name" "$RTDIR/e_inv" || fail "missing 'invalid name' message"
}

# malformed invocations are rejected with usage (exit 2), before any I/O.
check_usage() {
  "$COALESCE" >/dev/null 2>&1;            [ $? = 2 ] || fail "no args should exit 2"
  "$COALESCE" bogus >/dev/null 2>&1;      [ $? = 2 ] || fail "unknown subcommand should exit 2"
  "$COALESCE" run foo >/dev/null 2>&1;    [ $? = 2 ] || fail "run without -- should exit 2"
  "$COALESCE" run foo -- >/dev/null 2>&1; [ $? = 2 ] || fail "run without a command should exit 2"
  "$COALESCE" trigger >/dev/null 2>&1;    [ $? = 2 ] || fail "trigger without a name should exit 2"
}

# --- run ---
# All checks launch at once and run concurrently; each is independent and
# bounded by its own watchdog, so the whole suite finishes in about one
# TIMEOUT regardless of how many checks hang.

launch_check basic_state    check_basic_state
launch_check cancel         check_cancel
launch_check stale_socket   check_stale_socket
launch_check exec_fail      check_exec_fail
launch_check double_run     check_double_run
launch_check poke           check_poke
launch_check stress         check_stress
launch_check sigterm        check_sigterm
launch_check poke_ignores   check_poke_ignores_cmd
launch_check no_worker      check_no_worker
launch_check invalid_name   check_invalid_name
launch_check usage          check_usage

wait                  # every launcher exits once its check + watchdog finish
cleanup_leftovers     # reap any worker abandoned by a timed-out check
tally

echo
echo "$PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
