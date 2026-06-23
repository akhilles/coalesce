# coalesce

Debounce for shell commands: collapse a burst of triggers into the fewest
serialized, non-overlapping runs.

- at most one run at a time
- triggers during a run collapse into a single pending follow-up
- no unbounded queue

A trigger means "the world may have changed; reconcile." The command should be
a reconciliation task — deploy, rebuild, sync — not a per-event handler: no
payload is delivered, and it reconciles against durable external state.

## Commands

    coalesce run NAME -- COMMAND [ARG...]    start the worker loop
    coalesce trigger NAME                    signal the worker
    coalesce poke NAME -- COMMAND [ARG...]   start the worker if needed, then signal
    coalesce status NAME                     print state: idle | running | running dirty
    coalesce cancel NAME                     clear the pending follow-up

`run` is a long-lived worker and `trigger` requires it; `poke` starts one if
absent, then signals. This decouples the command from its trigger sources —
webhooks, file watchers, timers.

## Examples

systemd worker:

    [Service]
    Type=exec
    Restart=always
    ExecStart=/usr/local/bin/coalesce run deploy -- /srv/app/deploy-latest

Webhook:

    coalesce trigger deploy

File watcher:

    while inotifywait -e close_write -r src; do
        coalesce poke build -- make
    done

Each worker listens on `$XDG_RUNTIME_DIR/coalesce/NAME.sock` (`/run` for root,
`/tmp/coalesce-$UID` otherwise); stale sockets are reclaimed on the next `run`
or `poke`.

`coalesce` is not a job queue, scheduler, retry daemon, pub/sub system, lock
manager, or workflow engine — only a named, coalescing, edge-triggered runner.
Retry policy is out of scope; use a supervisor or the command itself.

See `coalesce(1)` for full semantics. Licensed under 0BSD.
