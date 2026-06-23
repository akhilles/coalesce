# coalesce

`coalesce` runs a command in response to external triggers, collapsing many
"something changed" signals into the minimum number of serialized
reconciliation runs.

A trigger means: the world may have changed, please reconcile. The command
should be a reconciliation task (deploy, rebuild, sync), not a per-event
handler. No payloads are delivered; the command reconciles against durable
external state.

Guarantees:

- at most one run at a time
- triggers during a run collapse into one pending follow-up
- no unbounded queue

## Commands

    coalesce run NAME -- COMMAND [ARG...]    start the worker loop
    coalesce trigger NAME                    signal the worker
    coalesce poke NAME -- COMMAND [ARG...]   ensure the worker exists, then trigger
    coalesce status NAME                     print state: idle | running | running dirty
    coalesce cancel NAME                     clear the pending follow-up

`run` is a long-lived worker. `trigger` requires it to be running; `poke`
spawns it if not, then triggers. This keeps the command (one place) decoupled
from the sources of triggers (webhooks, file watchers, timers).

## Examples

Worker under systemd:

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

Introspection:

    $ coalesce status deploy
    running dirty
    $ coalesce cancel deploy

## Sockets

Per-worker sockets live at `$XDG_RUNTIME_DIR/coalesce/NAME.sock` (root: `/run`;
otherwise `/tmp/coalesce-$UID`). Stale sockets from a killed worker are
reclaimed automatically.

## Scope

Not a job queue, scheduler, retry daemon, pub/sub system, lock manager,
workflow engine, or per-event delivery mechanism. It is only a named,
coalescing, edge-triggered runner. Retry policy is out of scope; use a
supervisor or the command itself.

See coalesce(1) for full semantics.
