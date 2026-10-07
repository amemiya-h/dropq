---
name: dropq
description: Run commands and scripts on the user's own machine (e.g. their GPU desktop) through a dropq job inbox. Use when a connected folder contains a `_jobs/` inbox with `status.json`, or the user mentions dropq, their job server, or running work on their machine.
---

# dropq: using the user's machine as a job server

dropq is a file-based job server. A server process on the user's machine watches each
registered project's inbox (`<project>/_jobs/`) and runs the job files that appear in
`queue/`. You submit work by writing a file and read the result from files — no shell on that
machine is needed, only file access to the project folder.

Jobs run on the user's real machine (often Windows, with their Python environment and GPU),
not in your sandbox. Treat it like running commands on their computer: useful, but not free.

## Inbox layout

```
<project>/_jobs/
  status.json        heartbeat: alive_at, this project's running/queued jobs, gpu, rules
  queue/ID.json      submit here (write ID.json.tmp, then rename to ID.json)
  running/ID.json    in progress
  done/ID.json       result: status, returncode, error, duration_s, log_tail (last 40 lines)
  logs/ID.log        full stdout + stderr
  cancel/ID          create this (empty) file to cancel a job
```

## Workflow

1. **Check the server.** Read `_jobs/status.json`. If it has `stopped_at`, or `alive_at` is more than ~30 s old, the
   server (or the machine) is off: tell the user — jobs would just wait in the queue.
   Note `rules` (what this project may run) and `gpu`.
2. **Submit.** Write the job as `queue/<ID>.json.tmp`, then rename it to `queue/<ID>.json`
   (never write the final name directly — the server may read half a file).
3. **Wait.** Poll `done/<ID>.json` every few seconds. While it runs, `logs/<ID>.log` grows.
4. **Read.** Check `status` and `returncode`; read `log_tail`, or the full log if needed.
   Then decide the next step and submit again — the loop closes without the user relaying.

If `scripts/dq.py` is available next to this skill, it does all of this:
`python dq.py <project>/_jobs status`, `submit --wait 600 -- python train.py`,
`wait ID`, `show ID`, `list`, `cancel ID`.

## Job file

```json
{
  "cmd": ["python", "analyze.py", "--n", "20"],
  "cwd": "experiments",
  "timeout_s": 3600,
  "background": false,
  "name": "analyze",
  "note": "submitted by Claude: balance check for n=20"
}
```

- **ID**: letters, digits, `_ . -` only, unique, e.g. `20261007-175645-analyze-a1b2`
  (timestamp, short name, 4 random hex chars).
- **cmd**: a list of arguments. `python` / `python3` / `py` means the server's own Python
  (the one with the user's packages). Other programs are looked up on the server's PATH;
  relative paths like `scripts/run.bat` must be inside the project.
- **cwd**: relative to the project folder; must stay inside it.
- **timeout_s**: default 6 h; `null` for none (the project may cap it).
- **background**: `true` starts it at once without waiting for the queue. Use it for long runs
  (training); ordinary jobs run one at a time across all projects so they don't fight over
  the GPU.
- `shell: true` (cmd as a string) and `env` (extra environment variables) only work if the
  project's rules allow them — check `rules` in status.json first.
- Always add a `note` saying what the job is for, so the user can follow along.

## Results

`done/<ID>.json` → `status` is one of:

| status | meaning |
|---|---|
| `done` | exited with 0 |
| `failed` | non-zero exit, or **rejected** before starting (then `error` says why and `returncode` is null) |
| `timeout` | killed after `timeout_s` |
| `cancelled` | cancelled via `cancel/ID` |
| `interrupted` | the server stopped while it ran |

A rejection (`error` like "program not allowed", "outside the project", "shell jobs are not
allowed") is the project's policy, set by the user. Report it; don't try to get around it with
a different program, path or encoding.

## Practical tips

- **Prefer script files over long one-liners.** Write a small script into the project folder
  (you have file access) and run `["python", "that_script.py"]`. Quoting is simpler and the
  user can see what ran.
- **The machine is probably Windows.** Paths use backslashes, and there is no shell unless the
  rules allow it. For system information, call a program directly, e.g.
  `["powershell", "-NoProfile", "-Command", "..."]` (if allowed) or a Python script.
- **Check the environment once** at the start of a session (Python version, packages, GPU):
  submit a tiny Python script that prints them.
- **Long jobs:** submit with `"background": true, "timeout_s": null`, report the job ID to the
  user, and check back by reading `status.json` and the log tail rather than blocking.
- **Results to keep** (CSV, plots, checkpoints) should be written by the job into the project
  folder, where both you and the user can read them.
- `status.json` and result files are rewritten atomically but may briefly fail to open on
  Windows; retry a read once before concluding anything.

## Etiquette

- The queue and GPU are shared with the user's own work: don't flood it, and keep jobs to what
  the task needs.
- Ask before anything destructive or hard to undo (deleting files, overwriting results,
  installing or upgrading packages, long GPU runs the user didn't ask for).
- Never write into other projects' inboxes or into the server's own `_dropq/` folder, and
  never edit `status.json` or `done/` files — they are the server's.
- Cancel jobs you started that are no longer needed.
