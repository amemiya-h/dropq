# dropq

**Drop a job file in a folder; the machine runs it.**

dropq turns any computer — typically a desktop with a GPU — into a small job server.
You submit work by writing a JSON file into a project's inbox folder: from an SSH session,
from a script, or from an AI assistant that can access that folder. The server runs jobs one
at a time (or in the background), streams output to log files, and writes a result file you
can read back.

- **One file, standard library only.** Python 3.8+, no dependencies, no open ports.
- **Projects with their own inbox and rules.** Each project can only run jobs inside its own
  folder; server state and rules live where projects can't change them.
- **Survives disconnects.** Jobs are started by the server, not by your SSH session, so closing
  the connection doesn't kill a 10-hour training run (a common problem on Windows).
- **Plain files as the API.** Anything that can write a file can submit a job and read the result.
- **Windows, Linux and macOS**, with one-command autostart at login.

## Install

```
pip install git+https://github.com/amemiya-h/dropq
```

or copy `dropq.py` into `<root>/_dropq/` and use `python dropq.py ...` instead of `dropq ...`.

## Quick start

The *root* is a workspace folder, e.g. `D:\Misc` or `~/work`. Set it once with `DROPQ_ROOT`
(or pass `--root` to every command).

```
dropq init myproject                 # create (or register) ROOT/myproject with its own inbox
dropq install                        # start the server at login; prints how to start it now
dropq status                         # ALIVE, GPU usage, running and queued jobs
```

Submitting, from anywhere (`-p` can be left out when you're inside the project folder):

```
dropq submit -p myproject -- python train.py --epochs 10
dropq submit -p myproject --background --timeout 0 --name train -- python train.py --resume
dropq submit -p myproject --follow -- python evaluate.py      # stream output until it ends
dropq list -p myproject
dropq show 20261007-165111-train-9150
dropq follow 20261007-165111-train-9150
dropq cancel 20261007-165111-train-9150
```

Over SSH (for example through [Tailscale](https://tailscale.com)):

```
ssh desktop dropq submit -p myproject --background -- python train.py
ssh desktop dropq status
```

## Layout

```
ROOT/
  _dropq/                 server-owned
    projects.json         registered projects and their rules
    status.json           heartbeat for the whole server
    server.log            what the server did
  myproject/
    _jobs/                the project's inbox
      queue/ID.json       ← write here to submit
      running/ID.json     in progress
      done/ID.json        results: status, return code, times, last 40 log lines
      logs/ID.log         full stdout + stderr
      cancel/ID           create to cancel a job
      status.json         heartbeat as this project sees it (only its own jobs)
    ...your files...
```

A job file:

```json
{
  "cmd": ["python", "train.py", "--epochs", "10"],
  "cwd": "experiments",
  "timeout_s": 21600,
  "background": false,
  "name": "train",
  "note": "baseline run"
}
```

- `cmd` is a list of arguments, or a string with `"shell": true` (if the project allows it).
- `cwd` is relative to the project folder and must stay inside it.
- `timeout_s` defaults to 6 hours; `null` means none (subject to the project's cap).
- Foreground jobs run one at a time across all projects, in submission order. `background`
  jobs start immediately and don't hold up the queue — use them for long runs.
- `env` adds environment variables (if the project allows it).

Write the file as `ID.json.tmp` and rename it to `ID.json`, so the server never reads half a
file (`dropq submit` does this). IDs use letters, digits, `_`, `.` and `-`. The result appears
in `done/ID.json` with `status` one of `done`, `failed`, `timeout`, `cancelled`, `interrupted`.

## Projects and rules

Only folders registered with `dropq init` are served. Creating a `_jobs/queue` folder somewhere
else under the root does nothing, and a project's own files can't register new projects or
change rules — those live in `_dropq/`, which only the owner needs to touch.

```
dropq init NAME                      defaults: no shell, no custom env, background allowed, any program
dropq init NAME --commands python    only allow these programs
dropq init NAME --shell --env        allow shell jobs and custom environment variables
dropq init NAME --no-background --max-timeout 3600 --max-queued 20
dropq disable NAME / enable NAME     pause / resume a project (its queue is kept)
dropq projects                       list projects and their rules
```

Running `init` again on an existing project updates only the rules you pass.

Programs are resolved by the server: `python`/`python3`/`py` means the server's own Python,
other names are looked up on the server's `PATH` (never inside the project, so a file in the
project can't stand in for `git`), and paths like `./run.sh` must point inside the project.

## Using it with an AI assistant

Give the assistant access to one project folder only. It writes a job file into that project's
inbox, waits for `done/ID.json`, reads the result, and submits the next job — using your
machine as a compute server without you relaying anything, and without being able to touch
other projects' queues or the server's rules.

[`skill/dropq/`](skill/dropq) is a ready-made skill that teaches Claude this workflow:
`SKILL.md` covers the inbox format, the submit–wait–read loop, the rules and etiquette, and
`scripts/dq.py` is a small helper that drives an inbox with nothing but file access
(`python dq.py <project> submit --wait 600 -- python train.py`). To use it with Claude, zip the
`skill/dropq` folder and add it as a skill, or copy it into your skills directory.

## Security

dropq decides **which** jobs start, **where** and **how**. It is **not a sandbox**: a job runs
with the permissions of the user running the server, and the code it runs can read and write
anything that user can — including other projects. The project boundary protects against
mix-ups and against tools that only have access to one folder; it doesn't contain malicious
code.

- Give a project's inbox only to people and tools you would let run code as that user.
- Don't put the root inside a folder others can write to (shared or synced folders).
- Run the server under a normal, non-administrator account.
- For untrusted code, use a separate OS account, a VM or a container.

## Development

```
python -m pytest -q
```

The tests start a real server on a temporary folder and drive it through the CLI, including
the isolation rules.

## License

MIT
