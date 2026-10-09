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
- **Remote machines over ssh.** Submit on a laptop, run on the desktop at home: dropq on both
  ends relays jobs, logs and results over one ssh connection, set up with a key file you carry
  over on a USB stick ([below](#running-jobs-on-another-machine)).

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
dropq monitor                        # live view, refreshed every 2 s (Ctrl+C to quit)
```

`dropq monitor` is a `top`-style view that works in any terminal, including over SSH:

```
dropq 0.4.0 monitor  D:\Misc                                                  22:03:17
server  ALIVE  up 3h12m   pid 5828   projects 2 (1 disabled: lens)
host    up 5d02h   load 1.92 1.40 1.11   312 procs   16 cpus
cpu     [##########............]  46%   cores |@*#=-..:@%#+:. .|
mem     [#########.............]  41%  13.1G/31.7G   swap 0.2G/4.0G
disk    [##########............]  44%  412.3G/931.5G   read 12.3M/s  write 1.1M/s
net     down 1.2M/s   up 230.4K/s
gpu     [#####################.]  97%  NVIDIA GeForce RTX 4060   vram 3.1/8.0G (39%)   64C

RUNNING (2)
 > bg Othogo/20261007-190314-train-a314  3h02m   cpu 104%  mem 2.3G  vram 2.6G
       | it   210 |   48s | games  245 | loss p 2.114 v 0.412 s 0.301
 >    Othogo/20261007-220201-analyze-0d40  1m16s  4h58m left   cpu 99%  mem 1.1G  vram 0.4G
       | 20  28 |   0.47   0.41  0.12 |   +0.81 ± 0.39 | -0.8

QUEUED (1)
  Othogo/20261007-220314-next-936c  waiting 1m02s

RECENT
  done        Othogo/20261007-215502-eval-5bd7  6m12s  rc=0  1m20s ago
  failed      Othogo/20261007-214901-plot-f517  2s  rc=1  14m ago
```

It shows the machine (uptime, load, CPU per core, memory, swap, disk space and I/O, network),
the GPU, each running job with its own CPU, memory and GPU memory (summed over any processes it
starts) and the latest line of its output, how long queued jobs have waited, and recent
results. The host figures describe the machine the monitor runs on — over SSH, the server.

`--once` prints a single frame (handy in scripts), `-i` sets the refresh interval, and
`NO_COLOR=1` turns colours off.

dropq itself needs nothing beyond the standard library. For the full set of figures on every
system, install [psutil](https://pypi.org/project/psutil/) too:

```
pip install "dropq[monitor] @ git+https://github.com/amemiya-h/dropq"
```

Without psutil the monitor shows what the operating system offers directly: everything on
Linux; CPU, memory and disk space on Windows; load and disk space on macOS. Per-job GPU memory
needs a driver that reports it (on Windows it often doesn't).

## Layout

```
ROOT/
  _dropq/                 server-owned
    projects.json         registered projects and their rules
    status.json           heartbeat for the whole server
    server.log            what the server did
    clients.json          machines paired with `dropq pair` (on B)
    links/NAME/           link to another machine: key, pinned host key, state (on A)
  myproject/
    _jobs/                the project's inbox
      queue/ID.json       ← write here to submit
      running/ID.json     in progress
      done/ID.json        results: status, return code, times, last 40 log lines
      logs/ID.log         full stdout + stderr
      cancel/ID           create to cancel a job
      status.json         heartbeat as this project sees it (only its own jobs)
      artifacts/ID/       files copied back over a link (on A)
    _src/                 checkouts for commit jobs
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
- `commit` runs the job in a checkout of the project's git repo at that commit (see below).
- `not_after` (an ISO date-time) — don't start the job after this time; it fails as expired.
- `artifacts` — globs, relative to the job's folder, of files a [link](#running-jobs-on-another-machine)
  copies back when the job ends.

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

### Jobs at a commit

Give a project a git repo and jobs can name a commit to run at:

```
dropq init NAME --repo https://github.com/you/project      # on the machine that runs the jobs
dropq submit -p NAME --commit 3f2a9c1e... -- python train.py
```

The server fetches the commit into `NAME/_src/` and runs the job in a worktree for it
(`NAME/_src/<first 12 hex digits>/`, reused by later jobs at the same commit), so every run is
tied to exact code. Only the owner sets the repo; a job can only pick the commit. Fetching uses
the server user's own git credentials, e.g. a read-only deploy key for a private repo. Use the
full hash when you can: then only that commit is fetched.

## Running jobs on another machine

Run dropq on both machines. On **B** (the one with the GPU) projects work as usual. On **A**
(say a laptop), a *link* makes some projects remote: jobs written into their inbox are sent to
B, and B's status, running jobs, logs, results and artifacts come back into the same inbox.
Anything that drives an inbox — `dropq submit`, the skill's `dq.py`, an AI assistant with
access to the folder — works on A unchanged.

The link is a single ssh connection that A keeps open (reconnecting with backoff). B keeps the
queue and the rules: if the link drops, running jobs carry on, new jobs wait on A, and
everything catches up when it's back. Code travels through git (see `commit` above), not the link.

**On B** — needs an ssh server (on Windows: *Settings → System → Optional features → OpenSSH
Server*, then `Start-Service sshd`):

```
dropq init lens                       # as usual (add --repo URL for commit jobs)
dropq install                         # B's server must be running to run anything
dropq pair laptop -p lens             # writes laptop.dropq and prints a one-time code
```

**On A** — needs the OpenSSH client (built into Windows 10+, macOS and most Linux):

```
dropq link laptop.dropq               # asks for the code; then deletes the file
dropq install                         # A's server runs the relay
dropq links                           # UP / DOWN, last contact, last error
```

A has to be able to reach B: on the same network, through a port forward, or — easiest — with
[Tailscale](https://tailscale.com) on both (`dropq pair` uses B's Tailscale name when it has one;
override with `--host`, `--user`, `--port` on either side).

How the key works:

- `dropq pair` makes a new key on B, adds it to B's `authorized_keys` locked to
  `restrict,command="…agent.sh laptop"`, and writes it into the bundle encrypted with the code it
  prints. So access starts with someone at B's keyboard, and a lost USB stick without the code is
  useless. The bundle also pins B's host key, so A never has to trust anything on first connect.
- The key can do nothing but talk to `dropq agent`: no shell, no port forwarding, and only the
  projects named with `-p`. The agent writes jobs into B's inbox, and B's own server decides
  whether they run, by B's rules.
- `dropq keys` lists paired machines; `dropq keys revoke laptop` cuts one off at once (even
  mid-connection). `--days N` makes a key expire. On A, `dropq unlink laptop` forgets the link.
- On Windows, if your account is an administrator, sshd reads
  `C:\ProgramData\ssh\administrators_authorized_keys` instead of your own `authorized_keys`;
  `dropq pair` writes the right one (run it from an Administrator terminal in that case).

On A, a remote project's `status.json` has a `remote` field (link up or down, last contact,
last error, jobs waiting to be sent), and its `alive_at` is the last time A heard from a live
server on B — so a dead link and a stopped server both look like a stale heartbeat. Artifacts
land in `_jobs/artifacts/<ID>/` on A (up to 500 MB per job; `max_artifact_mb` in
`_dropq/links/<name>/link.json`).

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
the isolation rules. The link tests run two servers on two folders and pair them for real
(with `ssh-keygen`), but connect them through a local process instead of ssh.

## License

MIT
