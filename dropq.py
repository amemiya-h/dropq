#!/usr/bin/env python3
"""
dropq - a tiny file-based job server. Drop a job file in a folder; the machine runs it.

Turns a computer (e.g. a desktop with a GPU) into a job server you can submit to from anywhere
that can write into a project's inbox folder: over SSH, from a synced folder, or from an AI
assistant with access to that folder. Standard library only, no open ports.

ROOT is the workspace. Every project is a registered folder directly under it, with its own
inbox; jobs from a project run inside that project. Server state and the per-project rules live
in ROOT/_dropq, which projects never need access to:

  ROOT/
    _dropq/                 server-owned: projects.json (registry + rules), status.json, server.log
    myproject/
      _jobs/                the project's inbox
        queue/ID.json       submit by writing here (as .tmp, then rename)
        running/ done/      in progress / results (status, return code, times, log tail)
        logs/ID.log         stdout + stderr
        cancel/ID           create to cancel
        status.json         heartbeat as seen by this project (its own jobs only)
      ... the project's files ...

Folders that are not registered in _dropq/projects.json are ignored, so writing an inbox into
some folder (or another project) does nothing until the owner runs `dropq init`.

ROOT comes from --root, else $DROPQ_ROOT, else - when this file sits in ROOT/_dropq - its parent,
else the current directory.

  dropq init NAME [rules]              create/register project NAME (or update its rules)
      --shell/--no-shell  --env/--no-env  --background/--no-background
      --commands python git ...  (allowed programs; --any-command to lift)
      --max-timeout SEC   --max-queued N
  dropq enable NAME / disable NAME     disabled projects keep their queue but nothing starts
  dropq projects                       list projects and rules
  dropq serve                          run the server in the foreground (Ctrl+C to stop)
  dropq install / uninstall            start the server at login (Windows/Linux/macOS)
  dropq submit [-p NAME] [opts] -- CMD ...   queue a job; prints its id
      --cwd DIR (inside the project)  --background  --timeout SEC (0 = none)
      --name/--note  --shell  --follow (stream output until it ends)
  dropq status                         server heartbeat, GPU, running and queued jobs
  dropq monitor [-i SEC] [--once]      live view: server, GPU, running jobs with their latest
                                       output line, queue, recent results (Ctrl+C to quit)
  dropq list [-p NAME] [-n 20]         recent finished jobs
  dropq show ID / follow ID / cancel ID

Remote machines (run dropq on both; jobs submitted on A run on B, over ssh):
  on B:  dropq pair NAME -p PROJECT ...    make a key for endpoint NAME; writes NAME.dropq
                                           (carry it to A) and prints a code to type there
         dropq keys / keys revoke NAME     list / revoke endpoint keys
  on A:  dropq link FILE.dropq             import a bundle; its projects become remote
         dropq links / unlink NAME         list links and their state / remove one
  dropq agent --client NAME            the ssh side of a link (run by sshd, not by hand)

-p defaults to the project the current directory is in.

Job file fields: cmd (list, or string with "shell": true), cwd (relative to the project),
timeout_s (default 6 h; null = none), background, env (dict), name, note,
commit (run in a checkout of the project's repo at that commit; see init --repo),
not_after (ISO time; don't start after it), artifacts (globs a link copies back to A).
Foreground jobs run one at a time across all projects, in submission order; background jobs
start immediately. "python"/"python3"/"py" means the Python running the server; other programs
are resolved on the server's PATH (or must be a path inside the project), never on the job's.

Security: dropq controls which jobs start, where and how - it is not a sandbox. A job runs with
the permissions of the user running the server and its code can touch anything that user can.
Give a project's inbox only to people and tools you would let run code as that user; for
untrusted code use a separate OS account, a VM or a container.
"""
import argparse, base64, datetime as dt, getpass, glob, json, os, re, secrets, shutil, signal, socket
import subprocess, sys, tempfile, threading, time, uuid

__version__ = "0.5.0"
WIN = os.name == "nt"
POLL = float(os.environ.get("DROPQ_POLL", "2"))   # seconds between server checks
DEFAULT_TIMEOUT = 6 * 3600
MAX_JOB_FILE = 64 * 1024
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
INBOX_DIRS = ("queue", "running", "done", "logs", "cancel")
DEFAULT_RULES = dict(enabled=True, allow_shell=False, allow_env=False, allow_background=True,
                     commands=None, max_timeout_s=None, max_queued=100, repo=None, remote=None)
SHA_RE = re.compile(r"^[0-9a-fA-F]{7,64}$")
NOWIN = subprocess.CREATE_NO_WINDOW if WIN else 0

ROOT = SRV = STATUS = LOCK = SERVER_LOG = REGISTRY = CLIENTS = LINKS = None


def configure(root=None):
    global ROOT, SRV, STATUS, LOCK, SERVER_LOG, REGISTRY, CLIENTS, LINKS
    if root is None:
        root = os.environ.get("DROPQ_ROOT")
    if root is None:
        mod_dir = os.path.dirname(os.path.abspath(__file__))
        root = os.path.dirname(mod_dir) if os.path.basename(mod_dir) == "_dropq" else os.getcwd()
    ROOT = os.path.realpath(os.path.expanduser(root))
    SRV = os.path.join(ROOT, "_dropq")
    STATUS, LOCK = os.path.join(SRV, "status.json"), os.path.join(SRV, "server.lock")
    SERVER_LOG, REGISTRY = os.path.join(SRV, "server.log"), os.path.join(SRV, "projects.json")
    CLIENTS, LINKS = os.path.join(SRV, "clients.json"), os.path.join(SRV, "links")


def inbox(project, sub=None):
    base = os.path.join(ROOT, project, "_jobs")
    return base if sub is None else os.path.join(base, sub)


def now():
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def write_json(path, obj, tries=10):
    """Atomic write. Retries because Windows refuses to replace a file someone has open."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=1, ensure_ascii=False)
    for i in range(tries):
        try:
            os.replace(tmp, path); return
        except PermissionError:
            if i == tries - 1:
                raise
            time.sleep(0.3)


def read_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def parse_time(s):
    s = str(s)
    t = dt.datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
    return t if t.tzinfo else t.astimezone()


def say(msg):
    line = f"[{now()}] {msg}"
    try:
        with open(SERVER_LOG, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass
    if sys.stdout is not None:
        print(line, flush=True)


def tail(path, n):
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2); size = fh.tell()
            fh.seek(max(0, size - 64 * 1024))
            return fh.read().decode("utf-8", "replace").splitlines()[-n:]
    except OSError:
        return []


def inside(path, base):
    path, base = os.path.realpath(path), os.path.realpath(base)
    try:
        return os.path.commonpath([path, base]) == base
    except ValueError:          # different drives on Windows
        return False


# ------------------------------------------------------------------ project registry
def load_registry():
    try:
        reg = read_json(REGISTRY)
    except (OSError, ValueError):
        return {}
    return {k: dict(DEFAULT_RULES, **v) for k, v in reg.get("projects", {}).items() if NAME_RE.match(k)}


def save_registry(reg):
    os.makedirs(SRV, exist_ok=True)
    write_json(REGISTRY, {"projects": reg})


def make_inbox(project):
    for d in INBOX_DIRS:
        os.makedirs(inbox(project, d), exist_ok=True)


def project_of_cwd():
    cwd = os.path.realpath(os.getcwd())
    if inside(cwd, ROOT) and cwd != ROOT:
        first = os.path.relpath(cwd, ROOT).split(os.sep)[0]
        if first in load_registry():
            return first
    return None


# ------------------------------------------------------------------ single-instance lock
class Lock:
    def __enter__(self):
        os.makedirs(SRV, exist_ok=True)
        self.fh = open(LOCK, "a+")
        try:
            if WIN:
                import msvcrt
                self.fh.seek(0); msvcrt.locking(self.fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            sys.exit(f"another dropq server is already running on {ROOT}")
        return self

    def __exit__(self, *a):
        self.fh.close()


# ------------------------------------------------------------------ server
def gpu_info():
    exe = shutil.which("nvidia-smi")
    if not exe:
        return None
    try:
        out = subprocess.run([exe, "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu",
                              "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=10,
                             creationflags=subprocess.CREATE_NO_WINDOW if WIN else 0).stdout.strip()
        res = []
        for line in out.splitlines():
            name, util, used, tot, temp = [x.strip() for x in line.split(",")]
            res.append(dict(name=name, util_pct=int(util), mem_used_mb=int(used), mem_total_mb=int(tot),
                            temp_c=int(temp)))
        return res
    except Exception as e:  # noqa: BLE001
        return [dict(error=str(e))]


def kill_tree(proc):
    if proc.poll() is not None:
        return
    if WIN:
        subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True,
                       creationflags=subprocess.CREATE_NO_WINDOW)
    else:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            time.sleep(2)
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


class Rejected(Exception):
    pass


def check_job(project, job, rules, base=None):
    """Apply the project's rules. Returns (cwd, cmd, shell, env, timeout, notes).

    base is the folder cwd is relative to: the project, or a commit's checkout inside it."""
    if not isinstance(job, dict) or "cmd" not in job:
        raise Rejected("job file needs a 'cmd'")
    pdir = os.path.join(ROOT, project)
    cwd = os.path.join(base or pdir, str(job.get("cwd") or "."))
    if not inside(cwd, pdir):
        raise Rejected(f"cwd {job.get('cwd')!r} is outside the project")
    if not os.path.isdir(cwd):
        raise Rejected(f"cwd {job.get('cwd')!r} does not exist")
    if inside(cwd, inbox(project)):
        raise Rejected("jobs cannot run inside the _jobs inbox")
    shell = bool(job.get("shell"))
    if shell and not rules["allow_shell"]:
        raise Rejected("shell jobs are not allowed for this project")
    if job.get("env") and not rules["allow_env"]:
        raise Rejected("custom environment variables are not allowed for this project")
    if job.get("background") and not rules["allow_background"]:
        raise Rejected("background jobs are not allowed for this project")
    cmd = job["cmd"]
    if shell:
        if not isinstance(cmd, str) or not cmd.split():
            raise Rejected("with shell, cmd must be a non-empty string")
        prog = cmd.split()[0]
    else:
        if not isinstance(cmd, list) or not cmd or not all(isinstance(c, (str, int, float)) for c in cmd):
            raise Rejected("cmd must be a non-empty list of strings")
        cmd = [str(c) for c in cmd]
        prog = cmd[0]
    allowed = rules["commands"]
    base = os.path.basename(prog)
    base = base[:-4] if base.lower().endswith(".exe") else base
    if allowed is not None and base not in allowed:
        raise Rejected(f"program {base!r} is not allowed for this project (allowed: {', '.join(allowed)})")
    if not shell:
        # resolve the program here, so a file in the project can't stand in for a system program
        if prog in ("python", "python3", "py"):
            cmd[0] = sys.executable.replace("pythonw.exe", "python.exe")
        elif os.sep in prog or (os.altsep and os.altsep in prog):
            exe = os.path.join(cwd, prog)
            if not inside(exe, pdir) or not os.path.isfile(exe):
                raise Rejected(f"{prog!r} must be an existing file inside the project")
            cmd[0] = os.path.realpath(exe)
        else:
            exe = shutil.which(prog)
            if not exe or inside(exe, pdir):
                raise Rejected(f"program {prog!r} not found on the server's PATH")
            cmd[0] = exe
    env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
    env.update({str(k): str(v) for k, v in (job.get("env") or {}).items()})
    timeout, notes = job.get("timeout_s", DEFAULT_TIMEOUT), []
    if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0):
        raise Rejected("timeout_s must be a positive number or null")
    mx = rules["max_timeout_s"]
    if mx is not None and (timeout is None or timeout > mx):
        timeout = mx; notes.append(f"timeout capped at {mx}s by project rules")
    return cwd, cmd, shell, env, timeout, notes


class Running:
    def __init__(self, project, jid, job, proc, logf, timeout):
        self.project, self.jid, self.job, self.proc = project, jid, job, proc
        self.logf, self.timeout, self.t0 = logf, timeout, time.time()


def check_before_start(job, rules):
    """Checks that don't need the checkout: not_after, and that a commit job can be served."""
    if job.get("not_after"):
        try:
            limit = parse_time(job["not_after"])
        except (TypeError, ValueError):
            raise Rejected("not_after must be an ISO date-time, e.g. 2026-10-10T09:00:00+09:00")
        if dt.datetime.now().astimezone() > limit:
            raise Rejected(f"expired: not started before not_after ({job['not_after']})")
    commit = job.get("commit")
    if commit is not None:
        if not isinstance(commit, str) or not SHA_RE.match(commit):
            raise Rejected("commit must be a commit hash (7-64 hex digits)")
        if not rules.get("repo"):
            raise Rejected("this project has no repo for commit jobs (owner: dropq init NAME --repo URL)")
        if not shutil.which("git"):
            raise Rejected("commit jobs need git on the server's PATH")


CHECKOUT_LOCK = threading.Lock()


def git_run(args, timeout=900):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="never")
    return subprocess.run([shutil.which("git")] + args, capture_output=True, text=True, timeout=timeout,
                          env=env, stdin=subprocess.DEVNULL, creationflags=NOWIN)


def checkout(project, url, commit):
    """Fetch commit from url into the project's cache and return a worktree at that commit.

    ROOT/PROJECT/_src/.cache.git is a bare repo; each commit gets ROOT/PROJECT/_src/<sha12>/,
    reused by later jobs on the same commit."""
    src = os.path.join(ROOT, project, "_src")
    gd = os.path.join(src, ".cache.git")
    g = ["--git-dir", gd]

    def must(args, what):
        r = git_run(args)
        if r.returncode:
            raise Rejected(f"{what} failed: {(r.stderr or r.stdout).strip()[-500:]}")
        return r.stdout.strip()

    def resolve():
        r = git_run(g + ["rev-parse", "--verify", "--quiet", commit + "^{commit}"])
        return r.stdout.strip() if r.returncode == 0 else None

    with CHECKOUT_LOCK:
        os.makedirs(src, exist_ok=True)
        if not os.path.isdir(gd):
            must(["init", "--quiet", "--bare", gd], "git init")
        full = resolve()
        if not full:
            if len(commit) >= 40:      # servers only hand out a single commit when asked by full hash
                git_run(g + ["fetch", "--quiet", "--depth", "1", url, commit])
                full = resolve()
            if not full:
                must(g + ["fetch", "--quiet", url, "+refs/heads/*:refs/dropq/heads/*",
                          "+refs/tags/*:refs/dropq/tags/*"], f"git fetch {url}")
                full = resolve()
            if not full:
                raise Rejected(f"commit {commit} not found in {url} (pushed yet?)")
        wt = os.path.join(src, full[:12])
        if os.path.isfile(os.path.join(wt, ".git")):
            r = git_run(["-C", wt, "rev-parse", "HEAD"])
            if r.returncode == 0 and r.stdout.strip() == full:
                return wt
            shutil.rmtree(wt, ignore_errors=True)
        elif os.path.exists(wt):
            shutil.rmtree(wt, ignore_errors=True)
        git_run(g + ["worktree", "prune"])
        must(g + ["worktree", "add", "--quiet", "--detach", "--force", wt, full], "git worktree add")
        return wt


class Prep(threading.Thread):
    """Checks out a commit job's source in the background, so the server keeps its heartbeat."""

    def __init__(self, project, jid, job, rules):
        super().__init__(daemon=True)
        self.project, self.jid, self.job, self.rules = project, jid, job, rules
        self.t0, self.base, self.error = time.time(), None, None
        self.start()

    def run(self):
        try:
            self.base = checkout(self.project, self.rules["repo"], self.job["commit"])
        except Rejected as e:
            self.error = str(e)
        except Exception as e:  # noqa: BLE001
            self.error = f"checkout failed: {e}"


def start(project, jid, job, rules, base=None):
    cwd, cmd, shell, env, timeout, notes = check_job(project, job, rules, base)
    job["run_dir"] = os.path.relpath(cwd, os.path.join(ROOT, project))
    logf = open(os.path.join(inbox(project, "logs"), jid + ".log"), "ab")
    logf.write(f"### {now()} start in {os.path.relpath(cwd, ROOT)}\n### {cmd}\n".encode())
    for n in notes:
        logf.write(f"### note: {n}\n".encode())
    logf.flush()
    kw = dict(cwd=cwd, env=env, stdout=logf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, shell=shell)
    if WIN:
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        kw["start_new_session"] = True
    try:
        proc = subprocess.Popen(cmd, **kw)
    except OSError as e:
        logf.close()
        raise Rejected(f"could not start: {e}")
    return Running(project, jid, job, proc, logf, timeout)


def finish(r, status, rc=None):
    r.logf.write(f"\n### {now()} {status} (rc={rc})\n".encode()); r.logf.close()
    log = os.path.join(inbox(r.project, "logs"), r.jid + ".log")
    res = dict(r.job, id=r.jid, project=r.project, status=status, returncode=rc, error=None,
               finished_at=now(), duration_s=round(time.time() - r.t0, 1),
               log=os.path.relpath(log, os.path.join(ROOT, r.project)), log_tail=tail(log, 40))
    write_json(os.path.join(inbox(r.project, "done"), r.jid + ".json"), res)
    for p in (os.path.join(inbox(r.project, "running"), r.jid + ".json"),
              os.path.join(inbox(r.project, "cancel"), r.jid)):
        if os.path.exists(p):
            os.remove(p)
    say(f"{r.project}/{r.jid}: {status} rc={rc} ({res['duration_s']}s)")


def reject(project, jid, job, error, status="failed"):
    job = dict(job if isinstance(job, dict) else {}, id=jid, project=project, status=status,
               error=error, finished_at=now())
    write_json(os.path.join(inbox(project, "done"), jid + ".json"), job)
    say(f"{project}/{jid}: {status} - {error}")


def queued(project):
    """(mtime, project, id) for each queued job file."""
    q, items = inbox(project, "queue"), []
    try:
        names = os.listdir(q)
    except OSError:
        return items
    for f in names:
        if f.endswith(".json"):
            try:
                items.append((os.path.getmtime(os.path.join(q, f)), project, f[:-5]))
            except OSError:
                pass
    return items


def recover(project):
    """Jobs left in running/ belong to a server that died: report them as interrupted."""
    rdir = inbox(project, "running")
    for f in os.listdir(rdir):
        if not f.endswith(".json"):
            continue
        jid = f[:-5]
        try:
            job = read_json(os.path.join(rdir, f))
        except Exception:  # noqa: BLE001
            job = {}
        err = "server stopped while this job was running"
        if isinstance(job, dict) and job.get("pid"):
            err += f"; its process (pid {job['pid']}) may still be running"
        reject(project, jid, job, err, "interrupted")
        os.remove(os.path.join(rdir, f))


def _sigterm(signum, frame):
    raise KeyboardInterrupt


def serve():
    if not WIN:
        signal.signal(signal.SIGTERM, _sigterm)   # systemd/launchd stop -> clean shutdown
    with Lock():
        say(f"dropq {__version__} serving {ROOT} (python {sys.executable})")
        server_started = now()
        recovered, running, preps, relays, gpu, gpu_t = set(), [], [], {}, None, 0.0
        try:
            while True:
                reg = load_registry()
                active = [p for p in reg if not reg[p]["remote"] and os.path.isdir(os.path.join(ROOT, p))]
                for p in active:
                    make_inbox(p)
                    if p not in recovered:
                        recover(p); recovered.add(p)
                # projects linked to another machine are served by a relay per link
                wanted = {reg[p]["remote"] for p in reg if reg[p]["remote"]}
                for name in list(relays):
                    if name not in wanted or not relays[name].is_alive():
                        relays.pop(name).stop()
                for name in wanted - set(relays):
                    if os.path.exists(os.path.join(LINKS, name, "link.json")):
                        relays[name] = Relay(name, [p for p in reg if reg[p]["remote"] == name])
                # finished, cancelled, timed out
                for r in list(running):
                    if os.path.exists(os.path.join(inbox(r.project, "cancel"), r.jid)):
                        kill_tree(r.proc); r.proc.wait(); finish(r, "cancelled", r.proc.returncode)
                    elif r.timeout is not None and time.time() - r.t0 > r.timeout:
                        kill_tree(r.proc); r.proc.wait(); finish(r, "timeout", r.proc.returncode)
                    elif r.proc.poll() is not None:
                        finish(r, "done" if r.proc.returncode == 0 else "failed", r.proc.returncode)
                    else:
                        continue
                    running.remove(r)
                # commit jobs whose checkout finished (or that were cancelled meanwhile)
                for pr in list(preps):
                    rf = os.path.join(inbox(pr.project, "running"), pr.jid + ".json")
                    if os.path.exists(os.path.join(inbox(pr.project, "cancel"), pr.jid)):
                        preps.remove(pr); _remove(rf)
                        reject(pr.project, pr.jid, pr.job, "cancelled while checking out the commit", "cancelled")
                        continue
                    if pr.is_alive():
                        continue
                    preps.remove(pr)
                    if pr.error:
                        _remove(rf); reject(pr.project, pr.jid, pr.job, pr.error); continue
                    pr.job.pop("preparing", None)
                    try:
                        r = start(pr.project, pr.jid, pr.job, pr.rules, pr.base)
                    except Rejected as e:
                        _remove(rf); reject(pr.project, pr.jid, pr.job, str(e)); continue
                    r.t0 = pr.t0
                    pr.job["pid"] = r.proc.pid
                    write_json(rf, pr.job)
                    running.append(r)
                    say(f"{pr.project}/{pr.jid}: started at {pr.job['commit'][:12]} {pr.job.get('cmd')}")
                # cancelled while still queued; clear stray cancel files
                for p in active:
                    cdir = inbox(p, "cancel")
                    for f in os.listdir(cdir):
                        q = os.path.join(inbox(p, "queue"), f + ".json")
                        if os.path.exists(q):
                            os.remove(q); reject(p, f, {}, "cancelled before it started", "cancelled")
                        if not any(r.project == p and r.jid == f for r in running + preps):
                            _remove(os.path.join(cdir, f))
                # start jobs in global submission order; foreground ones one at a time
                busy = any(not r.job.get("background") for r in running + preps)
                seen = {}
                for _, p, jid in sorted(i for p in active for i in queued(p)):
                    rules = reg[p]
                    if not rules["enabled"]:
                        continue
                    qf = os.path.join(inbox(p, "queue"), jid + ".json")
                    seen[p] = seen.get(p, 0) + 1
                    if not ID_RE.match(jid):
                        os.remove(qf)
                        reject(p, re.sub(r"[^A-Za-z0-9_.-]", "_", jid).lstrip("._-")[:100] or "bad-id", {},
                               "invalid job id (use letters, digits, _ . -)")
                        continue
                    if seen[p] > rules["max_queued"]:
                        os.remove(qf); reject(p, jid, {}, f"queue limit ({rules['max_queued']}) exceeded"); continue
                    try:
                        if os.path.getsize(qf) > MAX_JOB_FILE:
                            raise Rejected("job file too large")
                        job = read_json(qf)
                    except Rejected as e:
                        os.remove(qf); reject(p, jid, {}, str(e)); continue
                    except Exception as e:  # noqa: BLE001  (maybe half-written: retry for a minute)
                        if time.time() - os.path.getmtime(qf) > 60:
                            os.remove(qf); reject(p, jid, {}, f"unreadable job file: {e}")
                        continue
                    if not isinstance(job, dict):
                        os.remove(qf); reject(p, jid, {}, "job file must contain a JSON object"); continue
                    bg = bool(job.get("background"))
                    if not bg and busy:
                        continue
                    os.remove(qf)
                    job["started_at"] = now()
                    rf = os.path.join(inbox(p, "running"), jid + ".json")
                    try:
                        check_before_start(job, rules)
                        if job.get("commit") is None:
                            r = start(p, jid, job, rules)
                    except Rejected as e:
                        reject(p, jid, job, str(e)); continue
                    if not bg:
                        busy = True
                    if job.get("commit") is not None:
                        job["preparing"] = True
                        write_json(rf, job)
                        preps.append(Prep(p, jid, job, rules))
                        say(f"{p}/{jid}: checking out {job['commit']}")
                        continue
                    job["pid"] = r.proc.pid
                    write_json(rf, job)
                    running.append(r)
                    say(f"{p}/{jid}: started {'(background) ' if bg else ''}{job.get('cmd')}")
                # heartbeats: a full one for the owner, a project-only one inside each inbox
                if time.time() - gpu_t > 30:
                    gpu, gpu_t = gpu_info(), time.time()
                stamp, fg_busy = now(), any(not r.job.get("background") for r in running + preps)
                jobs = [dict(project=r.project, id=r.jid, name=r.job.get("name"),
                             background=bool(r.job.get("background")), pid=r.proc.pid,
                             elapsed_s=round(time.time() - r.t0), timeout_s=r.timeout,
                             note=r.job.get("note")) for r in running]
                jobs += [dict(project=r.project, id=r.jid, name=r.job.get("name"),
                              background=bool(r.job.get("background")), pid=None, preparing=True,
                              elapsed_s=round(time.time() - r.t0), timeout_s=None,
                              note=r.job.get("note")) for r in preps]
                try:
                    write_json(STATUS, dict(
                        alive_at=stamp, started_at=server_started, pid=os.getpid(), root=ROOT,
                        python=sys.executable, version=__version__, poll_s=POLL, running=jobs,
                        queued=[f"{p}/{j}" for _, p, j in sorted(i for p in active for i in queued(p))],
                        projects={p: dict(enabled=reg[p]["enabled"], remote=reg[p]["remote"]) for p in reg},
                        links={n: rl.summary() for n, rl in relays.items()}, gpu=gpu), tries=1)
                except OSError:
                    pass
                for p in active:
                    try:
                        write_json(os.path.join(inbox(p), "status.json"), dict(
                            alive_at=stamp, project=p, enabled=reg[p]["enabled"], server_busy=fg_busy,
                            running=[dict(id=j["id"], background=j["background"], elapsed_s=j["elapsed_s"],
                                          **({"preparing": True} if j.get("preparing") else {}))
                                     for j in jobs if j["project"] == p],
                            queued=[j for _, _, j in sorted(queued(p))], gpu=gpu,
                            rules={k: v for k, v in reg[p].items() if k not in ("enabled", "remote")}), tries=1)
                    except OSError:
                        pass
                time.sleep(POLL)
        except KeyboardInterrupt:
            say("stopping: cancelling running jobs")
            for rl in relays.values():
                rl.stop()
            for r in running:
                kill_tree(r.proc); r.proc.wait(); finish(r, "interrupted", r.proc.returncode)
            for pr in preps:
                _remove(os.path.join(inbox(pr.project, "running"), pr.jid + ".json"))
                reject(pr.project, pr.jid, pr.job, "server stopped during the checkout", "interrupted")
            stop = now()
            for path in [STATUS] + [os.path.join(inbox(p), "status.json") for p in load_registry()]:
                try:
                    st = read_json(path)
                    st.update(stopped_at=stop, running=[])
                    write_json(path, st)
                except Exception:  # noqa: BLE001
                    pass
            say("stopped")


def _remove(path):
    try:
        os.remove(path)
    except OSError:
        pass


# ------------------------------------------------------------------ owner commands
def init(a):
    if not NAME_RE.match(a.name) or a.name.startswith(("_", ".")):
        sys.exit("project names: letters, digits, _ . - (not starting with _ or .)")
    reg = load_registry()
    if reg.get(a.name, {}).get("remote"):
        sys.exit(f"{a.name} runs on the machine behind link {reg[a.name]['remote']!r}; its rules are set there")
    rules = dict(reg.get(a.name, DEFAULT_RULES))
    for flag, key in (("shell", "allow_shell"), ("env", "allow_env"), ("background", "allow_background")):
        v = getattr(a, flag)
        if v is not None:
            rules[key] = v
    if a.any_command:
        rules["commands"] = None
    elif a.commands:
        rules["commands"] = a.commands
    if a.max_timeout is not None:
        rules["max_timeout_s"] = a.max_timeout or None
    if a.max_queued is not None:
        rules["max_queued"] = a.max_queued
    if a.repo is not None:
        rules["repo"] = a.repo or None
    new = a.name not in reg
    reg[a.name] = rules
    os.makedirs(os.path.join(ROOT, a.name), exist_ok=True)
    make_inbox(a.name)
    save_registry(reg)
    print(f"{'created' if new else 'updated'} project {a.name} at {os.path.join(ROOT, a.name)}")
    print_rules(a.name, rules)


def print_rules(name, r):
    if r.get("remote"):
        print(f"  {name:20s} remote: runs on the machine behind link {r['remote']!r}"); return
    yn = lambda b: "yes" if b else "no"  # noqa: E731
    cmds = "any" if r["commands"] is None else " ".join(r["commands"])
    mt = "none" if r["max_timeout_s"] is None else f"{r['max_timeout_s']}s"
    print(f"  {name:20s} {'enabled ' if r['enabled'] else 'DISABLED'} shell={yn(r['allow_shell'])} "
          f"env={yn(r['allow_env'])} background={yn(r['allow_background'])} programs={cmds} "
          f"max_timeout={mt} max_queued={r['max_queued']}" + (f" repo={r['repo']}" if r.get("repo") else ""))


def set_enabled(a, value):
    reg = load_registry()
    if a.name not in reg:
        sys.exit(f"no project {a.name}")
    reg[a.name]["enabled"] = value
    save_registry(reg)
    print(f"{a.name} {'enabled' if value else 'disabled'}")


def projects(a):
    reg = load_registry()
    if not reg:
        print(f"no projects in {ROOT} yet - create one with `dropq init NAME`"); return
    for name, r in sorted(reg.items()):
        print_rules(name, r)


# ------------------------------------------------------------------ remote: the machine that runs jobs (B)
# A link connects dropq on machine A (where jobs are submitted, e.g. a laptop) to dropq on machine B
# (where they run). B's sshd starts `dropq agent --client NAME` for A's key - nothing else, see
# `pair` - and A's relay talks to it in JSON lines over that one ssh connection. B keeps the queue
# and the rules; the agent only reads and writes inbox files of the projects that key may use.

CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"     # 32 symbols, no 0/O or 1/I
LOG_CHUNK, READ_CHUNK = 256 * 1024, 1024 * 1024


def load_clients():
    try:
        return read_json(CLIENTS).get("clients", {})
    except (OSError, ValueError):
        return {}


def save_clients(clients):
    os.makedirs(SRV, exist_ok=True)
    write_json(CLIENTS, {"clients": clients})


def client_ok(name):
    c = load_clients().get(name)
    if not c:
        return None
    if c.get("expires_at") and dt.datetime.now().astimezone() > parse_time(c["expires_at"]):
        return None
    return c


class AgentError(Exception):
    def __init__(self, msg, fatal=False):
        super().__init__(msg)
        self.fatal = fatal


REVOKED = "this key has been revoked or has expired"


class Agent:
    def __init__(self, client):
        self.client = client

    def project(self, msg):
        p = msg.get("project")
        c = client_ok(self.client)
        if c is None:
            raise AgentError(REVOKED, fatal=True)
        reg = load_registry()
        if p not in c.get("projects", []) or p not in reg or reg[p]["remote"] or not os.path.isdir(inbox(p)):
            raise AgentError(f"project {p!r} is not available to this key")
        return p

    @staticmethod
    def job_id(msg):
        jid = msg.get("id")
        if not isinstance(jid, str) or not ID_RE.match(jid):
            raise AgentError("invalid job id")
        return jid

    def op_hello(self, msg):
        c = client_ok(self.client)
        if c is None:
            raise AgentError(REVOKED, fatal=True)
        reg = load_registry()
        return dict(version=__version__, client=self.client, host=socket.gethostname(), now=now(),
                    projects={p: {k: v for k, v in reg[p].items() if k != "remote"}
                              for p in c.get("projects", []) if p in reg and not reg[p]["remote"]})

    def op_submit(self, msg):
        p, jid, job = self.project(msg), self.job_id(msg), msg.get("job")
        if not isinstance(job, dict):
            raise AgentError("job must be a JSON object")
        job = dict(job, submitted_by=f"link:{self.client}")
        data = json.dumps(job, ensure_ascii=False)
        if len(data.encode()) > MAX_JOB_FILE:
            raise AgentError("job file too large")
        for sub in ("queue", "running", "done"):
            if os.path.exists(os.path.join(inbox(p, sub), jid + ".json")):
                return dict(dup=True)       # sent before, e.g. just before the connection dropped
        write_json(os.path.join(inbox(p, "queue"), jid + ".json"), job)
        return dict(dup=False)

    def op_cancel(self, msg):
        p, jid = self.project(msg), self.job_id(msg)
        if not any(os.path.exists(os.path.join(inbox(p, s), jid + ".json")) for s in ("queue", "running")):
            return dict(found=False)
        open(os.path.join(inbox(p, "cancel"), jid), "w").close()
        return dict(found=True)

    def op_pull(self, msg):
        """Everything that changed: status, queued ids, running jobs, results newer than `since`."""
        p = self.project(msg)
        since = int(msg.get("since") or 0)
        try:
            st = read_json(os.path.join(inbox(p), "status.json"))
        except (OSError, ValueError):
            st = None
        age = _age(st.get("alive_at")) if st else None
        alive = bool(st) and age is not None and age < 30 and not st.get("stopped_at")
        running = []
        for f in sorted(os.listdir(inbox(p, "running"))):
            if f.endswith(".json"):
                try:
                    running.append(dict(read_json(os.path.join(inbox(p, "running"), f)), id=f[:-5], project=p))
                except (OSError, ValueError, TypeError):
                    pass
        done = []
        with os.scandir(inbox(p, "done")) as it:
            for e in it:
                if e.name.endswith(".json"):
                    try:
                        mt = e.stat().st_mtime_ns
                    except OSError:
                        continue
                    if mt >= since:
                        done.append((mt, e.path))
        done.sort()
        docs, cursor = [], since
        for mt, path in done[:100]:
            try:
                d = read_json(path)
            except (OSError, ValueError):
                break                          # being written: pick it up next time
            d["log_size"] = _size_of(os.path.join(inbox(p, "logs"), d.get("id", "") + ".log"))
            docs.append(d); cursor = mt
        return dict(status=st, server_alive=alive, server_age_s=None if age is None else round(age),
                    queued=[j for _, _, j in sorted(queued(p))], running=running, done=docs, cursor=cursor)

    def op_log(self, msg):
        p, jid = self.project(msg), self.job_id(msg)
        path, off = os.path.join(inbox(p, "logs"), jid + ".log"), max(0, int(msg.get("offset") or 0))
        try:
            with open(path, "rb") as fh:
                fh.seek(0, 2); size = fh.tell()
                fh.seek(min(off, size)); data = fh.read(LOG_CHUNK)
        except OSError:
            return dict(offset=off, size=0, data="")
        return dict(offset=off, size=size, data=base64.b64encode(data).decode())

    def artifacts(self, p, jid):
        try:
            job = read_json(os.path.join(inbox(p, "done"), jid + ".json"))
        except (OSError, ValueError):
            raise AgentError("artifacts are available once the job has finished")
        pats = job.get("artifacts") or []
        if isinstance(pats, str):
            pats = [pats]
        pdir = os.path.join(ROOT, p)
        base = os.path.join(pdir, job.get("run_dir") or ".")
        if not inside(base, pdir):
            return {}
        files = {}
        for pat in pats[:50]:
            if not isinstance(pat, str):
                continue
            for f in sorted(glob.glob(os.path.join(glob.escape(base), pat), recursive=True)):
                if os.path.isfile(f) and inside(f, pdir) and not inside(f, inbox(p)) and len(files) < 1000:
                    files[os.path.relpath(f, base).replace(os.sep, "/")] = f
        return files

    def op_artifacts(self, msg):
        p, jid = self.project(msg), self.job_id(msg)
        return dict(files=[dict(path=k, size=_size_of(v)) for k, v in self.artifacts(p, jid).items()])

    def op_read(self, msg):
        p, jid = self.project(msg), self.job_id(msg)
        path = self.artifacts(p, jid).get(msg.get("path"))
        if path is None:
            raise AgentError("no such artifact")
        off = max(0, int(msg.get("offset") or 0))
        with open(path, "rb") as fh:
            fh.seek(off); data = fh.read(READ_CHUNK)
        return dict(data=base64.b64encode(data).decode(), eof=len(data) < READ_CHUNK)


def _size_of(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def agent(a):
    """Serve one link connection on stdin/stdout. sshd runs this for a paired key."""
    out, inp = sys.stdout.buffer, sys.stdin.buffer

    def send(obj):
        out.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")); out.flush()

    ag = Agent(a.client)
    if client_ok(a.client) is None:
        send(dict(ok=False, fatal=True, error=REVOKED)); return 1
    for line in inp:
        if not line.strip():
            continue
        n = None
        try:
            msg = json.loads(line)
            n = msg.get("n")
            fn = getattr(ag, "op_" + str(msg.get("op")), None)
            if fn is None:
                raise AgentError(f"unknown op {msg.get('op')!r}")
            res = dict(fn(msg), ok=True)
        except AgentError as e:
            res = dict(ok=False, error=str(e), **({"fatal": True} if e.fatal else {}))
        except Exception as e:  # noqa: BLE001
            res = dict(ok=False, error=f"agent error: {type(e).__name__}: {e}")
        res["n"] = n
        send(res)
    return 0


# ---- pairing: B makes a key for an endpoint and hands it over in a bundle file
def _windows_admin():
    try:
        out = subprocess.run(["whoami", "/groups"], capture_output=True, text=True, creationflags=NOWIN).stdout
        return "S-1-5-32-544" in out
    except OSError:
        return False


def _ssh_dir():
    if os.environ.get("DROPQ_SSH_DIR"):
        return os.environ["DROPQ_SSH_DIR"]
    return os.path.join(os.environ.get("PROGRAMDATA", r"C:\ProgramData"), "ssh") if WIN else "/etc/ssh"


def authorized_keys_path():
    """Where this machine's sshd looks for the current user's keys."""
    if os.environ.get("DROPQ_AUTHORIZED_KEYS"):
        return os.environ["DROPQ_AUTHORIZED_KEYS"]
    if WIN and _windows_admin():   # Windows' sshd ignores ~/.ssh/authorized_keys for administrators
        return os.path.join(_ssh_dir(), "administrators_authorized_keys")
    return os.path.join(os.path.expanduser("~"), ".ssh", "authorized_keys")


def host_keys():
    keys = []
    for f in sorted(glob.glob(os.path.join(glob.escape(_ssh_dir()), "ssh_host_*_key.pub"))):
        try:
            parts = open(f, encoding="utf-8").read().split()
        except OSError:
            continue
        if len(parts) >= 2:
            keys.append(f"{parts[0]} {parts[1]}")
    return keys


def default_host():
    ts = shutil.which("tailscale") or (r"C:\Program Files\Tailscale\tailscale.exe"
                                       if WIN and os.path.exists(r"C:\Program Files\Tailscale\tailscale.exe") else None)
    if ts:
        try:
            out = subprocess.run([ts, "status", "--json"], capture_output=True, text=True, timeout=10,
                                 creationflags=NOWIN).stdout
            name = (json.loads(out).get("Self") or {}).get("DNSName", "").rstrip(".")
            if name:
                return name
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
    return socket.gethostname()


def sshd_problems():
    """Human-readable hints when this machine doesn't look ready to accept ssh connections."""
    if os.environ.get("DROPQ_SSH_DIR"):
        return []
    if WIN:
        r = subprocess.run(["sc", "query", "sshd"], capture_output=True, text=True, creationflags=NOWIN)
        if r.returncode != 0:
            return ["OpenSSH Server is not installed. In an Administrator PowerShell:",
                    "  Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0",
                    "  Start-Service sshd; Set-Service sshd -StartupType Automatic"]
        if "RUNNING" not in r.stdout:
            return ["sshd is installed but not running. In an Administrator PowerShell:",
                    "  Start-Service sshd; Set-Service sshd -StartupType Automatic"]
        return []
    if not host_keys():
        return [f"no ssh host keys in {_ssh_dir()} - is an ssh server (sshd) installed and running?"]
    return []


def agent_script():
    """Write the launcher sshd runs for paired keys; returns its path."""
    py = sys.executable.replace("pythonw.exe", "python.exe")
    me = os.path.abspath(__file__)
    os.makedirs(SRV, exist_ok=True)
    if WIN:
        path = os.path.join(SRV, "agent.cmd")
        root = ROOT + "." if ROOT.endswith("\\") else ROOT      # "D:\" would escape the closing quote
        text = f'@echo off\r\n"{py}" "{me}" --root "{root}" agent --client %1\r\n'
    else:
        import shlex
        path = os.path.join(SRV, "agent.sh")
        text = (f"#!/bin/sh\nexec {shlex.quote(py)} {shlex.quote(me)} --root {shlex.quote(ROOT)} "
                f'agent --client "$1"\n')
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    if not WIN:
        os.chmod(path, 0o755)
    return path


def _key_lines(path, name):
    """authorized_keys lines that belong to endpoint `name`."""
    try:
        lines = open(path, encoding="utf-8").read().splitlines()
    except OSError:
        return [], []
    tag = f" dropq:{name}"
    return lines, [ln for ln in lines if ln.rstrip().endswith(tag)]


def _write_authorized_keys(path, lines):
    if os.path.dirname(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not WIN and os.path.basename(os.path.dirname(path)) == ".ssh":
            os.chmod(os.path.dirname(path), 0o700)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("".join(ln + "\n" for ln in lines))
    if WIN and os.path.basename(path) == "administrators_authorized_keys":
        # sshd only trusts this file when Administrators and SYSTEM alone can access it (SIDs: any language)
        subprocess.run(["icacls", path, "/inheritance:r", "/grant", "*S-1-5-32-544:F", "/grant", "*S-1-5-18:F"],
                       capture_output=True, creationflags=NOWIN)
    elif not WIN:
        os.chmod(path, 0o600)


def new_code():
    c = "".join(secrets.choice(CODE_ALPHABET) for _ in range(16))
    return "-".join(c[i:i + 4] for i in range(0, 16, 4))


def norm_code(c):
    c = re.sub(r"[^A-Za-z0-9]", "", c or "").upper()
    return "-".join(c[i:i + 4] for i in range(0, len(c), 4))


def pair(a):
    name = a.name
    if not NAME_RE.match(name):
        sys.exit("endpoint names: letters, digits, _ . -")
    reg = load_registry()
    if not a.project:
        sys.exit("say which projects this endpoint may use: dropq pair NAME -p PROJECT [-p ...]")
    for p in a.project:
        if p not in reg or reg[p]["remote"]:
            sys.exit(f"no local project {p} here (create it with `dropq init {p}`)")
    keygen = shutil.which("ssh-keygen")
    if not keygen:
        sys.exit("ssh-keygen not found: install OpenSSH (the client part) first")
    for hint in sshd_problems():
        print("warning:", hint)
    hks = host_keys()
    if not hks:
        sys.exit(f"no ssh host keys in {_ssh_dir()}: set up the ssh server first (see above)")
    ak = authorized_keys_path()
    code = new_code()
    with tempfile.TemporaryDirectory() as td:
        kp = os.path.join(td, "key")
        r = subprocess.run([keygen, "-q", "-t", "ed25519", "-a", "64", "-N", code, "-C", f"dropq:{name}",
                            "-f", kp], capture_output=True, text=True, creationflags=NOWIN)
        if r.returncode:
            sys.exit(f"ssh-keygen failed: {r.stderr.strip()}")
        priv = open(kp, encoding="utf-8").read()
        ktype, kdata = open(kp + ".pub", encoding="utf-8").read().split()[:2]
    script = agent_script()
    cmd = (f'"{script}" {name}' if " " in script else f"{script} {name}").replace('"', '\\"')
    opts = f'restrict,command="{cmd}"'
    expires = None
    if a.days:
        expires = (dt.datetime.now().astimezone() + dt.timedelta(days=a.days)).replace(microsecond=0)
        opts += f',expiry-time="{expires.strftime("%Y%m%d%H%M")}"'
    lines, mine = _key_lines(ak, name)
    try:
        _write_authorized_keys(ak, [ln for ln in lines if ln not in mine]
                               + [f"{opts} {ktype} {kdata} dropq:{name}"])
    except PermissionError:
        if ak.endswith("administrators_authorized_keys"):
            sys.exit(f"your account is an administrator, so Windows' sshd reads {ak}, which only an "
                     "elevated process may write: run `dropq pair` again from an Administrator terminal")
        sys.exit(f"cannot write {ak} (permission denied)")
    clients = load_clients()
    clients[name] = dict(projects=list(a.project), created_at=now(), key=f"{ktype} {kdata}",
                         expires_at=expires.isoformat() if expires else None)
    save_clients(clients)
    rules = {p: {k: v for k, v in reg[p].items() if k != "remote"} for p in a.project}
    bundle = dict(format="dropq-link/1", name=name, host=a.host or default_host(), port=a.port,
                  user=a.user or getpass.getuser(), host_keys=hks, key=priv, projects=rules,
                  created_at=now(), expires_at=clients[name]["expires_at"])
    out = a.output or f"{name}.dropq"
    write_json(out, bundle)
    print(f"{'replaced' if mine else 'added'} a key for {name!r} -> projects {', '.join(a.project)}"
          + (f", expires {expires:%Y-%m-%d %H:%M}" if expires else ""))
    print(f"connects to {bundle['user']}@{bundle['host']}:{a.port}  (change on A with dropq link --host/--user)")
    print(f"\nbundle: {os.path.abspath(out)}\ncarry it to the other machine and run there:\n"
          f"  dropq link {os.path.basename(out)}\n\nit will ask for this code (shown only here):\n\n"
          f"    {code}\n")
    print(f"the key in the bundle only opens dropq for {', '.join(a.project)}; revoke it with: dropq keys revoke {name}")


def keys(a):
    clients = load_clients()
    if a.action == "revoke":
        if not a.name:
            sys.exit("dropq keys revoke NAME")
        known = clients.pop(a.name, None)
        save_clients(clients)                      # the agent refuses unknown clients right away
        ak = authorized_keys_path()
        lines, mine = _key_lines(ak, a.name)
        if not known and not mine:
            sys.exit(f"no key for {a.name}")
        if mine:
            try:
                _write_authorized_keys(ak, [ln for ln in lines if ln not in mine])
            except PermissionError:
                print(f"note: could not edit {ak} (permission denied); the key no longer opens dropq, "
                      "but remove its line there (or rerun this as Administrator)")
        print(f"revoked {a.name}"); return 0
    if not clients:
        print("no paired endpoints - add one with `dropq pair NAME -p PROJECT`"); return 0
    for n, c in sorted(clients.items()):
        exp = c.get("expires_at")
        state = "EXPIRED" if exp and dt.datetime.now().astimezone() > parse_time(exp) else "ok"
        print(f"  {n:16s} {state:8s} projects={','.join(c.get('projects', []))} created={c.get('created_at')}"
              + (f" expires={exp}" if exp else ""))
    return 0


# ------------------------------------------------------------------ remote: the submitting machine (A)
RELAY_POLL = float(os.environ.get("DROPQ_RELAY_POLL", "1"))
NEVER = "2000-01-01T00:00:00+00:00"       # alive_at of a link that has never reached a live server


def _private(path):
    """Make a key file readable by its owner only (ssh refuses keys others can read)."""
    if WIN:
        user = os.environ.get("USERNAME") or getpass.getuser()
        subprocess.run(["icacls", path, "/inheritance:r", "/grant:r", f"{user}:F"], capture_output=True,
                       creationflags=NOWIN)
    else:
        os.chmod(path, 0o600)


def _shred(path):
    try:
        size = os.path.getsize(path)
        with open(path, "r+b") as fh:
            fh.write(secrets.token_bytes(size)); fh.flush(); os.fsync(fh.fileno())
    except OSError:
        pass
    _remove(path)


def link(a):
    try:
        b = read_json(a.bundle)
        assert b.get("format") == "dropq-link/1"
    except Exception:  # noqa: BLE001
        sys.exit(f"{a.bundle} is not a dropq link bundle (made by `dropq pair` on the other machine)")
    name = a.name or b["name"]
    if not NAME_RE.match(name):
        sys.exit("link names: letters, digits, _ . -")
    ldir = os.path.join(LINKS, name)
    if os.path.exists(os.path.join(ldir, "link.json")):
        sys.exit(f"already linked as {name!r}: `dropq unlink {name}` first, or pick another --name")
    reg = load_registry()
    for p in b["projects"]:
        if p in reg and reg[p]["remote"] != name:
            sys.exit(f"there is already a project {p!r} here; unlink or rename it before linking")
    keygen = shutil.which("ssh-keygen")
    if not keygen:
        sys.exit("ssh-keygen not found: install OpenSSH (the client) first")
    if not shutil.which("ssh"):
        print("warning: no ssh on PATH - the link will not connect until OpenSSH's client is installed")
    code = norm_code(a.code or getpass.getpass("code shown on the other machine: "))
    os.makedirs(ldir, exist_ok=True)
    key = os.path.join(ldir, "key")
    with open(key, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(b["key"])
    _private(key)
    r = subprocess.run([keygen, "-p", "-q", "-f", key, "-P", code, "-N", ""], capture_output=True, text=True,
                       stdin=subprocess.DEVNULL, creationflags=NOWIN)
    if r.returncode:
        shutil.rmtree(ldir, ignore_errors=True)
        sys.exit("wrong code (or a damaged bundle) - nothing was changed")
    _private(key)
    alias = f"dropq-{name}"
    with open(os.path.join(ldir, "known_hosts"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("".join(f"{alias} {hk}\n" for hk in b["host_keys"]))
    cfg = dict(name=name, host=a.host or b["host"], port=a.port or b.get("port", 22), user=a.user or b["user"],
               projects=sorted(b["projects"]), linked_at=now(), expires_at=b.get("expires_at"))
    write_json(os.path.join(ldir, "link.json"), cfg)
    for p in b["projects"]:
        reg[p] = dict(DEFAULT_RULES, remote=name)
        os.makedirs(os.path.join(ROOT, p), exist_ok=True)
        make_inbox(p)
    save_registry(reg)
    if not a.keep:
        _shred(a.bundle)
    print(f"linked {name!r}: {cfg['user']}@{cfg['host']}:{cfg['port']}")
    for p in sorted(b["projects"]):
        print(f"  {os.path.join(ROOT, p)}  -> jobs run on {cfg['host']}")
    if not a.keep:
        print(f"(deleted {a.bundle})")
    print("the server here relays them; if it isn't running: dropq serve (or dropq install)")


def unlink(a):
    ldir = os.path.join(LINKS, a.name)
    reg = load_registry()
    mine = [p for p in reg if reg[p]["remote"] == a.name]
    if not os.path.isdir(ldir) and not mine:
        sys.exit(f"no link {a.name}")
    for p in mine:
        del reg[p]
    save_registry(reg)
    key = os.path.join(ldir, "key")
    if os.path.exists(key):
        _shred(key)
    shutil.rmtree(ldir, ignore_errors=True)
    print(f"removed link {a.name}" + (f" (projects {', '.join(mine)} are no longer served; their folders stay)"
                                      if mine else ""))
    print("to also invalidate the key, run on the other machine: dropq keys revoke " + a.name)


def links(a):
    names = sorted(os.listdir(LINKS)) if os.path.isdir(LINKS) else []
    if not names:
        print("no links - import a bundle with `dropq link FILE.dropq`"); return 0
    try:
        live = read_json(STATUS).get("links", {})
    except (OSError, ValueError):
        live = {}
    for n in names:
        try:
            cfg = read_json(os.path.join(LINKS, n, "link.json"))
        except (OSError, ValueError):
            continue
        s = live.get(n) or {}
        state = ("UP" if s.get("up") else "DOWN") if s else "not running (start dropq serve)"
        print(f"  {n:16s} {state:5s} {cfg['user']}@{cfg['host']}:{cfg['port']}  projects={','.join(cfg['projects'])}"
              + (f"  last contact {s['last_contact']}" if s.get("last_contact") else "")
              + (f"\n  {'':16s} error: {s['error']}" if s.get("error") and not s.get("up") else ""))
    return 0


class LinkDown(Exception):
    pass


class Relay(threading.Thread):
    """Relays a link's projects: sends their queued jobs and cancels to the other machine and
    mirrors back its status, running jobs, logs, results and artifacts. One ssh connection;
    reconnects with backoff. While it is down, jobs wait here (or expire with not_after)."""

    def __init__(self, name, projects):
        super().__init__(daemon=True, name=f"relay-{name}")
        self.link, self.projects = name, projects
        self.dir = os.path.join(LINKS, name)
        self.cfg = read_json(os.path.join(self.dir, "link.json"))
        self.proc, self.n, self.up, self.error, self.last_contact = None, 0, False, None, None
        self.halt = threading.Event()
        try:
            self.state = read_json(os.path.join(self.dir, "state.json"))
        except (OSError, ValueError):
            self.state = {}
        for p in projects:
            self.state.setdefault(p, dict(sent=[], since=0, alive_at=None, status=None, server_alive=False))
        self.start()

    # ---- plumbing
    def summary(self):
        return dict(up=self.up, error=self.error, last_contact=self.last_contact, host=self.cfg["host"],
                    projects=self.projects)

    def stop(self):
        self.halt.set()
        self.close()

    def command(self):
        if self.cfg.get("command"):              # a custom transport (used by the tests)
            return self.cfg["command"]
        ssh = shutil.which("ssh")
        if not ssh:
            raise LinkDown("no ssh on PATH (install the OpenSSH client)")
        kh = os.path.join(self.dir, "known_hosts").replace("\\", "/")   # ssh's option parser and backslashes
        return [ssh, "-F", "none", "-T", "-p", str(self.cfg["port"]), "-i", os.path.join(self.dir, "key"),
                "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", f'UserKnownHostsFile="{kh}"', "-o", f"HostKeyAlias=dropq-{self.link}",
                "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3", "-o", "ConnectTimeout=15",
                "-o", "LogLevel=ERROR", f"{self.cfg['user']}@{self.cfg['host']}"]

    def connect(self):
        self.errlog = os.path.join(self.dir, "ssh.log")
        with open(self.errlog, "wb") as err:
            self.proc = subprocess.Popen(self.command(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=err, creationflags=NOWIN)
        hello = self.call("hello", version=__version__)
        if not hello.get("ok"):
            raise LinkDown(hello.get("error"))
        self.up, self.error, self.last_contact = True, None, now()
        say(f"link {self.link}: connected to {hello.get('host')} (dropq {hello.get('version')})")

    def close(self):
        p, self.proc = self.proc, None
        if p is not None:
            try:
                p.stdin.close()
            except OSError:
                pass
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    def call(self, op, **kw):
        if self.proc is None:
            raise LinkDown("not connected")
        self.n += 1
        try:
            self.proc.stdin.write((json.dumps(dict(kw, op=op, n=self.n)) + "\n").encode())
            self.proc.stdin.flush()
            line = self.proc.stdout.readline()
        except (OSError, ValueError) as e:
            raise LinkDown(f"connection lost: {e}")
        if not line:
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            msg = " ".join(tail(self.errlog, 3)).strip() or f"ssh exited ({self.proc.returncode})"
            raise LinkDown(msg)
        res = json.loads(line)
        if res.get("fatal"):
            raise LinkDown(res.get("error"))
        self.last_contact = now()
        return res

    # ---- the loop
    def run(self):
        backoff = 2.0
        while not self.halt.is_set():
            try:
                self.connect()
                backoff = 2.0
                while not self.halt.is_set():
                    for p in self.projects:
                        self.sync(p)
                    self.save()
                    self.halt.wait(RELAY_POLL)
            except Exception as e:  # noqa: BLE001  (LinkDown, or anything unexpected: reconnect)
                if self.halt.is_set():
                    break
                if self.up or self.error != str(e):
                    say(f"link {self.link}: down - {e}")
                self.up, self.error = False, str(e)
                self.close()
            t_end = time.time() + backoff
            while not self.halt.is_set() and time.time() < t_end:
                for p in self.projects:
                    try:
                        self.local(p); self.write_status(p)
                    except Exception:  # noqa: BLE001
                        pass
                self.halt.wait(min(2.0, max(0.0, t_end - time.time())))
            backoff = min(backoff * 2, 60.0)
        self.close()

    def save(self):
        try:
            write_json(os.path.join(self.dir, "state.json"), self.state, tries=1)
        except OSError:
            pass

    # ---- per project
    def local_done(self, p, jid, job, status, error):
        job = dict(job if isinstance(job, dict) else {}, id=jid, project=p, status=status, error=error,
                   finished_at=now())
        write_json(os.path.join(inbox(p, "done"), jid + ".json"), job)
        say(f"{p}/{jid}: {status} - {error}")

    def local(self, p):
        """What needs no connection: cancel or expire jobs that haven't been sent yet."""
        sent = set(self.state[p]["sent"])
        for f in os.listdir(inbox(p, "cancel")):
            qf = os.path.join(inbox(p, "queue"), f + ".json")
            if f not in sent and os.path.exists(qf):
                _remove(qf); _remove(os.path.join(inbox(p, "cancel"), f))
                self.local_done(p, f, {}, "cancelled", "cancelled before it was sent")
        for _, _, jid in sorted(queued(p)):
            if jid in sent:
                continue
            qf = os.path.join(inbox(p, "queue"), jid + ".json")
            try:
                job = read_json(qf)
                if job.get("not_after") and dt.datetime.now().astimezone() > parse_time(job["not_after"]):
                    _remove(qf)
                    self.local_done(p, jid, job, "failed",
                                    f"expired: the link was down until after not_after ({job['not_after']})")
            except Exception:  # noqa: BLE001  (half-written or bad: sync() deals with it)
                pass

    def sync(self, p):
        st, ib = self.state[p], inbox(p)
        self.local(p)
        sent = set(st["sent"])
        # cancels for jobs the other side has
        for f in os.listdir(inbox(p, "cancel")):
            if ID_RE.match(f):
                self.call("cancel", project=p, id=f)
            _remove(os.path.join(inbox(p, "cancel"), f))
        # new jobs
        for _, _, jid in sorted(queued(p)):
            if jid in sent:
                continue
            qf = os.path.join(ib, "queue", jid + ".json")
            if not ID_RE.match(jid):
                _remove(qf); self.local_done(p, re.sub(r"[^A-Za-z0-9_.-]", "_", jid)[:100] or "bad-id", {},
                                             "failed", "invalid job id (use letters, digits, _ . -)")
                continue
            try:
                job = read_json(qf)
            except Exception as e:  # noqa: BLE001
                if time.time() - os.path.getmtime(qf) > 60:
                    _remove(qf); self.local_done(p, jid, {}, "failed", f"unreadable job file: {e}")
                continue
            r = self.call("submit", project=p, id=jid, job=job)
            if r.get("ok"):
                st["sent"].append(jid); sent.add(jid)
                say(f"{p}/{jid}: sent to {self.link}")
            else:
                _remove(qf); self.local_done(p, jid, job, "failed", r.get("error"))
        # what happened over there
        r = self.call("pull", project=p, since=st["since"])
        if not r.get("ok"):
            raise LinkDown(r.get("error"))
        st["status"], st["server_alive"] = r.get("status"), r.get("server_alive")
        if r.get("server_alive"):
            st["alive_at"] = now()
        remote_q = set(r.get("queued", []))
        st["queued"] = sorted(remote_q)
        running = {d["id"]: d for d in r.get("running", []) if isinstance(d, dict) and ID_RE.match(str(d.get("id")))}
        for jid, d in running.items():
            write_json(os.path.join(ib, "running", jid + ".json"), d)
            self.fetch_log(p, jid, None)
        for d in r.get("done", []):
            jid = str(d.get("id"))
            if not ID_RE.match(jid):
                continue
            if not os.path.exists(os.path.join(ib, "done", jid + ".json")):
                self.fetch_log(p, jid, d.pop("log_size", None))
                if d.get("artifacts") and d.get("status") != "cancelled" and d.get("run_dir") is not None:
                    self.fetch_artifacts(p, jid, d)
                d.pop("log_size", None)
                write_json(os.path.join(ib, "done", jid + ".json"), d)
            if jid in sent:
                st["sent"].remove(jid); sent.discard(jid)
            _remove(os.path.join(ib, "queue", jid + ".json"))
        st["since"] = r.get("cursor", st["since"])
        for f in os.listdir(os.path.join(ib, "running")):
            if f.endswith(".json") and f[:-5] not in running:
                _remove(os.path.join(ib, "running", f))
        for jid in list(sent):                  # started over there: no longer queued
            if jid not in remote_q:
                _remove(os.path.join(ib, "queue", jid + ".json"))
        self.write_status(p)

    def fetch_log(self, p, jid, size):
        """Append new log bytes. size=None: one chunk (running job); else read up to size."""
        path = os.path.join(inbox(p, "logs"), jid + ".log")
        for _ in range(10000):
            have = _size_of(path)
            if size is not None and have >= size:
                return
            r = self.call("log", project=p, id=jid, offset=have)
            data = base64.b64decode(r.get("data") or "") if r.get("ok") else b""
            if not data:
                return
            with open(path, "ab") as fh:
                fh.write(data)
            if size is None:
                return

    def fetch_artifacts(self, p, jid, d):
        """Copy the files the job's `artifacts` globs matched into _jobs/artifacts/ID/."""
        r = self.call("artifacts", project=p, id=jid)
        if not r.get("ok"):
            d["artifacts_error"] = r.get("error"); return
        dest = os.path.join(inbox(p), "artifacts", jid)
        cap = float(self.cfg.get("max_artifact_mb", 500)) * 1024 * 1024
        got, skipped, total = [], [], 0
        for f in r.get("files", []):
            rel = str(f.get("path", ""))
            out = os.path.normpath(os.path.join(dest, rel))
            if not rel or not inside(out, dest):
                continue
            if total + f.get("size", 0) > cap:
                skipped.append(rel); continue
            os.makedirs(os.path.dirname(out), exist_ok=True)
            off = 0
            with open(out + ".part", "wb") as fh:
                while True:
                    c = self.call("read", project=p, id=jid, path=rel, offset=off)
                    if not c.get("ok"):
                        break
                    data = base64.b64decode(c.get("data") or "")
                    fh.write(data); off += len(data)
                    if c.get("eof") or not data:
                        break
            os.replace(out + ".part", out)
            total += off
            got.append(rel)
        d["artifacts_dir"] = os.path.relpath(dest, os.path.join(ROOT, p)).replace(os.sep, "/")
        d["artifacts_fetched"] = got
        if skipped:
            d["artifacts_skipped"] = skipped
            d["artifacts_error"] = f"over the link's {cap / 1048576:.0f} MB limit (max_artifact_mb in link.json)"

    def write_status(self, p):
        """The project's status.json here: the other side's view, plus the link's state.

        alive_at is this machine's time of the last contact that found the server there alive,
        so a dead link or a stopped server both show up as a stale heartbeat."""
        st = self.state[p]
        s = dict(st.get("status") or {})
        if not self.up:
            s.pop("stopped_at", None)         # unknown while the link is down
        sent = set(st["sent"])
        unsent = [j for _, _, j in sorted(queued(p)) if j not in sent]
        s.update(project=p, alive_at=st.get("alive_at") or NEVER,
                 queued=(st.get("queued") or []) + unsent if self.up else [j for _, _, j in sorted(queued(p))])
        s.setdefault("rules", {})
        s["remote"] = dict(link=self.link, host=self.cfg["host"], link_up=self.up, link_error=self.error,
                           last_contact=self.last_contact, server_alive=bool(st.get("server_alive")) and self.up,
                           waiting_to_send=len(unsent))
        try:
            write_json(os.path.join(inbox(p), "status.json"), s, tries=1)
        except OSError:
            pass


# ------------------------------------------------------------------ client commands
def new_id(name):
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    slug = "".join(c if c.isalnum() or c in "-_" else "-" for c in (name or "job"))[:30]
    return f"{stamp}-{slug}-{uuid.uuid4().hex[:4]}"


def pick_project(a):
    p = a.project or project_of_cwd()
    if not p:
        sys.exit("which project? use -p NAME (or run from inside the project folder)")
    if not os.path.isdir(inbox(p)):
        sys.exit(f"no project inbox at {inbox(p)} - has the owner run `dropq init {p}`?")
    return p


def find_job(jid, project=None):
    """Locate a job by id: returns (project, state, path)."""
    if not ID_RE.match(jid):
        return None, None, None
    try:
        cands = [project] if project else sorted(d for d in os.listdir(ROOT) if os.path.isdir(inbox(d)))
    except OSError:
        cands = []
    for p in cands:
        for sub in ("done", "running", "queue"):
            path = os.path.join(inbox(p, sub), jid + ".json")
            if os.path.exists(path):
                return p, sub, path
    return None, None, None


def submit(a):
    p = pick_project(a)
    cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
    if not cmd:
        sys.exit("give the command after --, e.g. dropq submit -- python script.py")
    default_name = os.path.basename(cmd[1] if len(cmd) > 1 and not a.shell else cmd[0])
    job = dict(cmd=" ".join(cmd) if a.shell else cmd, shell=a.shell, cwd=a.cwd,
               timeout_s=None if a.timeout == 0 else a.timeout, background=a.background,
               name=a.name, note=a.note, submitted_at=now())
    for k, v in (("commit", a.commit), ("artifacts", a.artifact), ("not_after", a.not_after)):
        if v:
            job[k] = v
    jid = new_id(a.name or default_name)
    write_json(os.path.join(inbox(p, "queue"), jid + ".json"), job)
    print(jid, flush=True)
    if a.follow:
        a.id = jid
        return follow(a)


def status(a):
    if not os.path.exists(STATUS):
        print(f"no {STATUS} - the server has never run on {ROOT}"); return 1
    s = read_json(STATUS)
    age = (dt.datetime.now().astimezone() - dt.datetime.fromisoformat(s["alive_at"])).total_seconds()
    alive = age < max(15, 5 * POLL) and not s.get("stopped_at")
    state = "ALIVE" if alive else ("STOPPED" if s.get("stopped_at") else f"NOT RESPONDING (last seen {age:.0f}s ago)")
    print(f"server {state} | {s['root']}")
    for g in s.get("gpu") or []:
        print("gpu:", ", ".join(f"{k}={v}" for k, v in g.items()))
    for r in s["running"]:
        print(f"running: {r['project']}/{r['id']} {'[bg] ' if r['background'] else ''}{r['elapsed_s']}s")
    print("queued:", ", ".join(s["queued"]) or "none")
    for n, l in sorted((s.get("links") or {}).items()):
        print(f"link {n}: {'UP' if l.get('up') else 'DOWN'} -> {l.get('host')} ({', '.join(l.get('projects', []))})"
              + ("" if l.get("up") or not l.get("error") else f"  {l['error']}"))
    return 0 if alive else 1


def list_done(a):
    here = project_of_cwd()
    ps = [a.project] if a.project else ([here] if here else sorted(load_registry()))
    rows = []
    for p in ps:
        d = inbox(p, "done")
        if os.path.isdir(d):
            rows += [(f, p) for f in os.listdir(d) if f.endswith(".json")]
    for f, p in sorted(rows)[-a.n:]:
        try:
            r = read_json(os.path.join(inbox(p, "done"), f))
        except Exception:  # noqa: BLE001
            continue
        dur = r.get("duration_s")
        print(f"{p + '/' + r['id']:60s} {r['status']:11s} rc={r.get('returncode')!s:5s} "
              f"{'' if dur is None else f'{dur}s'}")


def show(a):
    p, sub, path = find_job(a.id, a.project)
    if not path:
        sys.exit(f"no job {a.id}")
    r = read_json(path); r.pop("log_tail", None)
    print(json.dumps(dict(r, state=sub), indent=1, ensure_ascii=False))
    print("--- log tail ---")
    print("\n".join(tail(os.path.join(inbox(p, "logs"), a.id + ".log"), a.n)))


def follow(a):
    """Stream a job's log until it finishes; exit with the job's return code."""
    jid, pos, waited = a.id, 0, False
    try:
        while True:
            p, sub, path = find_job(jid, getattr(a, "project", None))
            if sub is None:
                sys.exit(f"no job {jid}")
            log = os.path.join(inbox(p, "logs"), jid + ".log")
            if os.path.exists(log):
                with open(log, "rb") as fh:
                    fh.seek(pos); chunk = fh.read(); pos = fh.tell()
                if chunk:
                    sys.stdout.write(chunk.decode("utf-8", "replace")); sys.stdout.flush()
            elif sub == "queue" and not waited:
                print("(queued; waiting for it to start)", flush=True); waited = True
            if sub == "done":
                r = read_json(path)
                if r.get("error"):
                    print(f"error: {r['error']}")
                print(f"[{jid}: {r['status']}, rc={r.get('returncode')}]")
                rc = r.get("returncode")
                return rc if isinstance(rc, int) and rc >= 0 else (0 if r["status"] == "done" else 1)
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n(stopped following; {jid} keeps running)")
        return 130


def cancel(a):
    p, sub, _ = find_job(a.id, a.project)
    if sub in (None, "done"):
        sys.exit(f"no queued or running job {a.id}")
    open(os.path.join(inbox(p, "cancel"), a.id), "w").close()
    print("cancel requested for", f"{p}/{a.id}")


# ------------------------------------------------------------------ live monitor
def _enable_ansi():
    """Turn on ANSI escape handling in the Windows console (no-op elsewhere)."""
    if not WIN:
        return True
    try:
        import ctypes
        k = ctypes.windll.kernel32
        h = k.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if not k.GetConsoleMode(h, ctypes.byref(mode)):
            return False
        return bool(k.SetConsoleMode(h, mode.value | 0x0004))
    except Exception:  # noqa: BLE001
        return False


def _dur(sec):
    sec = int(max(0, sec))
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m{sec % 60:02d}s"
    if sec < 86400:
        return f"{sec // 3600}h{sec % 3600 // 60:02d}m"
    return f"{sec // 86400}d{sec % 86400 // 3600:02d}h"


def _age(iso):
    try:
        return (dt.datetime.now().astimezone() - dt.datetime.fromisoformat(iso)).total_seconds()
    except (TypeError, ValueError):
        return None


def _size(n, rate=False):
    n = float(n)
    for unit in ("B", "K", "M", "G", "T"):
        if abs(n) < 1024 or unit == "T":
            s = f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
            return s + ("/s" if rate else "")
        n /= 1024


def _bar(frac, width=20):
    frac = min(max(frac, 0.0), 1.0)
    full = int(round(frac * width))
    return "#" * full + "." * (width - full)


def _spark(fracs):
    """One character per core: ' ' idle .. '@' busy."""
    levels = " .:-=+*#%@"
    return "".join(levels[min(len(levels) - 1, int(round(min(max(f, 0), 1) * (len(levels) - 1))))] for f in fracs)


def _last_line(path):
    for line in reversed(tail(path, 5)):
        line = line.split("\r")[-1].strip()
        if line and not line.startswith("### "):
            return line
    return ""


def _recent_done(projects, n):
    rows = []
    for p in projects:
        try:
            with os.scandir(inbox(p, "done")) as it:
                for e in it:
                    if e.name.endswith(".json"):
                        try:
                            rows.append((e.stat().st_mtime, p, e.path))
                        except OSError:
                            pass
        except OSError:
            continue
    out = []
    for mt, p, path in sorted(rows, reverse=True)[:n]:
        try:
            out.append((mt, p, read_json(path)))
        except Exception:  # noqa: BLE001
            pass
    return out


def _gpu_procs():
    """{pid: used GPU memory in MiB} from nvidia-smi (empty if unavailable)."""
    exe = shutil.which("nvidia-smi")
    if not exe:
        return {}
    try:
        out = subprocess.run([exe, "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits"],
                             capture_output=True, text=True, timeout=10,
                             creationflags=subprocess.CREATE_NO_WINDOW if WIN else 0).stdout
    except Exception:  # noqa: BLE001
        return {}
    res = {}
    for line in out.splitlines():
        parts = [x.strip() for x in line.split(",")]
        if len(parts) == 2 and parts[0].isdigit():
            try:
                res[int(parts[0])] = res.get(int(parts[0]), 0) + float(parts[1])
            except ValueError:
                pass      # "[N/A]" on some Windows drivers
    return res


class SystemStats:
    """Host and per-job resource usage. Uses psutil when installed, else what the OS offers.

    sample() returns a dict; rates and CPU percentages are measured since the previous sample.
    """

    def __init__(self, disk_path):
        self.disk_path = disk_path
        self.prev_t = None
        self.prev = {}
        self.procs = {}          # psutil.Process cache, so per-process cpu_percent has a baseline
        try:
            if os.environ.get("DROPQ_NO_PSUTIL"):
                raise ImportError
            import psutil
            self.ps = psutil
            psutil.cpu_percent(percpu=True)
        except ImportError:
            self.ps = None
        self.backend = "psutil" if self.ps else ("proc" if os.path.exists("/proc/stat") else
                                                 "windows" if WIN else "basic")

    # ---- helpers for the /proc backend
    @staticmethod
    def _proc_cpu_times():
        cores, total = [], None
        with open("/proc/stat") as fh:
            for line in fh:
                f = line.split()
                if not f or not f[0].startswith("cpu"):
                    break
                vals = [int(x) for x in f[1:]]
                entry = (sum(vals), vals[3] + (vals[4] if len(vals) > 4 else 0))   # (all, idle+iowait)
                if f[0] == "cpu":
                    total = entry
                else:
                    cores.append(entry)
        return total, cores

    @staticmethod
    def _proc_tree_stats():
        """pid -> (ppid, cpu ticks, rss bytes) for every process."""
        page = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096
        res = {}
        for d in os.listdir("/proc"):
            if not d.isdigit():
                continue
            try:
                with open(f"/proc/{d}/stat") as fh:
                    st = fh.read()
                rest = st[st.rindex(")") + 2:].split()
                res[int(d)] = (int(rest[1]), int(rest[11]) + int(rest[12]), int(rest[21]) * page)
            except (OSError, ValueError, IndexError):
                pass
        return res

    def _rate(self, key, value, dt_s):
        prev = self.prev.get(key)
        self.prev[key] = value
        if prev is None or dt_s <= 0:
            return None
        return max(0.0, (value - prev) / dt_s)

    def sample(self, job_pids=()):
        t = time.time()
        dt_s = t - self.prev_t if self.prev_t else 0.0
        self.prev_t = t
        s = dict(backend=self.backend, cpu=None, cores=None, mem=None, swap=None, load=None, uptime=None,
                 procs=None, disk=None, disk_io=None, net=None, jobs={})
        try:
            u = shutil.disk_usage(self.disk_path)
            s["disk"] = (u.used, u.total)
        except OSError:
            pass
        if hasattr(os, "getloadavg"):
            try:
                s["load"] = os.getloadavg()
            except OSError:
                pass
        s["ncpu"] = os.cpu_count()
        if self.ps:
            self._sample_psutil(s, dt_s, job_pids)
        elif self.backend == "proc":
            self._sample_proc(s, dt_s, job_pids)
        elif self.backend == "windows":
            self._sample_windows(s, dt_s)
        return s

    def _sample_psutil(self, s, dt_s, job_pids):
        ps = self.ps
        cores = ps.cpu_percent(percpu=True)
        s["cores"] = [c / 100 for c in cores]
        s["cpu"] = sum(cores) / max(len(cores), 1) / 100
        vm, sw = ps.virtual_memory(), ps.swap_memory()
        s["mem"] = (vm.total - vm.available, vm.total)
        s["swap"] = (sw.used, sw.total)
        try:
            s["load"] = ps.getloadavg()
        except (AttributeError, OSError):
            pass
        s["uptime"] = time.time() - ps.boot_time()
        s["procs"] = len(ps.pids())
        try:
            io = ps.disk_io_counters()
            if io:
                s["disk_io"] = (self._rate("dr", io.read_bytes, dt_s), self._rate("dw", io.write_bytes, dt_s))
        except Exception:  # noqa: BLE001
            pass
        try:
            n = ps.net_io_counters()
            s["net"] = (self._rate("rx", n.bytes_recv, dt_s), self._rate("tx", n.bytes_sent, dt_s))
        except Exception:  # noqa: BLE001
            pass
        alive = set()
        for jp in job_pids:
            cpu = rss = 0.0
            pids = []
            try:
                root = self.procs.get(jp) or self.procs.setdefault(jp, ps.Process(jp))
                tree = [root] + root.children(recursive=True)
            except ps.Error:
                continue
            for p in tree:
                p = self.procs.setdefault(p.pid, p)
                alive.add(p.pid)
                try:
                    cpu += p.cpu_percent(None) / 100
                    rss += p.memory_info().rss
                    pids.append(p.pid)
                except ps.Error:
                    pass
            s["jobs"][jp] = dict(cpu=cpu if dt_s else None, rss=rss, nproc=len(pids), pids=pids)
        for pid in list(self.procs):
            if pid not in alive and pid not in job_pids:
                del self.procs[pid]

    def _sample_proc(self, s, dt_s, job_pids):
        total, cores = self._proc_cpu_times()
        pt, pc = self.prev.get("cpu_total"), self.prev.get("cpu_cores")
        self.prev["cpu_total"], self.prev["cpu_cores"] = total, cores
        if pt and pc and len(pc) == len(cores):
            frac = lambda a, b: 1 - (a[1] - b[1]) / max(a[0] - b[0], 1)  # noqa: E731
            s["cpu"] = frac(total, pt)
            s["cores"] = [frac(a, b) for a, b in zip(cores, pc)]
        info = {}
        with open("/proc/meminfo") as fh:
            for line in fh:
                k, v = line.split(":", 1)
                info[k] = int(v.split()[0]) * 1024
        tot = info.get("MemTotal", 0)
        s["mem"] = (tot - info.get("MemAvailable", info.get("MemFree", 0)), tot)
        s["swap"] = (info.get("SwapTotal", 0) - info.get("SwapFree", 0), info.get("SwapTotal", 0))
        try:
            with open("/proc/uptime") as fh:
                s["uptime"] = float(fh.read().split()[0])
        except OSError:
            pass
        try:
            blocks = {d for d in os.listdir("/sys/block") if not d.startswith(("loop", "ram", "zram", "dm-"))}
            rd = wr = 0
            with open("/proc/diskstats") as fh:
                for line in fh:
                    f = line.split()
                    if len(f) > 9 and f[2] in blocks:
                        rd += int(f[5]) * 512; wr += int(f[9]) * 512
            s["disk_io"] = (self._rate("dr", rd, dt_s), self._rate("dw", wr, dt_s))
        except OSError:
            pass
        try:
            rx = tx = 0
            with open("/proc/net/dev") as fh:
                for line in fh.readlines()[2:]:
                    name, data = line.split(":", 1)
                    if name.strip() != "lo":
                        f = data.split(); rx += int(f[0]); tx += int(f[8])
            s["net"] = (self._rate("rx", rx, dt_s), self._rate("tx", tx, dt_s))
        except OSError:
            pass
        allp = self._proc_tree_stats()
        s["procs"] = len(allp)
        kids = {}
        for pid, (ppid, _, _) in allp.items():
            kids.setdefault(ppid, []).append(pid)
        hz = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
        prev_ticks = self.prev.get("ticks", {})
        ticks_now = {}
        for jp in job_pids:
            if jp not in allp:
                continue
            tree, stack = [], [jp]
            while stack:
                p = stack.pop(); tree.append(p); stack.extend(kids.get(p, []))
            cpu, rss = 0.0, 0
            for p in tree:
                _, ticks, r = allp[p]
                ticks_now[p] = ticks
                rss += r
                if dt_s and p in prev_ticks:
                    cpu += (ticks - prev_ticks[p]) / hz / dt_s
            s["jobs"][jp] = dict(cpu=cpu if dt_s and jp in prev_ticks else None, rss=rss, nproc=len(tree),
                                 pids=tree)
        self.prev["ticks"] = ticks_now

    def _sample_windows(self, s, dt_s):
        import ctypes
        from ctypes import wintypes
        k = ctypes.windll.kernel32
        idle, kern, user = wintypes.FILETIME(), wintypes.FILETIME(), wintypes.FILETIME()
        if k.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kern), ctypes.byref(user)):
            v = lambda ft: (ft.dwHighDateTime << 32) | ft.dwLowDateTime  # noqa: E731
            cur = (v(kern) + v(user), v(idle))       # kernel time includes idle time
            prev = self.prev.get("cpu_total")
            self.prev["cpu_total"] = cur
            if prev:
                s["cpu"] = 1 - (cur[1] - prev[1]) / max(cur[0] - prev[0], 1)

        class MEMSTAT(ctypes.Structure):
            _fields_ = [("dwLength", wintypes.DWORD), ("dwMemoryLoad", wintypes.DWORD),
                        ("ullTotalPhys", ctypes.c_uint64), ("ullAvailPhys", ctypes.c_uint64),
                        ("ullTotalPageFile", ctypes.c_uint64), ("ullAvailPageFile", ctypes.c_uint64),
                        ("ullTotalVirtual", ctypes.c_uint64), ("ullAvailVirtual", ctypes.c_uint64),
                        ("ullAvailExtendedVirtual", ctypes.c_uint64)]
        m = MEMSTAT(); m.dwLength = ctypes.sizeof(MEMSTAT)
        if k.GlobalMemoryStatusEx(ctypes.byref(m)):
            s["mem"] = (m.ullTotalPhys - m.ullAvailPhys, m.ullTotalPhys)
        try:
            k.GetTickCount64.restype = ctypes.c_uint64
            s["uptime"] = k.GetTickCount64() / 1000
        except AttributeError:
            pass


def render_monitor(width, color, stats=None, sample=None, gpu_mem=None):
    """One frame of the monitor as a string."""
    C = (lambda code, t: f"\x1b[{code}m{t}\x1b[0m") if color else (lambda code, t: t)
    STATUS_COLOR = {"done": "32", "failed": "31", "timeout": "33", "cancelled": "90", "interrupted": "35"}
    clip = lambda t: t if len(t) <= width else t[:max(0, width - 1)] + "~"  # noqa: E731
    pct = lambda f: "  -" if f is None else f"{f * 100:3.0f}%"  # noqa: E731
    bw = max(10, min(30, width // 5))
    lines = []
    head = f"dropq {__version__} monitor  {ROOT}"
    stamp = dt.datetime.now().strftime("%H:%M:%S")
    lines.append(C("1", clip(head + " " * max(1, width - len(head) - len(stamp)) + stamp)))
    s = None
    if os.path.exists(STATUS):
        try:
            s = read_json(STATUS)
        except Exception:  # noqa: BLE001
            lines.append(C("33", "server  status.json is being written, retrying..."))
    else:
        lines.append(C("31", "server  never run on this root (no _dropq/status.json)"))
    reg = load_registry()
    off = [p for p, r in reg.items() if not r["enabled"]]
    if s is not None:
        age = _age(s.get("alive_at")) or 1e9
        stopped = s.get("stopped_at")
        alive = age < max(15, 5 * float(s.get("poll_s", POLL))) and not stopped
        up = _age(s.get("started_at"))
        if alive:
            srv = C("32", "ALIVE") + (f"  up {_dur(up)}" if up is not None else "")
        elif stopped:
            srv = C("31;1", f"STOPPED  {_dur(_age(stopped) or 0)} ago")
        else:
            srv = C("31;1", f"NOT RESPONDING  last heartbeat {_dur(age)} ago")
        lines.append(clip(f"server  {srv}   pid {s.get('pid')}   projects {len(reg)}"
                          + (f" ({len(off)} disabled: {', '.join(off)})" if off else "")))

    # ---- host
    m = sample or {}
    if m:
        host = []
        if m.get("uptime") is not None:
            host.append(f"up {_dur(m['uptime'])}")
        if m.get("load"):
            host.append("load " + " ".join(f"{x:.2f}" for x in m["load"]))
        if m.get("procs"):
            host.append(f"{m['procs']} procs")
        host.append(f"{m.get('ncpu')} cpus")
        lines.append(clip("host    " + "   ".join(host)))
        if m.get("cpu") is not None:
            lines.append(clip(f"cpu     [{_bar(m['cpu'], bw)}] {pct(m['cpu'])}"
                              + (f"   cores |{_spark(m['cores'])}|" if m.get("cores") else "")))
        if m.get("mem"):
            used, tot = m["mem"]
            line = f"mem     [{_bar(used / max(tot, 1), bw)}] {pct(used / max(tot, 1))}  {_size(used)}/{_size(tot)}"
            if m.get("swap") and m["swap"][1]:
                line += f"   swap {_size(m['swap'][0])}/{_size(m['swap'][1])}"
            lines.append(clip(line))
        if m.get("disk"):
            used, tot = m["disk"]
            line = f"disk    [{_bar(used / max(tot, 1), bw)}] {pct(used / max(tot, 1))}  {_size(used)}/{_size(tot)}"
            if m.get("disk_io") and m["disk_io"][0] is not None:
                line += f"   read {_size(m['disk_io'][0], True)}  write {_size(m['disk_io'][1], True)}"
            lines.append(clip(line))
        if m.get("net") and m["net"][0] is not None:
            lines.append(clip(f"net     down {_size(m['net'][0], True)}   up {_size(m['net'][1], True)}"))
        if m.get("backend") in ("windows", "basic"):
            lines.append(C("90", clip("        (pip install psutil for network, disk I/O, swap and per-job usage)")))

    # ---- GPU: ask nvidia-smi directly when it is here (fresher), else use the server's snapshot
    gpus = gpu_info() if shutil.which("nvidia-smi") else (s or {}).get("gpu")
    for g in gpus or []:
        if "error" in g:
            lines.append(clip(f"gpu     {C('33', g['error'])}")); continue
        memf = g["mem_used_mb"] / max(g["mem_total_mb"], 1)
        lines.append(clip(f"gpu     [{_bar(g['util_pct'] / 100, bw)}] {g['util_pct']:3d}%  {g['name']}"
                          f"   vram {g['mem_used_mb'] / 1024:.1f}/{g['mem_total_mb'] / 1024:.1f}G"
                          f" ({memf * 100:.0f}%)   {g['temp_c']}C"))
    if not gpus:
        lines.append(C("90", "gpu     none detected"))
    lines.append("")
    if s is None:
        return "\n".join(lines)

    # ---- jobs
    running = s.get("running", [])
    lines.append(C("1", f"RUNNING ({len(running)})"))
    for r in running:
        to = r.get("timeout_s")
        left = f"  {_dur(to - r['elapsed_s'])} left" if to else ""
        tag = C("36", " bg") if r.get("background") else "   "
        usage = ""
        j = m.get("jobs", {}).get(r.get("pid")) if m else None
        if j:
            usage = f"   cpu {pct(j['cpu']).strip()}  mem {_size(j['rss'])}"
            if j["nproc"] > 1:
                usage += f"  {j['nproc']} procs"
            g = sum((gpu_mem or {}).get(p, 0) for p in j["pids"])
            if g:
                usage += f"  vram {g / 1024:.1f}G"
        lines.append(clip(f" {C('33', '>')}{tag} {r['project']}/{r['id']}  {_dur(r['elapsed_s'])}{left}{usage}"))
        last = _last_line(os.path.join(inbox(r["project"], "logs"), r["id"] + ".log"))
        if last:
            lines.append(C("90", clip(f"       | {last}")))
    if not running:
        lines.append(C("90", "  (idle)"))
    lines.append("")
    q = s.get("queued", [])
    lines.append(C("1", f"QUEUED ({len(q)})"))
    for item in q[:5]:
        p, _, jid = item.partition("/")
        try:
            w = f"  waiting {_dur(time.time() - os.path.getmtime(os.path.join(inbox(p, 'queue'), jid + '.json')))}"
        except OSError:
            w = ""
        lines.append(clip(f"  {item}{w}" + (C("33", "  (project disabled)") if p in off else "")))
    if len(q) > 5:
        lines.append(C("90", f"  ... and {len(q) - 5} more"))
    lines.append("")
    lines.append(C("1", "RECENT"))
    recent = _recent_done(list(reg), 6)
    for mt, p, r in recent:
        st = r.get("status", "?")
        dur, rc = r.get("duration_s"), r.get("returncode")
        extra = f"  {r['error']}" if r.get("error") else ""
        lines.append(clip(f"  {C(STATUS_COLOR.get(st, '0'), f'{st:11s}')} {p}/{r.get('id')}"
                          f"  {'' if dur is None else _dur(dur)}"
                          f"{'' if rc is None else f'  rc={rc}'}  {_dur(time.time() - mt)} ago{extra}"))
    if not recent:
        lines.append(C("90", "  (nothing yet)"))
    lines.append("")
    lines.append(C("90", clip("Ctrl+C to quit  |  dropq follow ID  |  dropq cancel ID")))
    return "\n".join(lines)


def _job_pids():
    try:
        return [r["pid"] for r in read_json(STATUS).get("running", []) if r.get("pid")]
    except Exception:  # noqa: BLE001
        return []


def monitor(a):
    tty = sys.stdout.isatty()
    color = tty and not os.environ.get("NO_COLOR") and _enable_ansi()
    stats = SystemStats(ROOT)
    stats.sample(_job_pids())                       # baseline for rates and CPU percentages
    if a.once or not tty:
        time.sleep(0.5)
        print(render_monitor(shutil.get_terminal_size((100, 40)).columns, color,
                             stats, stats.sample(_job_pids()), _gpu_procs()))
        return 0
    out = sys.stdout
    out.write("\x1b[?1049h\x1b[?25l")          # alternate screen, hide cursor
    try:
        time.sleep(min(a.interval, 0.5))
        while True:
            cols, rows = shutil.get_terminal_size((100, 40))
            frame = render_monitor(cols, color, stats, stats.sample(_job_pids()), _gpu_procs()).split("\n")[:rows]
            out.write("\x1b[H" + "\n".join(f + "\x1b[K" for f in frame) + "\x1b[J")
            out.flush()
            time.sleep(a.interval)
    except KeyboardInterrupt:
        pass
    finally:
        out.write("\x1b[?25h\x1b[?1049l"); out.flush()
    return 0


# ------------------------------------------------------------------ autostart
def _server_cmd():
    py = sys.executable.replace("pythonw.exe", "python.exe")
    return [py, os.path.abspath(__file__), "--root", ROOT, "serve"]


def _autostart_path():
    if WIN:
        return os.path.join(os.environ["APPDATA"], r"Microsoft\Windows\Start Menu\Programs\Startup",
                            "dropq_startup.vbs")
    if sys.platform == "darwin":
        return os.path.expanduser("~/Library/LaunchAgents/io.github.dropq.plist")
    return os.path.expanduser("~/.config/systemd/user/dropq.service")


def install(a):
    os.makedirs(SRV, exist_ok=True)
    cmd, path = _server_cmd(), _autostart_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if WIN:
        quoted = " ".join(f'""{c}""' for c in cmd)
        text = (f'Set sh = CreateObject("WScript.Shell")\n'
                f'sh.CurrentDirectory = "{SRV}"\n'
                f'sh.Run "{quoted}", 0, False\n')
        start_now = f'wscript "{path}"'
    elif sys.platform == "darwin":
        args = "".join(f"<string>{c}</string>" for c in cmd)
        text = ('<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
                '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n<plist version="1.0"><dict>'
                '<key>Label</key><string>io.github.dropq</string>'
                f'<key>ProgramArguments</key><array>{args}</array>'
                '<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>'
                f'<key>WorkingDirectory</key><string>{SRV}</string></dict></plist>\n')
        start_now = f"launchctl load -w {path}"
    else:
        text = ("[Unit]\nDescription=dropq job server\n\n[Service]\n"
                f"ExecStart={' '.join(chr(34) + c + chr(34) for c in cmd)}\nWorkingDirectory={SRV}\n"
                "Restart=on-failure\n\n[Install]\nWantedBy=default.target\n")
        start_now = ("systemctl --user daemon-reload && systemctl --user enable --now dropq\n"
                     "  (and `loginctl enable-linger $USER` to keep it running while logged out)")
    with open(path, "w") as fh:
        fh.write(text)
    print(f"installed {path}\nserving {ROOT}\nto start it now:\n  {start_now}")


def uninstall(a):
    path = _autostart_path()
    if not os.path.exists(path):
        print("not installed"); return
    if sys.platform == "darwin":
        subprocess.run(["launchctl", "unload", "-w", path], capture_output=True)
    elif not WIN and shutil.which("systemctl"):
        subprocess.run(["systemctl", "--user", "disable", "--now", "dropq"], capture_output=True)
    os.remove(path)
    print("removed", path, "(a server that is running now keeps running until it is stopped)")


# ------------------------------------------------------------------ CLI
def main(argv=None):
    ap = argparse.ArgumentParser(prog="dropq", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", help="workspace folder (default: $DROPQ_ROOT, see above)")
    ap.add_argument("--version", action="version", version=f"dropq {__version__}")
    sp = ap.add_subparsers(dest="c", required=True)
    for c in ("serve", "install", "uninstall", "status", "projects"):
        sp.add_parser(c)
    p = sp.add_parser("init", help="create a project or update its rules")
    p.add_argument("name")
    for flag, what in (("shell", "shell jobs"), ("env", "custom environment variables"),
                       ("background", "background jobs")):
        g = p.add_mutually_exclusive_group()
        g.add_argument(f"--{flag}", dest=flag, action="store_const", const=True, help=f"allow {what}")
        g.add_argument(f"--no-{flag}", dest=flag, action="store_const", const=False, help=f"forbid {what}")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--commands", nargs="+", metavar="PROG", help="only allow these programs")
    g.add_argument("--any-command", action="store_true", help="allow any program")
    p.add_argument("--max-timeout", type=int, help="seconds; 0 = no cap")
    p.add_argument("--max-queued", type=int)
    p.add_argument("--repo", metavar="URL", help="git repo that commit jobs check out (\"\" to clear)")
    for c in ("enable", "disable"):
        sp.add_parser(c).add_argument("name")
    p = sp.add_parser("submit")
    p.add_argument("-p", "--project")
    p.add_argument("--cwd", default=".", help="folder relative to the project")
    p.add_argument("--name"); p.add_argument("--note")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="seconds, 0 = none")
    p.add_argument("--background", action="store_true")
    p.add_argument("--shell", action="store_true")
    p.add_argument("--follow", action="store_true", help="stream output until the job ends")
    p.add_argument("--commit", help="run in a checkout of the project's repo at this commit")
    p.add_argument("--artifact", action="append", metavar="GLOB",
                   help="files to copy back over a link (relative to the job's folder; repeatable)")
    p.add_argument("--not-after", metavar="TIME", help="don't start after this ISO date-time")
    p.add_argument("cmd", nargs=argparse.REMAINDER)
    p = sp.add_parser("list"); p.add_argument("-p", "--project"); p.add_argument("-n", type=int, default=20)
    p = sp.add_parser("monitor", help="live view of the server, GPU, jobs and recent results")
    p.add_argument("-i", "--interval", type=float, default=2.0, help="seconds between refreshes")
    p.add_argument("--once", action="store_true", help="print one frame and exit")
    for c in ("show", "follow", "cancel"):
        p = sp.add_parser(c); p.add_argument("id"); p.add_argument("-p", "--project")
        if c == "show":
            p.add_argument("-n", type=int, default=50)
    p = sp.add_parser("pair", help="(on the machine that runs jobs) make a key bundle for another machine")
    p.add_argument("name", help="a name for the other machine, e.g. laptop")
    p.add_argument("-p", "--project", action="append", help="a project it may use (repeatable)")
    p.add_argument("--host", help="address the other machine connects to (default: Tailscale name or hostname)")
    p.add_argument("--port", type=int, default=22)
    p.add_argument("--user", help="ssh login (default: the current user)")
    p.add_argument("--days", type=int, help="the key stops working after this many days")
    p.add_argument("-o", "--output", help="bundle file (default: NAME.dropq)")
    p = sp.add_parser("keys", help="list paired machines, or revoke one")
    p.add_argument("action", nargs="?", choices=["list", "revoke"], default="list")
    p.add_argument("name", nargs="?")
    p = sp.add_parser("link", help="(on the submitting machine) import a bundle made by dropq pair")
    p.add_argument("bundle")
    p.add_argument("--name", help="name for the link (default: the bundle's)")
    p.add_argument("--host"); p.add_argument("--port", type=int); p.add_argument("--user")
    p.add_argument("--code", help="the code shown by dropq pair (asked for if omitted)")
    p.add_argument("--keep", action="store_true", help="don't delete the bundle file afterwards")
    sp.add_parser("links", help="list links and whether they are connected")
    sp.add_parser("unlink").add_argument("name")
    p = sp.add_parser("agent", help="serve a link connection on stdin/stdout (run by sshd)")
    p.add_argument("--client", required=True)
    a = ap.parse_args(argv)
    configure(a.root)
    cmds = {"serve": lambda a: serve(), "install": install, "uninstall": uninstall, "status": status,
            "projects": projects, "init": init, "enable": lambda a: set_enabled(a, True),
            "disable": lambda a: set_enabled(a, False), "submit": submit, "list": list_done, "show": show,
            "follow": follow, "cancel": cancel, "monitor": monitor, "pair": pair, "keys": keys,
            "link": link, "links": links, "unlink": unlink, "agent": agent}
    rc = cmds[a.c](a)
    return rc if isinstance(rc, int) else 0


if __name__ == "__main__":
    sys.exit(main())
