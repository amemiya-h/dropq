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
  dropq list [-p NAME] [-n 20]         recent finished jobs
  dropq show ID / follow ID / cancel ID

-p defaults to the project the current directory is in.

Job file fields: cmd (list, or string with "shell": true), cwd (relative to the project),
timeout_s (default 6 h; null = none), background, env (dict), name, note.
Foreground jobs run one at a time across all projects, in submission order; background jobs
start immediately. "python"/"python3"/"py" means the Python running the server; other programs
are resolved on the server's PATH (or must be a path inside the project), never on the job's.

Security: dropq controls which jobs start, where and how - it is not a sandbox. A job runs with
the permissions of the user running the server and its code can touch anything that user can.
Give a project's inbox only to people and tools you would let run code as that user; for
untrusted code use a separate OS account, a VM or a container.
"""
import argparse, datetime as dt, json, os, re, shutil, signal, subprocess, sys, time, uuid

__version__ = "0.2.0"
WIN = os.name == "nt"
POLL = float(os.environ.get("DROPQ_POLL", "2"))   # seconds between server checks
DEFAULT_TIMEOUT = 6 * 3600
MAX_JOB_FILE = 64 * 1024
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
INBOX_DIRS = ("queue", "running", "done", "logs", "cancel")
DEFAULT_RULES = dict(enabled=True, allow_shell=False, allow_env=False, allow_background=True,
                     commands=None, max_timeout_s=None, max_queued=100)

ROOT = SRV = STATUS = LOCK = SERVER_LOG = REGISTRY = None


def configure(root=None):
    global ROOT, SRV, STATUS, LOCK, SERVER_LOG, REGISTRY
    if root is None:
        root = os.environ.get("DROPQ_ROOT")
    if root is None:
        mod_dir = os.path.dirname(os.path.abspath(__file__))
        root = os.path.dirname(mod_dir) if os.path.basename(mod_dir) == "_dropq" else os.getcwd()
    ROOT = os.path.realpath(os.path.expanduser(root))
    SRV = os.path.join(ROOT, "_dropq")
    STATUS, LOCK = os.path.join(SRV, "status.json"), os.path.join(SRV, "server.lock")
    SERVER_LOG, REGISTRY = os.path.join(SRV, "server.log"), os.path.join(SRV, "projects.json")


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


def check_job(project, job, rules):
    """Apply the project's rules. Returns (cwd, cmd, shell, env, timeout, notes)."""
    if not isinstance(job, dict) or "cmd" not in job:
        raise Rejected("job file needs a 'cmd'")
    pdir = os.path.join(ROOT, project)
    cwd = os.path.join(pdir, str(job.get("cwd") or "."))
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


def start(project, jid, job, rules):
    cwd, cmd, shell, env, timeout, notes = check_job(project, job, rules)
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


def serve():
    with Lock():
        say(f"dropq {__version__} serving {ROOT} (python {sys.executable})")
        recovered, running, gpu, gpu_t = set(), [], None, 0.0
        try:
            while True:
                reg = load_registry()
                active = [p for p in reg if os.path.isdir(os.path.join(ROOT, p))]
                for p in active:
                    make_inbox(p)
                    if p not in recovered:
                        recover(p); recovered.add(p)
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
                # cancelled while still queued; clear stray cancel files
                for p in active:
                    cdir = inbox(p, "cancel")
                    for f in os.listdir(cdir):
                        q = os.path.join(inbox(p, "queue"), f + ".json")
                        if os.path.exists(q):
                            os.remove(q); reject(p, f, {}, "cancelled before it started", "cancelled")
                        if not any(r.project == p and r.jid == f for r in running):
                            try:
                                os.remove(os.path.join(cdir, f))
                            except OSError:
                                pass
                # start jobs in global submission order; foreground ones one at a time
                busy = any(not r.job.get("background") for r in running)
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
                        r = start(p, jid, job, rules)
                    except Rejected as e:
                        reject(p, jid, job, str(e)); continue
                    job["pid"] = r.proc.pid
                    write_json(rf, job)
                    running.append(r)
                    say(f"{p}/{jid}: started {'(background) ' if bg else ''}{job.get('cmd')}")
                    if not bg:
                        busy = True
                # heartbeats: a full one for the owner, a project-only one inside each inbox
                if time.time() - gpu_t > 30:
                    gpu, gpu_t = gpu_info(), time.time()
                stamp, fg_busy = now(), any(not r.job.get("background") for r in running)
                try:
                    write_json(STATUS, dict(
                        alive_at=stamp, pid=os.getpid(), root=ROOT, python=sys.executable, version=__version__,
                        running=[dict(project=r.project, id=r.jid, name=r.job.get("name"),
                                      background=bool(r.job.get("background")), pid=r.proc.pid,
                                      elapsed_s=round(time.time() - r.t0)) for r in running],
                        queued=[f"{p}/{j}" for _, p, j in sorted(i for p in active for i in queued(p))],
                        projects={p: dict(enabled=reg[p]["enabled"]) for p in reg}, gpu=gpu), tries=1)
                except OSError:
                    pass
                for p in active:
                    try:
                        write_json(os.path.join(inbox(p), "status.json"), dict(
                            alive_at=stamp, project=p, enabled=reg[p]["enabled"], server_busy=fg_busy,
                            running=[dict(id=r.jid, background=bool(r.job.get("background")),
                                          elapsed_s=round(time.time() - r.t0)) for r in running if r.project == p],
                            queued=[j for _, _, j in sorted(queued(p))], gpu=gpu,
                            rules={k: v for k, v in reg[p].items() if k != "enabled"}), tries=1)
                    except OSError:
                        pass
                time.sleep(POLL)
        except KeyboardInterrupt:
            say("stopping: cancelling running jobs")
            for r in running:
                kill_tree(r.proc); r.proc.wait(); finish(r, "interrupted", r.proc.returncode)


# ------------------------------------------------------------------ owner commands
def init(a):
    if not NAME_RE.match(a.name) or a.name.startswith(("_", ".")):
        sys.exit("project names: letters, digits, _ . - (not starting with _ or .)")
    reg = load_registry()
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
    new = a.name not in reg
    reg[a.name] = rules
    os.makedirs(os.path.join(ROOT, a.name), exist_ok=True)
    make_inbox(a.name)
    save_registry(reg)
    print(f"{'created' if new else 'updated'} project {a.name} at {os.path.join(ROOT, a.name)}")
    print_rules(a.name, rules)


def print_rules(name, r):
    yn = lambda b: "yes" if b else "no"  # noqa: E731
    cmds = "any" if r["commands"] is None else " ".join(r["commands"])
    mt = "none" if r["max_timeout_s"] is None else f"{r['max_timeout_s']}s"
    print(f"  {name:20s} {'enabled ' if r['enabled'] else 'DISABLED'} shell={yn(r['allow_shell'])} "
          f"env={yn(r['allow_env'])} background={yn(r['allow_background'])} programs={cmds} "
          f"max_timeout={mt} max_queued={r['max_queued']}")


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
    alive = age < max(15, 5 * POLL)
    print(f"server {'ALIVE' if alive else f'NOT RESPONDING (last seen {age:.0f}s ago)'} | {s['root']}")
    for g in s.get("gpu") or []:
        print("gpu:", ", ".join(f"{k}={v}" for k, v in g.items()))
    for r in s["running"]:
        print(f"running: {r['project']}/{r['id']} {'[bg] ' if r['background'] else ''}{r['elapsed_s']}s")
    print("queued:", ", ".join(s["queued"]) or "none")
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
    p.add_argument("cmd", nargs=argparse.REMAINDER)
    p = sp.add_parser("list"); p.add_argument("-p", "--project"); p.add_argument("-n", type=int, default=20)
    for c in ("show", "follow", "cancel"):
        p = sp.add_parser(c); p.add_argument("id"); p.add_argument("-p", "--project")
        if c == "show":
            p.add_argument("-n", type=int, default=50)
    a = ap.parse_args(argv)
    configure(a.root)
    cmds = {"serve": lambda a: serve(), "install": install, "uninstall": uninstall, "status": status,
            "projects": projects, "init": init, "enable": lambda a: set_enabled(a, True),
            "disable": lambda a: set_enabled(a, False), "submit": submit, "list": list_done, "show": show,
            "follow": follow, "cancel": cancel}
    rc = cmds[a.c](a)
    return rc if isinstance(rc, int) else 0


if __name__ == "__main__":
    sys.exit(main())
