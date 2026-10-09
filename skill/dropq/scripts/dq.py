#!/usr/bin/env python3
"""
dq - drive a dropq project inbox directly, with only file access to the project folder.

Unlike the `dropq` CLI, this needs no access to the server's root or registry: point it at a
project's _jobs folder (or the project folder itself) and it submits, waits and reads results
by reading and writing files. Standard library only.

  python dq.py INBOX status
  python dq.py INBOX submit [--cwd DIR] [--name N] [--note T] [--background] [--timeout SEC]
                            [--commit SHA] [--artifact GLOB] [--not-after TIME]
                            [--wait SEC] -- CMD ARG ...
  python dq.py INBOX wait ID [--timeout SEC] [--tail N]     exit code = the job's
  python dq.py INBOX show ID [--tail N]
  python dq.py INBOX list [-n 10]
  python dq.py INBOX cancel ID
"""
import argparse, datetime as dt, json, os, sys, time, uuid

ID_OK = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")


def inbox_path(p):
    p = os.path.abspath(p)
    if os.path.basename(p) != "_jobs" and os.path.isdir(os.path.join(p, "_jobs")):
        p = os.path.join(p, "_jobs")
    if not os.path.isdir(os.path.join(p, "queue")):
        sys.exit(f"{p} is not a dropq inbox (no queue/ folder)")
    return p


def read_json(path, tries=5):
    for i in range(tries):
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, ValueError):
            if i == tries - 1:
                raise
            time.sleep(0.3)


def server_status(ib):
    path = os.path.join(ib, "status.json")
    if not os.path.exists(path):
        return None, None
    s = read_json(path)
    age = (dt.datetime.now().astimezone() - dt.datetime.fromisoformat(s["alive_at"])).total_seconds()
    return s, age


def cmd_status(ib, a):
    s, age = server_status(ib)
    if s is None:
        print("no status.json - the server has not served this project yet"); return 1
    alive = age < 30 and not s.get("stopped_at")
    state = "ALIVE" if alive else ("STOPPED" if s.get("stopped_at") else f"NOT RESPONDING (last heartbeat {age:.0f}s ago)")
    print(f"server {state}"
          f" | project {s.get('project')} {'enabled' if s.get('enabled', True) else 'DISABLED'}"
          f" | other work running: {'yes' if s.get('server_busy') else 'no'}")
    for g in s.get("gpu") or []:
        print("gpu:", ", ".join(f"{k}={v}" for k, v in g.items()))
    for r in s.get("running", []):
        print(f"running: {r['id']} {'[bg] ' if r.get('background') else ''}{r.get('elapsed_s')}s")
    print("queued:", ", ".join(s.get("queued", [])) or "none")
    print("rules:", json.dumps(s.get("rules", {})))
    rm = s.get("remote")
    if rm:
        print(f"remote: runs on {rm.get('host')} via link {rm.get('link')} {'up' if rm.get('link_up') else 'DOWN'}"
              + (f", {rm['waiting_to_send']} job(s) waiting to be sent" if rm.get("waiting_to_send") else "")
              + ("" if rm.get("link_up") else f" - {rm.get('link_error')} (last contact {rm.get('last_contact')})")
              + ("" if not rm.get("link_up") or rm.get("server_alive") else " - its dropq server is not running"))
    return 0 if alive else 1


def new_id(name):
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    slug = "".join(c if c in ID_OK and c != "." else "-" for c in (name or "job"))[:30]
    return f"{stamp}-{slug}-{uuid.uuid4().hex[:4]}"


def find(ib, jid):
    for sub in ("done", "running", "queue"):
        p = os.path.join(ib, sub, jid + ".json")
        if os.path.exists(p):
            return sub, p
    return None, None


def tail(path, n):
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2); fh.seek(max(0, fh.tell() - 64 * 1024))
            return fh.read().decode("utf-8", "replace").splitlines()[-n:]
    except OSError:
        return []


def report(ib, jid, n):
    sub, p = find(ib, jid)
    if sub is None:
        print(f"no job {jid}"); return 2
    r = read_json(p)
    if sub != "done":
        print(f"{jid}: {sub}")
    else:
        print(f"{jid}: {r['status']} rc={r.get('returncode')} "
              f"{'' if r.get('duration_s') is None else str(r['duration_s']) + 's'}"
              f"{'  error: ' + r['error'] if r.get('error') else ''}")
    lines = tail(os.path.join(ib, "logs", jid + ".log"), n)
    if lines:
        print("--- log tail ---"); print("\n".join(lines))
    if sub != "done":
        return 3
    rc = r.get("returncode")
    return rc if isinstance(rc, int) and rc >= 0 else (0 if r["status"] == "done" else 1)


def wait(ib, jid, timeout, n):
    t0 = time.time()
    while True:
        sub, _ = find(ib, jid)
        if sub == "done" or sub is None:
            return report(ib, jid, n)
        if timeout and time.time() - t0 > timeout:
            print(f"still {sub} after {timeout}s - check again later with: wait {jid}")
            report(ib, jid, n)
            return 3
        time.sleep(2)


def cmd_submit(ib, a):
    cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
    if not cmd:
        sys.exit("give the command after --")
    job = dict(cmd=cmd, cwd=a.cwd, background=a.background, name=a.name, note=a.note,
               timeout_s=None if a.timeout == 0 else a.timeout,
               submitted_at=dt.datetime.now().astimezone().isoformat(timespec="seconds"))
    for k, v in (("commit", a.commit), ("artifacts", a.artifact), ("not_after", a.not_after)):
        if v:
            job[k] = v
    jid = new_id(a.name or os.path.basename(cmd[1] if len(cmd) > 1 else cmd[0]))
    q = os.path.join(ib, "queue", jid + ".json")
    with open(q + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(job, fh, indent=1)
    os.replace(q + ".tmp", q)
    print(jid, flush=True)
    s, age = server_status(ib)
    if s is None or age > 30 or s.get("stopped_at"):
        print("warning: the server is not responding; the job will wait in the queue", file=sys.stderr)
    if a.wait is not None:
        return wait(ib, jid, a.wait, a.tail)
    return 0


def cmd_list(ib, a):
    d = os.path.join(ib, "done")
    for f in sorted(os.listdir(d))[-a.n:]:
        if f.endswith(".json"):
            try:
                r = read_json(os.path.join(d, f))
            except Exception:  # noqa: BLE001
                continue
            print(f"{r['id']:48s} {r['status']:11s} rc={r.get('returncode')}")
    return 0


def cmd_cancel(ib, a):
    sub, _ = find(ib, a.id)
    if sub in (None, "done"):
        print(f"no queued or running job {a.id}"); return 2
    open(os.path.join(ib, "cancel", a.id), "w").close()
    print("cancel requested for", a.id)
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("inbox", help="the project's _jobs folder, or the project folder")
    sp = ap.add_subparsers(dest="c", required=True)
    sp.add_parser("status")
    p = sp.add_parser("submit")
    p.add_argument("--cwd", default="."); p.add_argument("--name"); p.add_argument("--note")
    p.add_argument("--background", action="store_true")
    p.add_argument("--timeout", type=int, default=6 * 3600, help="seconds, 0 = none")
    p.add_argument("--wait", type=int, metavar="SEC", help="wait up to SEC for the result (0 = forever)")
    p.add_argument("--tail", type=int, default=30)
    p.add_argument("--commit", help="run in a checkout of the project's repo at this commit")
    p.add_argument("--artifact", action="append", metavar="GLOB", help="files to copy back over a link")
    p.add_argument("--not-after", metavar="TIME", help="don't start after this ISO date-time")
    p.add_argument("cmd", nargs=argparse.REMAINDER)
    p = sp.add_parser("wait"); p.add_argument("id"); p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--tail", type=int, default=30)
    p = sp.add_parser("show"); p.add_argument("id"); p.add_argument("--tail", type=int, default=50)
    p = sp.add_parser("list"); p.add_argument("-n", type=int, default=10)
    p = sp.add_parser("cancel"); p.add_argument("id")
    a = ap.parse_args()
    ib = inbox_path(a.inbox)
    rc = {"status": cmd_status, "submit": cmd_submit, "list": cmd_list, "cancel": cmd_cancel,
          "wait": lambda ib, a: wait(ib, a.id, a.timeout, a.tail),
          "show": lambda ib, a: report(ib, a.id, a.tail)}[a.c](ib, a)
    sys.exit(rc)


if __name__ == "__main__":
    main()
