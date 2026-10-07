"""End-to-end tests: start a real server on a temporary root and drive it through the CLI.

Run with:  python -m pytest -q
"""
import json, os, subprocess, sys, time

import pytest

DROPQ = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dropq.py")
ENV = dict(os.environ, DROPQ_POLL="0.2")
SLEEP = ("import sys, time\nprint('start', sys.argv[1], flush=True)\ntime.sleep(float(sys.argv[2]))\n"
         "print('end', sys.argv[1])\nsys.exit(int(sys.argv[3]) if len(sys.argv) > 3 else 0)\n")


def cli(root, *args, check=True):
    r = subprocess.run([sys.executable, DROPQ, "--root", str(root), *args], env=ENV,
                       capture_output=True, text=True, timeout=60)
    if check and r.returncode != 0:
        raise AssertionError(f"dropq {args} failed ({r.returncode}):\n{r.stdout}\n{r.stderr}")
    return r


def result(root, project, jid, timeout=20):
    p = root / project / "_jobs" / "done" / f"{jid}.json"
    t0 = time.time()
    while not p.exists():
        if time.time() - t0 > timeout:
            raise AssertionError(f"job {project}/{jid} did not finish")
        time.sleep(0.1)
    time.sleep(0.1)
    return json.loads(p.read_text(encoding="utf-8"))


def submit(root, project, *cmd, **opts):
    args = ["submit", "-p", project]
    for k, v in opts.items():
        args += [f"--{k}"] if v is True else [f"--{k}", str(v)]
    return cli(root, *args, "--", *cmd).stdout.strip().splitlines()[0]


def drop(root, project, jid, job):
    """Write a raw job file the way any other tool would."""
    q = root / project / "_jobs" / "queue"
    q.mkdir(parents=True, exist_ok=True)
    (q / f"{jid}.json.tmp").write_text(json.dumps(job), encoding="utf-8")
    os.replace(q / f"{jid}.json.tmp", q / f"{jid}.json")


@pytest.fixture
def server(tmp_path):
    cli(tmp_path, "init", "proj", "--shell", "--env")
    cli(tmp_path, "init", "other")
    cli(tmp_path, "init", "locked", "--no-background", "--commands", "python", "--max-timeout", "1")
    for p in ("proj", "other", "locked"):
        (tmp_path / p / "sleep.py").write_text(SLEEP)
    proc = subprocess.Popen([sys.executable, DROPQ, "--root", str(tmp_path), "serve"], env=ENV,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    t0 = time.time()
    while not (tmp_path / "_dropq" / "status.json").exists():
        assert time.time() - t0 < 10, "server did not start"
        time.sleep(0.1)
    yield tmp_path
    proc.terminate()
    proc.wait(timeout=10)


# ---------------------------------------------------------------- basics
def test_success_and_failure(server):
    ok = submit(server, "proj", "python", "sleep.py", "a", "0")
    bad = submit(server, "proj", "python", "sleep.py", "b", "0", "3")
    r = result(server, "proj", ok)
    assert r["status"] == "done" and r["returncode"] == 0 and "end a" in r["log_tail"]
    r = result(server, "proj", bad)
    assert r["status"] == "failed" and r["returncode"] == 3


def test_foreground_jobs_run_in_submission_order_across_projects(server):
    ids = [("proj", submit(server, "proj", "python", "sleep.py", "1", "0.3")),
           ("other", submit(server, "other", "python", "sleep.py", "2", "0.3")),
           ("proj", submit(server, "proj", "python", "sleep.py", "3", "0.3"))]
    for p, j in ids:
        result(server, p, j)
    log = (server / "_dropq" / "server.log").read_text(encoding="utf-8")
    assert sorted(ids, key=lambda pj: log.index(f"{pj[0]}/{pj[1]}: started")) == ids


def test_background_does_not_block_queue(server):
    bg = submit(server, "proj", "python", "sleep.py", "bg", "30", background=True, timeout=0)
    fg = submit(server, "proj", "python", "sleep.py", "fg", "0")
    assert result(server, "proj", fg)["status"] == "done"
    cli(server, "cancel", bg)
    assert result(server, "proj", bg)["status"] == "cancelled"


def test_timeout_and_cancel_queued(server):
    slow = submit(server, "proj", "python", "sleep.py", "slow", "30", timeout=1)
    queued = submit(server, "proj", "python", "sleep.py", "q", "0")
    cli(server, "cancel", queued)
    assert result(server, "proj", slow)["status"] == "timeout"
    assert result(server, "proj", queued)["status"] == "cancelled"


def test_follow_streams_and_returns_exit_code(server):
    r = cli(server, "submit", "-p", "proj", "--follow", "--", "python", "sleep.py", "f", "0.5", "4", check=False)
    assert "start f" in r.stdout and "end f" in r.stdout and r.returncode == 4


def test_shell_and_env_when_allowed(server):
    sh = submit(server, "proj", "echo shell-ok", shell=True)
    assert "shell-ok" in result(server, "proj", sh)["log_tail"]
    drop(server, "proj", "envjob", {"cmd": ["python", "-c", "import os; print(os.environ['DQ_X'])"],
                                    "env": {"DQ_X": "env-ok"}})
    assert "env-ok" in result(server, "proj", "envjob")["log_tail"]


def test_status(server):
    assert "ALIVE" in cli(server, "status").stdout
    ps = json.loads((server / "proj" / "_jobs" / "status.json").read_text(encoding="utf-8"))
    assert ps["project"] == "proj" and "queued" in ps


def test_second_server_refused(server):
    r = subprocess.run([sys.executable, DROPQ, "--root", str(server), "serve"], env=ENV,
                       capture_output=True, text=True, timeout=20)
    assert r.returncode != 0 and "already running" in (r.stdout + r.stderr)


# ---------------------------------------------------------------- isolation and rules
def test_unregistered_folder_is_ignored(server):
    (server / "rogue").mkdir()
    (server / "rogue" / "sleep.py").write_text(SLEEP)
    drop(server, "rogue", "sneaky", {"cmd": ["python", "sleep.py", "x", "0"]})
    time.sleep(1.5)
    assert (server / "rogue" / "_jobs" / "queue" / "sneaky.json").exists()
    assert not (server / "rogue" / "_jobs" / "done").exists()


def test_cannot_run_in_another_project_or_outside(server):
    for jid, cwd in (("sib", "../proj"), ("up", ".."), ("inbox", "_jobs")):
        drop(server, "other", jid, {"cmd": ["python", "-c", "1"], "cwd": cwd})
    assert "outside the project" in result(server, "other", "sib")["error"]
    assert "outside the project" in result(server, "other", "up")["error"]
    assert "inbox" in result(server, "other", "inbox")["error"]


def test_default_rules_forbid_shell_and_env(server):
    drop(server, "other", "sh", {"cmd": "echo hi", "shell": True})
    drop(server, "other", "env", {"cmd": ["python", "-c", "1"], "env": {"PATH": "x"}})
    assert "shell" in result(server, "other", "sh")["error"]
    assert "environment" in result(server, "other", "env")["error"]


def test_program_rules(server):
    drop(server, "locked", "git", {"cmd": ["git", "--version"]})
    drop(server, "locked", "bg", {"cmd": ["python", "-c", "1"], "background": True})
    capped = submit(server, "locked", "python", "sleep.py", "c", "30", timeout=0)
    assert "not allowed" in result(server, "locked", "git")["error"]
    assert "background" in result(server, "locked", "bg")["error"]
    r = result(server, "locked", capped)
    assert r["status"] == "timeout" and any("capped" in line for line in r["log_tail"])


def test_programs_resolve_on_server_path_not_project(server):
    drop(server, "other", "missing", {"cmd": ["surely-not-a-program-xyz"]})
    drop(server, "other", "escape", {"cmd": ["../proj/sleep.py"]})
    assert "not found" in result(server, "other", "missing")["error"]
    assert "inside the project" in result(server, "other", "escape")["error"]


def test_disable_pauses_and_enable_resumes(server):
    cli(server, "disable", "other")
    jid = submit(server, "other", "python", "sleep.py", "d", "0")
    time.sleep(1.2)
    assert not (server / "other" / "_jobs" / "done" / f"{jid}.json").exists()
    cli(server, "enable", "other")
    assert result(server, "other", jid)["status"] == "done"


def test_bad_job_files(server):
    q = server / "other" / "_jobs" / "queue"
    (q / "not a valid id!.json").write_text(json.dumps({"cmd": ["python", "-c", "1"]}))
    (q / "notadict.json").write_text("[1, 2]")
    assert "invalid job id" in result(server, "other", "not_a_valid_id_")["error"]
    assert "JSON object" in result(server, "other", "notadict")["error"]


def test_init_validates_names(tmp_path):
    r = cli(tmp_path, "init", "_dropq", check=False)
    assert r.returncode != 0
    r = cli(tmp_path, "init", "../evil", check=False)
    assert r.returncode != 0


@pytest.mark.skipif(os.name == "nt", reason="creating symlinks needs extra rights on Windows")
def test_symlink_out_of_project_rejected(server):
    os.symlink(server / "proj", server / "other" / "link")
    drop(server, "other", "via-link", {"cmd": ["python", "-c", "1"], "cwd": "link"})
    assert "outside the project" in result(server, "other", "via-link")["error"]


# ---------------------------------------------------------------- the skill's inbox helper
DQ = os.path.join(os.path.dirname(DROPQ), "skill", "dropq", "scripts", "dq.py")


def dq(*args):
    return subprocess.run([sys.executable, DQ, *args], capture_output=True, text=True, timeout=60)


def test_skill_helper_with_inbox_access_only(server):
    inbox = str(server / "proj")
    assert "ALIVE" in dq(inbox, "status").stdout
    r = dq(inbox, "submit", "--name", "viadq", "--wait", "30", "--", "python", "sleep.py", "dq", "0", "2")
    assert r.returncode == 2 and "end dq" in r.stdout and "failed rc=2" in r.stdout
    r = dq(inbox, "submit", "--wait", "30", "--", "surely-not-a-program-xyz")
    assert "not found" in r.stdout
