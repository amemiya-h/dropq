"""Links between two machines, simulated with two roots on one machine.

`dropq pair` and `dropq link` run for real (they need OpenSSH's ssh-keygen). The link then talks
to B's agent through a local subprocess instead of ssh, so no ssh server is needed.

Run with:  python -m pytest -q
"""
import json, os, shutil, subprocess, sys, time

import pytest

from test_dropq import SLEEP

DROPQ = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dropq.py")
DQ = os.path.join(os.path.dirname(DROPQ), "skill", "dropq", "scripts", "dq.py")
pytestmark = pytest.mark.skipif(not shutil.which("ssh-keygen"), reason="needs OpenSSH's ssh-keygen")


def env_for(tmp):
    return dict(os.environ, DROPQ_POLL="0.2", DROPQ_RELAY_POLL="0.2",
                DROPQ_SSH_DIR=str(tmp / "sshd"), DROPQ_AUTHORIZED_KEYS=str(tmp / "authorized_keys"))


def cli(tmp, root, *args, check=True, inp=None):
    r = subprocess.run([sys.executable, DROPQ, "--root", str(root), *args], env=env_for(tmp), input=inp,
                       capture_output=True, text=True, timeout=120, cwd=str(tmp))
    if check and r.returncode != 0:
        raise AssertionError(f"dropq {args} failed ({r.returncode}):\n{r.stdout}\n{r.stderr}")
    return r


def wait_for(path, timeout=30):
    t0 = time.time()
    while not path.exists():
        assert time.time() - t0 < timeout, f"{path} did not appear"
        time.sleep(0.1)
    time.sleep(0.1)
    return json.loads(path.read_text(encoding="utf-8"))


def submit(tmp, root, *cmd, **opts):
    args = ["submit", "-p", "proj"]
    for k, v in opts.items():
        k = k.replace("_", "-")
        if isinstance(v, list):
            for x in v:
                args += [f"--{k}", x]
        else:
            args += [f"--{k}"] if v is True else [f"--{k}", str(v)]
    return cli(tmp, root, *args, "--", *cmd).stdout.strip().splitlines()[0]


def start_server(tmp, root):
    proc = subprocess.Popen([sys.executable, DROPQ, "--root", str(root), "serve"], env=env_for(tmp),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for(root / "_dropq" / "status.json", 15)
    return proc


def make_host_key(tmp):
    (tmp / "sshd").mkdir()
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(tmp / "sshd" / "ssh_host_ed25519_key")],
                   check=True, capture_output=True)


def pair_and_link(tmp, a, b, name="laptop", project="proj"):
    out = cli(tmp, b, "pair", name, "-p", project, "--host", "home.example", "-o", str(tmp / f"{name}.dropq")).stdout
    code = [ln.strip() for ln in out.splitlines() if ln.strip().count("-") == 3 and len(ln.strip()) == 19][0]
    cli(tmp, a, "link", str(tmp / f"{name}.dropq"), "--code", code)
    # talk to B's agent through a local process instead of ssh
    lj = a / "_dropq" / "links" / name / "link.json"
    cfg = json.loads(lj.read_text(encoding="utf-8"))
    cfg["command"] = [sys.executable, DROPQ, "--root", str(b), "agent", "--client", name]
    lj.write_text(json.dumps(cfg), encoding="utf-8")
    return code


@pytest.fixture
def two(tmp_path):
    a, b = tmp_path / "A", tmp_path / "B"
    make_host_key(tmp_path)
    cli(tmp_path, b, "init", "proj")
    (b / "proj" / "sleep.py").write_text(SLEEP)
    pair_and_link(tmp_path, a, b)
    procs = [start_server(tmp_path, b), start_server(tmp_path, a)]
    yield tmp_path, a, b
    for p in procs:
        p.terminate(); p.wait(timeout=15)


def done(a, jid, timeout=30):
    return wait_for(a / "proj" / "_jobs" / "done" / f"{jid}.json", timeout)


# ---------------------------------------------------------------- pairing
def test_pair_writes_restricted_key_and_link_needs_the_code(tmp_path):
    a, b = tmp_path / "A", tmp_path / "B"
    make_host_key(tmp_path)
    cli(tmp_path, b, "init", "proj")
    out = cli(tmp_path, b, "pair", "laptop", "-p", "proj", "--host", "h", "--days", "30",
              "-o", str(tmp_path / "laptop.dropq")).stdout
    line = (tmp_path / "authorized_keys").read_text().strip()
    assert line.startswith('restrict,command="') and "agent" in open(
        line.split('command="')[1].split()[0].strip('"\\'), encoding="utf-8").read()
    assert "expiry-time=" in line and line.endswith(" dropq:laptop")
    bundle = json.loads((tmp_path / "laptop.dropq").read_text())
    assert bundle["host_keys"] and bundle["host"] == "h" and list(bundle["projects"]) == ["proj"]
    r = cli(tmp_path, a, "link", str(tmp_path / "laptop.dropq"), "--code", "AAAA-AAAA-AAAA-AAAA", check=False)
    assert r.returncode != 0 and "wrong code" in (r.stdout + r.stderr)
    assert not (a / "_dropq" / "links" / "laptop").exists() and (tmp_path / "laptop.dropq").exists()
    code = [ln.strip() for ln in out.splitlines() if ln.strip().count("-") == 3 and len(ln.strip()) == 19][0]
    cli(tmp_path, a, "link", str(tmp_path / "laptop.dropq"), "--code", code.lower().replace("-", ""))
    assert not (tmp_path / "laptop.dropq").exists()            # deleted after import
    key = a / "_dropq" / "links" / "laptop" / "key"
    pub = subprocess.run(["ssh-keygen", "-y", "-P", "", "-f", str(key)], capture_output=True, text=True)
    assert pub.returncode == 0 and pub.stdout.split()[1] == line.split()[-2]   # decrypted, same key
    assert "dropq-laptop ssh-ed25519" in (a / "_dropq" / "links" / "laptop" / "known_hosts").read_text()
    assert "remote" in cli(tmp_path, a, "projects").stdout
    assert "laptop" in cli(tmp_path, b, "keys").stdout


def test_repair_replaces_and_revoke_removes_key(tmp_path):
    b = tmp_path / "B"
    make_host_key(tmp_path)
    cli(tmp_path, b, "init", "proj")
    cli(tmp_path, b, "pair", "laptop", "-p", "proj", "-o", str(tmp_path / "1.dropq"))
    cli(tmp_path, b, "pair", "laptop", "-p", "proj", "-o", str(tmp_path / "2.dropq"))
    (tmp_path / "authorized_keys").write_text("ssh-ed25519 AAAAother me@elsewhere\n"
                                              + (tmp_path / "authorized_keys").read_text())
    assert (tmp_path / "authorized_keys").read_text().count("dropq:laptop") == 1
    cli(tmp_path, b, "keys", "revoke", "laptop")
    assert (tmp_path / "authorized_keys").read_text() == "ssh-ed25519 AAAAother me@elsewhere\n"
    assert "laptop" not in cli(tmp_path, b, "keys").stdout


def test_pair_refuses_unknown_project(tmp_path):
    make_host_key(tmp_path)
    r = cli(tmp_path, tmp_path / "B", "pair", "laptop", "-p", "nope", check=False)
    assert r.returncode != 0 and "no local project" in r.stderr


# ---------------------------------------------------------------- relaying
def test_job_runs_on_the_other_side(two):
    tmp, a, b = two
    jid = submit(tmp, a, "python", "sleep.py", "remote", "0.5", "3")
    r = done(a, jid)
    assert r["status"] == "failed" and r["returncode"] == 3 and r["submitted_by"] == "link:laptop"
    log = (a / "proj" / "_jobs" / "logs" / f"{jid}.log").read_text()
    assert "start remote" in log and "end remote" in log and "### " in log
    assert (b / "proj" / "_jobs" / "done" / f"{jid}.json").exists()
    assert not (a / "proj" / "_jobs" / "queue" / f"{jid}.json").exists()
    st = json.loads((a / "proj" / "_jobs" / "status.json").read_text())
    assert st["remote"]["link_up"] and st["remote"]["server_alive"] and "rules" in st
    assert "link laptop: UP" in cli(tmp, a, "status").stdout


def test_skill_helper_works_on_a_linked_project(two):
    tmp, a, b = two
    r = subprocess.run([sys.executable, DQ, str(a / "proj"), "submit", "--wait", "60", "--", "python", "sleep.py",
                        "viadq", "0"], capture_output=True, text=True, timeout=90)
    assert r.returncode == 0 and "end viadq" in r.stdout
    r = subprocess.run([sys.executable, DQ, str(a / "proj"), "status"], capture_output=True, text=True)
    assert "ALIVE" in r.stdout and "link laptop up" in r.stdout


def test_running_job_mirrors_and_cancels(two):
    tmp, a, b = two
    jid = submit(tmp, a, "python", "sleep.py", "long", "60", background=True, timeout=0)
    running = a / "proj" / "_jobs" / "running" / f"{jid}.json"
    wait_for(running)
    t0 = time.time()
    while "start long" not in (a / "proj" / "_jobs" / "logs" / f"{jid}.log").read_text() if (
            a / "proj" / "_jobs" / "logs" / f"{jid}.log").exists() else True:
        assert time.time() - t0 < 20, "log was not mirrored while running"
        time.sleep(0.2)
    cli(tmp, a, "cancel", jid)
    assert done(a, jid)["status"] == "cancelled"
    time.sleep(0.6)
    assert not running.exists()


def test_rules_are_enforced_on_the_other_side(two):
    tmp, a, b = two
    jid = submit(tmp, a, "echo hi", shell=True)
    r = done(a, jid)
    assert r["status"] == "failed" and "shell" in r["error"]


def test_artifacts_come_back(two):
    tmp, a, b = two
    (b / "proj" / "make.py").write_text("import os\nos.makedirs('out/sub', exist_ok=True)\n"
                                        "open('out/r.csv','w').write('a,b\\n1,2\\n')\n"
                                        "open('out/sub/big.bin','wb').write(os.urandom(3*1024*1024))\n")
    jid = submit(tmp, a, "python", "make.py", artifact=["out/**/*.csv", "out/sub/*.bin", "../../escape/*"])
    r = done(a, jid)
    assert r["status"] == "done" and sorted(r["artifacts_fetched"]) == ["out/r.csv", "out/sub/big.bin"]
    art = a / "proj" / r["artifacts_dir"]
    assert (art / "out" / "r.csv").read_text() == "a,b\n1,2\n"
    assert (art / "out" / "sub" / "big.bin").read_bytes() == (b / "proj" / "out" / "sub" / "big.bin").read_bytes()


@pytest.mark.skipif(not shutil.which("git"), reason="needs git")
def test_commit_job_runs_that_exact_commit(two):
    tmp, a, b = two
    src = tmp / "src"
    src.mkdir()
    g = ["git", "-C", str(src), "-c", "user.name=t", "-c", "user.email=t@t"]
    subprocess.run(g + ["init", "-q"], check=True)
    (src / "v.py").write_text("print('version one')\n")
    subprocess.run(g + ["add", "."], check=True); subprocess.run(g + ["commit", "-qm", "1"], check=True)
    one = subprocess.run(g + ["rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    (src / "v.py").write_text("print('version two')\n")
    subprocess.run(g + ["commit", "-qam", "2"], check=True)
    two_ = subprocess.run(g + ["rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    cli(tmp, b, "init", "proj", "--repo", str(src))
    j1 = submit(tmp, a, "python", "v.py", commit=one)
    j2 = submit(tmp, a, "python", "v.py", commit=two_[:10])
    r1, r2 = done(a, j1, 60), done(a, j2, 60)
    assert r1["status"] == "done" and "version one" in "\n".join(r1["log_tail"])
    assert r2["status"] == "done" and "version two" in "\n".join(r2["log_tail"])
    assert r1["run_dir"].replace("\\", "/") == f"_src/{one[:12]}"
    bad = submit(tmp, a, "python", "v.py", commit="0" * 40)
    assert "not found" in done(a, bad, 60)["error"]


def test_commit_job_needs_a_repo(two):
    tmp, a, b = two
    jid = submit(tmp, a, "python", "v.py", commit="abcdef1")
    assert "no repo" in done(a, jid)["error"]


def test_revoked_key_stops_the_link(two):
    tmp, a, b = two
    cli(tmp, b, "keys", "revoke", "laptop")
    jid = submit(tmp, a, "python", "sleep.py", "x", "0")
    t0 = time.time()
    while True:
        st = json.loads((a / "proj" / "_jobs" / "status.json").read_text())
        if not st["remote"]["link_up"] and "revoked" in (st["remote"]["link_error"] or ""):
            break
        assert time.time() - t0 < 20, st
        time.sleep(0.2)
    assert (a / "proj" / "_jobs" / "queue" / f"{jid}.json").exists()


# ---------------------------------------------------------------- link down
def test_link_down_keeps_jobs_and_expires_them(tmp_path):
    a, b = tmp_path / "A", tmp_path / "B"
    make_host_key(tmp_path)
    cli(tmp_path, b, "init", "proj")
    pair_and_link(tmp_path, a, b)
    lj = a / "_dropq" / "links" / "laptop" / "link.json"
    cfg = json.loads(lj.read_text())
    cfg["command"] = [sys.executable, "-c", "import sys; sys.stderr.write('connection refused\\n'); sys.exit(255)"]
    lj.write_text(json.dumps(cfg))
    srv = start_server(tmp_path, a)
    try:
        soon = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() + 2))
        keep = submit(tmp_path, a, "python", "-c", "1")
        exp = submit(tmp_path, a, "python", "-c", "1", not_after=soon)
        gone = submit(tmp_path, a, "python", "-c", "1")
        cli(tmp_path, a, "cancel", gone)
        assert "expired" in done(a, exp, 20)["error"]
        assert done(a, gone, 20)["status"] == "cancelled"
        st = json.loads((a / "proj" / "_jobs" / "status.json").read_text())
        assert not st["remote"]["link_up"] and "connection refused" in st["remote"]["link_error"]
        assert st["queued"] == [keep] and st["remote"]["waiting_to_send"] == 1
        r = subprocess.run([sys.executable, DQ, str(a / "proj"), "status"], capture_output=True, text=True)
        assert "NOT RESPONDING" in r.stdout and "link laptop DOWN" in r.stdout
        assert "DOWN" in cli(tmp_path, a, "links").stdout
    finally:
        srv.terminate(); srv.wait(timeout=15)


def test_unlink_removes_projects_and_key(tmp_path):
    a, b = tmp_path / "A", tmp_path / "B"
    make_host_key(tmp_path)
    cli(tmp_path, b, "init", "proj")
    pair_and_link(tmp_path, a, b)
    r = cli(tmp_path, a, "init", "proj", check=False)
    assert r.returncode != 0 and "rules are set there" in r.stderr
    cli(tmp_path, a, "unlink", "laptop")
    assert not (a / "_dropq" / "links" / "laptop").exists()
    assert "no projects" in cli(tmp_path, a, "projects").stdout
