from pathlib import Path
import datetime, json, os, signal, subprocess, sys, time
from isolate_host import prepare, restore
ROOT=Path(__file__).resolve().parent
UTC=datetime.timezone.utc

def stamp():return datetime.datetime.now(UTC).isoformat()
def save(value):
    p=ROOT/'supervisor-status.json.tmp'
    p.write_text(json.dumps(value,indent=2)+'\n')
    p.replace(ROOT/'supervisor-status.json')

status=dict(started_at=stamp(),state='isolating',pid=os.getpid())
save(status)
code=1
try:
    prepare()
    deadline=datetime.datetime.fromisoformat(json.loads((ROOT/'lease.json').read_text())['expires_at'])
    remaining=(deadline-datetime.datetime.now(UTC)).total_seconds()
    assert remaining>180, 'exclusive interval exhausted before start'
    status.update(state='building_and_measuring',isolation_ready=True)
    save(status)
    with (ROOT/'progress.log').open('x') as output:
        child=subprocess.Popen([sys.executable,str(ROOT/'build_run.py')],cwd=ROOT,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT)
        status.update(child_pid=child.pid)
        save(status)
        try: code=child.wait(timeout=remaining-90)
        except subprocess.TimeoutExpired:
            child.terminate()
            try: child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            status['timeout']=True
            code=1
    status.update(state='restoring',exit_code=code,measurement_finished_at=stamp())
    save(status)
except BaseException as error:
    status.update(state='failed',error_type=type(error).__name__,error=str(error))
    save(status)
finally:
    try:
        restore()
        status['isolation_restored']=True
    except BaseException as error:
        status.update(isolation_restored=False,restore_error=str(error))
        code=1
    status.update(state='finished',exit_code=code,finished_at=stamp())
    save(status)
sys.exit(code)
