from pathlib import Path
import ipaddress, json, os, shutil, subprocess, sys

ROOT = Path(__file__).resolve().parent
STATE = ROOT/'host-isolation.json'
CHAIN = 'WP810_SSH'
UNITS = ['google-osconfig-agent.service', 'apt-daily.timer', 'apt-daily-upgrade.timer']

def run(command, check=True):
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check)

def sudo(*args, check=True):
    return run(['sudo','-n',*args], check=check)

def save(state):
    temporary = ROOT/'host-isolation.json.tmp'
    temporary.write_text(json.dumps(state, indent=2)+'\n')
    temporary.replace(STATE)

def prepare():
    assert not STATE.exists(), 'isolation already has a recorded owner'
    client = str(ipaddress.ip_address(os.environ['SSH_CONNECTION'].split()[0]))
    state = dict(client=client, chain=CHAIN, families=[], units={}, restored=False)
    save(state)
    # Do not interrupt an already-running package update.
    for unit in ['apt-daily.service','apt-daily-upgrade.service']:
        assert run(['systemctl','is-active',unit], check=False).stdout.strip() != 'active', unit
    for unit in UNITS:
        before = run(['systemctl','is-active',unit], check=False).stdout.strip()
        state['units'][unit] = before
        save(state)
        if before == 'active':
            sudo('systemctl','stop',unit)
    for tool,version in [('iptables',4),('ip6tables',6)]:
        assert shutil.which(tool), tool
        existing = sudo(tool,'-S',CHAIN,check=False)
        assert existing.returncode != 0, 'refuse to replace another isolation owner'
        (ROOT/(tool+'-before.txt')).write_text(sudo(tool+'-save').stdout)
        sudo(tool,'-N',CHAIN)
        state['families'].append(tool)
        save(state)
        if ipaddress.ip_address(client).version == version:
            sudo(tool,'-A',CHAIN,'-s',client,'-j','RETURN')
        sudo(tool,'-A',CHAIN,'-j','DROP')
        sudo(tool,'-I','INPUT','1','-p','tcp','--dport','22','-j',CHAIN)
    state['ready'] = True
    save(state)

def restore():
    if not STATE.exists():
        return
    state = json.loads(STATE.read_text())
    if state['restored']:
        return
    results = []
    for tool in reversed(state['families']):
        # Remove only this task's jump/chain, not a whole firewall snapshot.
        jump = sudo(tool,'-C','INPUT','-p','tcp','--dport','22','-j',CHAIN,check=False)
        if jump.returncode == 0:
            sudo(tool,'-D','INPUT','-p','tcp','--dport','22','-j',CHAIN)
        sudo(tool,'-F',CHAIN)
        sudo(tool,'-X',CHAIN)
        results.append(tool)
    for unit,before in state['units'].items():
        if before == 'active':
            sudo('systemctl','start',unit)
    state.update(restored=True, restored_families=results,
        units_after={u:run(['systemctl','is-active',u],check=False).stdout.strip() for u in state['units']})
    save(state)

if __name__ == '__main__':
    if sys.argv[1:] == ['restore']:
        restore()
    else:
        try:
            prepare()
        except BaseException:
            restore()
            raise
