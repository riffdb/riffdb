from pathlib import Path
import ctypes,json,os,signal,subprocess,time,sys
root=Path(__file__).resolve().parent
trace=root/'kernel_trace.bt'
if sys.argv[1:]==['workload']:
    libc=ctypes.CDLL(None)
    counts=[]
    for role,name in enumerate((b'riffdb-journal',b'riffdb-checkpoi'),1):
        assert libc.prctl(15,name,0,0,0)==0
        p=root/f'qualification-{role}.data'
        fd=os.open(p,os.O_CREAT|os.O_EXCL|os.O_RDWR,0o600)
        try:
            for i in range(20):
                os.pwrite(fd,b'x'*4096,i*4096)
                os.fdatasync(fd)
            counts.append(dict(role=role,pid=os.getpid(),tid=os.getpid(),syncs=20,device=os.fstat(fd).st_dev,inode=os.fstat(fd).st_ino))
        finally:
            os.close(fd);p.unlink()
    (root/'kernel-qualification-expected.json').write_text(json.dumps(counts,indent=2)+'\n')
    sys.exit(0)
with (root/'kernel-qualification.log').open('x') as out,(root/'kernel-qualification.stderr').open('x') as err:
    process=subprocess.Popen(['sudo','-n','bpftrace','-q','-B','line',str(trace)],stdout=out,stderr=err,start_new_session=True)
    try:
        deadline=time.monotonic()+60
        while 'HEADER\t' not in (root/'kernel-qualification.log').read_text():
            assert process.poll() is None,'probe exited before ready'
            assert time.monotonic()<deadline,'probe startup timeout'
            time.sleep(.1)
        subprocess.run([sys.executable,__file__,'workload'],check=True,timeout=30)
    finally:
        subprocess.run(['sudo','-n','kill','-INT','--',f'-{process.pid}'],check=False)
        try:code=process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            subprocess.run(['sudo','-n','kill','-KILL','--',f'-{process.pid}'],check=False)
            code=process.wait(timeout=10)
    assert code==0,code
assert not (root/'kernel-qualification.stderr').read_text().strip(), 'probe warnings/errors'
print('kernel qualification captured')
