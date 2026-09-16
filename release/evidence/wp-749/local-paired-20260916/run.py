from pathlib import Path
import hashlib,json,os,subprocess,time
inputs=Path('/tmp/wp749-clean-measure-inputs')
root=Path('/home/user/tmp/wp749-local-paired-20260916')
root.mkdir(exist_ok=False)
(root/'tmp').mkdir()
identities=json.loads((inputs/'identities.json').read_text())
runner=inputs/'riffdb-app-baseline'
current=Path('/home/user/tmp/wp749-measure-current')
control=Path('/home/user/tmp/wp749-prefix-cost-control')
cells=[('prefix-control',control,'control',None),('prefix-current',current,'current',None),('archive-disabled',current,'current','disabled'),('archive-enabled',current,'current','enabled')]
args=['--full','--load','write_only','--load-clients','32','--load-duration-secs','90','--load-warmup-secs','15','--reps','3','--require-stable','--skip-postgres','--database-root','/home/user/tmp/wp749-measure-db']
protocol={'purpose':'WP-749 local same-workload disclosure; not WP-750 N1/E2 campaign qualification','identities':identities,'cells':[{'name':n,'source':identities[v]['source_revision'],'archive_mode':m} for n,c,v,m in cells],'arguments':args,'storage_profile':'standard','order':'fixed listed order, no performance-selected retries','on_failure':'retain all output and stop before the next cell','runner_script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'comparison_threshold':'none added; preserve existing within-cell stability checks; publish ratios'}
(root/'protocol.json').write_text(json.dumps(protocol,indent=2)+'\n')
for name,cwd,variant,mode in cells:
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=cwd)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=cwd).decode().strip()==identities[variant]['source_revision']
 daemon=inputs/(variant+'-riffdbd')
 assert hashlib.sha256(daemon.read_bytes()).hexdigest()==identities[variant]['daemon_sha256']
 assert hashlib.sha256(runner.read_bytes()).hexdigest()==identities['harness']['runner_sha256']
 env={'PATH':'/home/user/.cargo/bin:/usr/bin:/bin','HOME':'/home/user','CARGO_HOME':'/home/user/.cargo','LC_ALL':'C','TZ':'UTC','RIFFDB_TMP_ROOT':str(root/'tmp'),'RIFFDB_REDB_COMMIT_PROFILE':'standard','RIFFDB_APP_BASELINE_RIFFDBD_BIN':str(daemon),'RIFFDB_APP_BASELINE_RUNNER_BIN':str(runner)}
 if mode is not None: env['RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION']=mode
 command=[str(cwd/'benchmarks/run-app-baseline'),*args,'--output',str(root/(name+'.json'))]
 started=time.time()
 print('starting',name,flush=True)
 with (root/(name+'.log')).open('xb') as log:
  result=subprocess.run(command,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT)
 receipt={'cell':name,'exit_code':result.returncode,'elapsed_seconds':time.time()-started,'command':command}
 (root/(name+'-execution.json')).write_text(json.dumps(receipt,indent=2)+'\n')
 print('finished',name,'exit',result.returncode,flush=True)
 if result.returncode: raise SystemExit(result.returncode)
print('all four cells complete',flush=True)
