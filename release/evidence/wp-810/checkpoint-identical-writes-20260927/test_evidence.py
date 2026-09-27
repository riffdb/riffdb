# req: PERF-018
import copy,json,tempfile,unittest
from pathlib import Path
from validate_cell import validate,bucket_upper,STAGES

class Evidence(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        values=[1000+i*7 for i in range(32)]
        hist=dict(sample_count=32,min_ns=min(values),max_ns=max(values),mean_ns=sum(values)//32)
        for q in (50,95,99):hist[f'p{q}_ns']=bucket_upper(sorted(values)[(32*q+99)//100-1])
        writer=dict(storage_queue_duration_us=dict(count=32),logical_commands_committed=32,busy_us=2,idle_us=1)
        point=dict(logical_ops=32,server_stage_evidence=dict(writer=writer))
        point.update({f'aggregate_p{q}_ns':hist[f'p{q}_ns'] for q in (50,95,99)})
        self.report=dict(correctness=dict(clean=True),curve=[point],backends=[dict(aggregate=dict(latency=hist))])
        self.raw=dict(schema='private.wp810-request-tail/v1',valid=True,dropped_samples=0,sample_count=32,expected_sample_count=32,
            workers=[dict(worker=i,dropped_samples=0,samples_ns=[n]) for i,n in enumerate(values)],
            slowest_20=[dict(elapsed_ns=values[i],worker=i,worker_ordinal=0) for i in range(31,11,-1)])
        stages=[1000]*8+[2000]+[0]*(len(STAGES)-9)
        self.log='riffdb-writer-batch-stages-v1\ttotal=2\tlabels='+','.join(STAGES)+'\twindows='+','.join(str(n) for n in [2]+stages+[32,2])+'\n'
        self.log+='riffdb-writer-wall-probe-v1\titerations=2\tspan_ns=12000\titeration_sum_ns=10000\n'
    def check(self):
        for name,value in [('report.json',self.report),('raw.json',self.raw)]:
            (self.root/name).write_text(json.dumps(value))
        (self.root/'run.log').write_text(self.log)
        return validate(self.root/'report.json',self.root/'raw.json',self.root/'run.log')
    def test_signed_reconciliation_keeps_two_distinct_remainders(self):
        r=self.check()['independent_wall']
        self.assertEqual(r['span_minus_iterations_ns'],2000)
        self.assertEqual(r['stage_residual_ns'],2000)
        self.assertEqual(r['busy_idle_undercount_ns'],9000)
    def test_reject_negative_or_saturated_stage_remainder(self):
        self.log=self.log.replace('iteration_sum_ns=10000','iteration_sum_ns=7000')
        with self.assertRaises(AssertionError):self.check()
    def test_reject_incomplete_wall_population(self):
        self.log=self.log.replace('iterations=2','iterations=3')
        with self.assertRaises(AssertionError):self.check()
    def test_reject_histogram_raw_drift(self):
        self.raw['workers'][0]['samples_ns'][0]+=5
        with self.assertRaises(AssertionError):self.check()
    def test_reject_missing_wall_line(self):
        self.log=self.log.splitlines()[0]+'\nother\n'
        with self.assertRaises(AssertionError):self.check()

if __name__=='__main__':unittest.main()
