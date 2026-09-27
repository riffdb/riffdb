# req: PERF-018
import tempfile,unittest
from pathlib import Path
from analyze_kernel import parse_trace,summarize
from join_native import pair_calls

FIXTURE='''HEADER\t1\t100
SUB\t1\t2\t1\t1\t120\t160\t0\t0
SUB\t1\t2\t1\t2\t150\t180\t0\t7
EXT4\t1\t2\t1\t110\t190\t271581185\t4\t0
SYNC\t1\t2\t1\t3\t1\t105\t195\t0\t271581185\t4
FOOTER\t1\t200
@counts[1]: 1
@counts[2]: 1
@counts[3]: 1
@counts[4]: 1
@counts[5]: 1
@counts[6]: 1
@counts[7]: 1
@counts[8]: 1
'''
class KernelEvidenceTests(unittest.TestCase):
    def parse(self,text=FIXTURE,err=''):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);(p/'log').write_text(text);(p/'err').write_text(err)
            return parse_trace(p/'log',p/'err')
    def test_overlapping_subcalls_are_unioned(self):
        r=self.parse();self.assertEqual(r['syncs'][0]['unattributed_ns'],30)
    def test_missing_footer_refused(self):
        with self.assertRaises(AssertionError):self.parse(FIXTURE.replace('FOOTER\t1\t200\n',''))
    def test_dropped_record_count_refused(self):
        with self.assertRaises(AssertionError):self.parse(FIXTURE.replace('@counts[2]: 1','@counts[2]: 2'))
    def test_orphan_subcall_refused(self):
        with self.assertRaises(AssertionError):self.parse(FIXTURE.replace('SUB\t1\t2\t1\t2','SUB\t1\t2\t2\t2'))
    def test_stage_outside_syscall_refused(self):
        with self.assertRaises(AssertionError):self.parse(FIXTURE.replace('\t150\t180\t','\t150\t210\t'))
    def test_lost_event_warning_refused(self):
        with self.assertRaises(AssertionError):self.parse(err='Lost 1 events')
    def test_failed_sync_refused(self):
        with self.assertRaises(AssertionError):self.parse(FIXTURE.replace('\t105\t195\t0\t','\t105\t195\t-5\t'))
    def test_duplicate_identity_refused(self):
        row=[r for r in FIXTURE.splitlines() if r.startswith('SYNC')][0]
        with self.assertRaises(AssertionError):self.parse(FIXTURE+row+'\n')
    def test_incomplete_stack_refused(self):
        with self.assertRaises(AssertionError):self.parse(FIXTURE+'STACK\t1\t2\t1\t1\nframe\n')
    def test_clock_pairing_requires_one_common_enclosing_offset(self):
        calls=[dict(start_ns=1015,end_ns=1025),dict(start_ns=1055,end_ns=1075)]
        pairs,width=pair_calls({1:(10,30),2:(50,80)},calls)
        self.assertEqual(width,10);self.assertEqual(list(pairs),[1,2])
        calls[1]=dict(start_ns=1255,end_ns=1275)
        with self.assertRaises(AssertionError):pair_calls({1:(10,30),2:(50,80)},calls)
    def test_same_population_does_not_admit_an_overlong_kernel_call(self):
        with self.assertRaises(AssertionError):pair_calls({1:(10,30)},[dict(start_ns=1010,end_ns=1040)])
    def test_clock_pairing_refuses_missing_calls(self):
        with self.assertRaises(AssertionError):pair_calls({1:(10,30)},[])
if __name__=='__main__':unittest.main()
