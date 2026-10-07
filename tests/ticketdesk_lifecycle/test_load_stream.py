# req: END-007, END-008, END-010
"""A complete day is bounded by windows, without truncating actual observations."""
import io
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from lib.ticketdesk_lifecycle_load_stream import CampaignLoadStream, LoadStreamError


def ready():
    return dict(schema='riffdb.ticketdesk-lifecycle-load-ready/v1',workers=28,queue_slots=32768)


def window(index):
    return dict(schema='riffdb.ticketdesk-lifecycle-load-probe/v1',start_s=index*300,end_s=(index+1)*300,
        offered_commands=30000,offered_reads=120000,completed_commands=30000,completed_reads=120000,
        settled_submission_attempts=150000,transport_attempts=150000,retries=0,p95_us=500,
        p99_us=900,load_peak_in_flight=28,reserved_auxiliary_slots=4,latency_overflow=0)


def stopped(count):
    return dict(schema='riffdb.ticketdesk-lifecycle-load-stopped/v1',completed_updates=count*30000,
        completed_reads=count*120000,post_duration_updates=0,post_duration_reads=0)


def encoded(value): return json.dumps(value,sort_keys=True).encode()+b'\n'


class LoadStream(unittest.TestCase):
    def test_all_288_windows_can_exceed_the_small_probe_byte_ceiling(self):
        lines=[encoded(ready()),*(encoded(window(index)) for index in range(288)),encoded(stopped(288))]
        self.assertGreater(sum(map(len,lines)),65536)
        source=io.BytesIO(b''.join(lines));observations=CampaignLoadStream(duration_s=86400)
        seen=[]
        while (value:=observations.read(source)) is not None: seen.append(value)
        self.assertEqual(len(seen),290)
        self.assertEqual(seen[-1],stopped(288))
        self.assertTrue(observations.complete)

    def test_incomplete_day_cannot_be_completed_by_eof(self):
        observations=CampaignLoadStream(duration_s=86400)
        source=io.BytesIO(encoded(ready())+encoded(window(0)))
        observations.read(source);observations.read(source)
        with self.assertRaisesRegex(LoadStreamError,'^load_stream_incomplete$'):observations.read(source)

    def test_duplicate_or_skipped_or_over_budget_window_refuses(self):
        for index in (0,2,288):
            observations=CampaignLoadStream(duration_s=86400)
            source=io.BytesIO(encoded(ready())+encoded(window(0))+encoded(window(index)))
            observations.read(source);observations.read(source)
            with self.assertRaises(LoadStreamError):observations.read(source)

    def test_invented_attempt_accounting_or_auxiliary_bypass_refuses(self):
        for field,value in (('transport_attempts',149999),('settled_submission_attempts',150001),
                            ('reserved_auxiliary_slots',3),('load_peak_in_flight',29),
                            ('completed_commands',200001),('retries',True),('p99_us',1.5)):
            observed=window(0);observed[field]=value
            observations=CampaignLoadStream(duration_s=86400)
            source=io.BytesIO(encoded(ready())+encoded(observed));observations.read(source)
            with self.assertRaises(LoadStreamError):observations.read(source)

    def test_retries_and_latency_overflow_remain_actual_fault_observations(self):
        observed=window(0);observed.update(retries=7,transport_attempts=150007,
            settled_submission_attempts=150007,p99_us=2**64-1,latency_overflow=15000)
        observations=CampaignLoadStream(duration_s=86400)
        source=io.BytesIO(encoded(ready())+encoded(observed));observations.read(source)
        self.assertEqual(observations.read(source),observed)

    def test_trailing_or_duplicate_fields_and_truncated_lines_refuse(self):
        for line in (encoded(ready()).replace(b'"workers": 28',b'"workers": 28, "workers": 28'),
                     encoded(ready())[:-1],b' '+b'x'*4096+b'\n'):
            with self.assertRaises(LoadStreamError):CampaignLoadStream(duration_s=86400).read(io.BytesIO(line))
        observations=CampaignLoadStream(duration_s=21900)
        source=io.BytesIO(encoded(ready())+b''.join(encoded(window(index)) for index in range(73))
                          +encoded(stopped(73))+encoded(ready()))
        for _ in range(75):observations.read(source)
        with self.assertRaises(LoadStreamError):observations.read(source)

    def test_terminal_totals_must_equal_every_actual_window_and_post_duration_completion(self):
        observed=window(72);observed['completed_commands']-=1
        observed['transport_attempts']-=1;observed['settled_submission_attempts']-=1
        source=io.BytesIO(encoded(ready())+b''.join(encoded(window(index)) for index in range(72))
            +encoded(observed)+encoded(stopped(73)))
        observations=CampaignLoadStream(duration_s=21900)
        for _ in range(74):observations.read(source)
        with self.assertRaisesRegex(LoadStreamError,'^load_stream_terminal$'):observations.read(source)


if __name__=='__main__':unittest.main()
