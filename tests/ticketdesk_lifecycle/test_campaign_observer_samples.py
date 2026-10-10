# req: END-003, END-007, END-008, END-010
"""Original-clock collector fixtures are not actual database progress evidence."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from lib.ticketdesk_lifecycle_campaign_runtime import CampaignRuntime
from lib.ticketdesk_lifecycle_campaign_observer_samples import CampaignObserverSamples, ObserverSamplesError

SECOND=10**9


def observation(**changes):
    return dict(dict(schema="riffdb.ticketdesk-lifecycle-observation/v1",elapsed_s=200,
        events_checked=1024,duplicates=0,consumer_checkpoint=4,live_head=5,live_updates=1,
        history_incarnation=1,live_renewals=0,release_generation=1,auxiliary_slots=2,
        consumer_paused=False,observer_quiesced=False),**changes)


class CampaignObserverSamplesTests(unittest.TestCase):
    def test_runtime_observer_retains_actual_message_on_the_original_campaign_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with CampaignObserverSamples(root,duration_s=21900,origin_ns=10*SECOND) as samples:
                runtime=SimpleNamespace(outputs={"observer":SimpleNamespace(failure=None,
                    latest=(10*SECOND+100,observation()))},observer_samples=samples)
                with patch("lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns",return_value=10*SECOND+120):
                    self.assertEqual(CampaignRuntime.observer(runtime),observation())
                rows=(root/"campaign-observer-observations.jsonl").read_text().splitlines()
                self.assertEqual(len(rows),1)
                self.assertEqual(json.loads(rows[0]),dict(schema="riffdb.ticketdesk-lifecycle-campaign-observer/v1",
                    elapsed_ns=120,observation_age_ns=20,observation=observation()))

    def test_repeated_census_never_fabricates_another_message_or_post_period_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with CampaignObserverSamples(root,duration_s=21900,origin_ns=10*SECOND) as samples:
                samples.observe(10*SECOND+20,10*SECOND+10,observation())
                samples.observe(10*SECOND+30,10*SECOND+10,observation())
                samples.observe((21900+10)*SECOND,(21900+10)*SECOND,observation(elapsed_s=22100))
                self.assertEqual(samples.count,1)
            rows=(root/"campaign-observer-observations.jsonl").read_text().splitlines()
            self.assertEqual(len(rows),1)
            self.assertEqual(os.stat(root/"campaign-observer-observations.jsonl").st_mode&0o777,0o600)

    def test_stale_replaced_future_or_rewound_original_messages_refuse_stickily(self):
        for fault in ("stale","future","changed","receive_rewind","counter_rewind","clock_rewind"):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                with CampaignObserverSamples(root,duration_s=21900,origin_ns=10*SECOND) as samples:
                    samples.observe(11*SECOND,11*SECOND,observation())
                    now,received,value=12*SECOND,12*SECOND,observation(elapsed_s=201)
                    if fault=="stale":now=18*SECOND
                    if fault=="future":received=now+1
                    if fault=="changed":received=11*SECOND
                    if fault=="receive_rewind":received=11*SECOND-1
                    if fault=="counter_rewind":value["events_checked"]=1023
                    if fault=="clock_rewind":now=11*SECOND-1;received=now
                    with self.assertRaises(ObserverSamplesError):samples.observe(now,received,value)
                    with self.assertRaisesRegex(ObserverSamplesError,"campaign_observer_sample_state"):
                        samples.observe(13*SECOND,13*SECOND,observation(elapsed_s=202))

    def test_malformed_observations_and_finite_collection_bound_refuse(self):
        for changes in ({"elapsed_s":True},{"consumer_paused":1},{"extra":0},{"duplicates":1}):
            with self.subTest(changes=changes),tempfile.TemporaryDirectory() as directory:
                with CampaignObserverSamples(Path(directory),duration_s=21900,origin_ns=1) as samples:
                    with self.assertRaises(ObserverSamplesError):samples.observe(2,2,observation(**changes))
        with tempfile.TemporaryDirectory() as directory:
            with CampaignObserverSamples(Path(directory),duration_s=21900,origin_ns=1) as samples:
                samples.count=86475
                with self.assertRaisesRegex(ObserverSamplesError,"campaign_observer_sample_bound"):
                    samples.observe(2,2,observation())

    def test_output_cannot_replace_an_existing_or_symlinked_observation_file(self):
        for symlink in (False,True):
            with self.subTest(symlink=symlink),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);path=root/"campaign-observer-observations.jsonl"
                if symlink:path.symlink_to(root/"missing")
                else:path.write_bytes(b"original")
                with self.assertRaises(OSError):
                    with CampaignObserverSamples(root,duration_s=21900,origin_ns=1):pass


if __name__=="__main__":unittest.main()
