# req: END-007, END-008, END-010
"""Pre-offer model fixtures and deterministic observer handover; no DB claim."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from lib import ticketdesk_lifecycle_campaign_runtime as runtime_module
from lib.ticketdesk_lifecycle_campaign_observer_samples import CampaignObserverSamples
from lib.ticketdesk_lifecycle_campaign_resource_model import admit,artifacts,DISK_LIMITS,PROCESS_LIMITS,ResourceModelError,GIB

SECOND=10**9


def message(elapsed):
    return dict(schema="riffdb.ticketdesk-lifecycle-observation/v1",elapsed_s=elapsed,
        events_checked=1024,duplicates=0,consumer_checkpoint=4,live_head=5,live_updates=1,
        history_incarnation=1,live_renewals=0,release_generation=1,auxiliary_slots=2,
        consumer_paused=False,observer_quiesced=False)


class CampaignResourceAdmissionTests(unittest.TestCase):
    def binaries(self,root):
        directory=root/"artifacts";directory.mkdir(mode=0o700)
        for name in ("riffdbd","riffdb","lifecycle","riffdb-driverd"):
            path=directory/name;path.write_bytes(b"synthetic binary fixture");path.chmod(0o700)
        return directory

    def test_observer_uses_one_original_message_when_reader_publishes_the_next(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            class UpdatingOwner:
                failure=None
                reads=0
                @property
                def latest(self):
                    self.reads+=1
                    return (11*SECOND,message(200)) if self.reads==1 else (12*SECOND,message(201))
            owner=UpdatingOwner()
            with CampaignObserverSamples(root,duration_s=21900,origin_ns=10*SECOND) as samples:
                runtime=SimpleNamespace(outputs={"observer":owner},observer_samples=samples,interference=None)
                with patch("lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns",return_value=11*SECOND+20):
                    try:value=runtime_module.CampaignRuntime.observer(runtime)
                    except ValueError:self.fail("observer snapshot mixed two reader publications")
                self.assertEqual(value,message(200));self.assertEqual(owner.reads,1)
                row=json.loads((root/"campaign-observer-observations.jsonl").read_text())
                self.assertEqual(row["observation"],message(200));self.assertEqual(row["observation_age_ns"],20)

    def test_runtime_uses_the_same_pre_offer_frozen_limits_as_the_independent_model(self):
        self.assertEqual(runtime_module.DISK_LIMITS,DISK_LIMITS)
        self.assertEqual(runtime_module.LIMITS,PROCESS_LIMITS)
        self.assertEqual(DISK_LIMITS["database_bytes"],64*GIB)
        self.assertEqual(sum(DISK_LIMITS.values()),126*GIB)

    def test_artifact_scope_includes_all_four_private_immutable_copies_in_whole_file_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=self.binaries(root)
            values=artifacts(root,path)
            self.assertEqual(len(values),4)
            self.assertTrue(all(item["bytes"]>0 and item["file_bound_bytes"]>=item["bytes"] for item in values.values()))
            with self.assertRaisesRegex(ResourceModelError,"campaign_model_artifact_scope"):
                artifacts(root,root/"outside")
            (path/"extra").write_bytes(b"extra")
            with self.assertRaises(ResourceModelError):artifacts(root,path)

    def test_missing_alias_symlink_or_nonprivate_executable_refuses(self):
        for fault in ("missing","alias","symlink","mode"):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);path=self.binaries(root);binary=path/"riffdbd"
                if fault=="missing":binary.unlink()
                if fault=="alias":os.link(binary,root/"alias")
                if fault=="symlink":binary.unlink();binary.symlink_to(root/"absent")
                if fault=="mode":binary.chmod(0o755)
                with self.assertRaises(ResourceModelError):artifacts(root,path)

    def test_admission_reserves_all_disk_categories_and_never_inflates_a_running_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=self.binaries(root)
            with patch("lib.ticketdesk_lifecycle_campaign_resource_model.shutil.disk_usage",return_value=SimpleNamespace(free=134*GIB)):
                value=admit(root,path,duration_s=28800)
            self.assertEqual(value["minimum_starting_free_bytes"],134*GIB)
            self.assertEqual(value["allocator_upper_bound_bytes"],27*GIB)
            self.assertEqual(value["population"]["commands"],2880000)
            self.assertFalse(value["qualifies_complete_resource_model"])
            with patch("lib.ticketdesk_lifecycle_campaign_resource_model.shutil.disk_usage",return_value=SimpleNamespace(free=134*GIB-1)):
                with self.assertRaisesRegex(ResourceModelError,"campaign_model_free_space"):admit(root,path,duration_s=28800)
            self.assertEqual(DISK_LIMITS["database_bytes"],64*GIB)

    def test_duration_must_match_the_original_finite_campaign(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=self.binaries(root)
            for value in (True,0,21899,86401):
                with self.subTest(value=value),self.assertRaises(ResourceModelError):admit(root,path,duration_s=value)


if __name__=="__main__":unittest.main()
