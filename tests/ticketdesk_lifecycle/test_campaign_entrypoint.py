# req: END-003, END-007, END-008, END-009, END-010
"""Controller fixtures do not establish a measured database rehearsal."""
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from lib.ticketdesk_lifecycle_campaign_entrypoint import (
    DURATION,schedule,source_artifacts,copy_artifacts,prepare,post_validation,CampaignEntrypointError,
)
from lib.ticketdesk_lifecycle_campaign_resource_model import ARTIFACT_NAMES,DISK_LIMITS,PROCESS_LIMITS
from lib.ticketdesk_lifecycle_campaign_disk import CampaignDiskSamples,DiskError
from lib.ticketdesk_lifecycle_campaign_timeline import CampaignTimeline


class CampaignEntrypointTests(unittest.TestCase):
    def source(self,parent):
        source=parent/'source';source.mkdir(mode=0o700)
        for name in ARTIFACT_NAMES:(source/name).write_bytes(name.encode())
        return source

    def test_fixed_eight_hour_schedule_preserves_all_seven_actions_and_six_quiet_hours(self):
        self.assertEqual(DURATION,28800)
        plans=schedule();CampaignTimeline(plans,duration_s=DURATION)
        self.assertEqual(len({row['kind'] for row in plans}),7)
        self.assertEqual([row['start_s'] for row in plans],[600,1800,3000,3600,4200,4800,5400])
        self.assertTrue(all(row['start_s']+row['duration_s']+600<=DURATION-21600 for row in plans))
        plans[0]['start_s']=0;self.assertEqual(schedule()[0]['start_s'],600)

    def test_entrypoint_selects_only_the_fixed_rehearsal_or_full_campaign_duration(self):
        from lib.ticketdesk_lifecycle_campaign_entrypoint import main
        for mode,seconds in ((None,28800),('rehearsal',28800),('campaign',86400)):
            with self.subTest(mode=mode):
                argv=['ticketdesk-lifecycle-campaign','--root','/tmp/fresh-campaign',
                      '--artifacts','/tmp/frozen-binaries']
                if mode is not None:argv+=['--mode',mode]
                with patch('sys.argv',argv), \
                     patch('lib.ticketdesk_lifecycle_campaign_entrypoint.run') as run, \
                     patch('builtins.print'):
                    self.assertEqual(main(),0)
                self.assertEqual(run.call_args.kwargs,{'duration_s':seconds})
                plans=schedule();CampaignTimeline(plans,duration_s=seconds)
                self.assertEqual(len(plans),7)
                self.assertTrue(all(row['start_s']+row['duration_s']+600<=seconds-21600
                                    for row in plans))

    def test_unknown_run_duration_is_refused_before_controls_or_resource_changes(self):
        from lib.ticketdesk_lifecycle_campaign_entrypoint import run
        for seconds in (0,300,28799,28800.0,True,86401):
            with self.subTest(seconds=seconds), \
                 patch('lib.ticketdesk_lifecycle_campaign_entrypoint.prepare') as prepare, \
                 patch('lib.ticketdesk_lifecycle_campaign_entrypoint.resource.setrlimit') as limit:
                with self.assertRaisesRegex(CampaignEntrypointError,'campaign_entry_duration'):
                    run(Path('/tmp/fresh-campaign'),Path('/tmp/frozen-binaries'),Path('/tmp/source'),
                        duration_s=seconds)
                prepare.assert_not_called();limit.assert_not_called()

    def test_frozen_copies_bind_exact_preparation_binaries_and_private_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            parent=Path(directory);source=self.source(parent);root=parent/'root';root.mkdir(mode=0o700)
            frozen=source_artifacts(source);copied=copy_artifacts(root,source,frozen)
            self.assertEqual(copied.keys(),ARTIFACT_NAMES)
            for name in ARTIFACT_NAMES:
                self.assertEqual((root/'artifacts'/name).read_bytes(),name.encode())
                self.assertEqual((root/'artifacts'/name).stat().st_mode&0o777,0o700)
                self.assertEqual(copied[name]['sha256'],frozen[name]['sha256'])
            with self.assertRaises(FileExistsError):copy_artifacts(root,source,frozen)

    def test_a_changed_binary_or_link_cannot_supply_prepared_controls(self):
        for fault in ('changed','symlink','hardlink','empty'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
                parent=Path(directory);source=self.source(parent);root=parent/'root';root.mkdir(mode=0o700)
                expected=source_artifacts(source);path=source/'riffdbd'
                if fault=='changed':path.write_bytes(b'changed source')
                if fault=='symlink':path.unlink();path.symlink_to(source/'riffdb')
                if fault=='hardlink':os.link(path,parent/'other-owner')
                if fault=='empty':path.write_bytes(b'')
                with self.assertRaises((ValueError,OSError)):copy_artifacts(root,source,expected)

    def test_existing_root_is_refused_before_any_control_process(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch('lib.ticketdesk_lifecycle_campaign_entrypoint.subprocess.Popen') as launch:
                with self.assertRaisesRegex(CampaignEntrypointError,'campaign_entry_fresh_root'):
                    prepare(root,root,root)
                launch.assert_not_called()

    def test_post_validation_reopens_only_the_original_archive_and_keeps_peaks_unmeasured(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'archive').mkdir(mode=0o700)
            archive=root/'archive/archive.redb';archive.write_bytes(b'actual retained archive')
            with CampaignDiskSamples(root,DURATION,DISK_LIMITS) as disk:pass
            runtime=SimpleNamespace(root=root,disk=disk,children={},roster=SimpleNamespace(children={}),
                elapsed=lambda:(DURATION+80)*10**9)
            closed={'resources':{'windows':[{'allocator_bytes':27*1024**3}]}}
            memory=SimpleNamespace(tree_sample=lambda:({'rss_peak_bytes':4096},set()))
            with patch('lib.ticketdesk_lifecycle_campaign_entrypoint.ProcessMemory',return_value=memory), \
                 patch('lib.ticketdesk_lifecycle_campaign_entrypoint.resource.getrlimit',
                       return_value=(PROCESS_LIMITS['controller'],PROCESS_LIMITS['controller'])):
                result=post_validation(runtime,closed)
                self.assertFalse(result['measured']);self.assertFalse(result['qualifies_complete_campaign'])
                self.assertGreaterEqual(result['files']['control_and_evidence_bytes'],4096)
                original=root/'retained-archive';archive.rename(original);archive.write_bytes(b'replacement')
                with self.assertRaisesRegex(DiskError,'campaign_disk_archive_identity'):
                    post_validation(runtime,closed)

    def test_post_period_controller_growth_or_remaining_children_refuse(self):
        runtime=SimpleNamespace(children={},roster=SimpleNamespace(children={}))
        for memory in (({'rss_peak_bytes':PROCESS_LIMITS['controller']+1},set()),
                       ({'rss_peak_bytes':4096},{123})):
            with self.subTest(memory=memory), \
                 patch('lib.ticketdesk_lifecycle_campaign_entrypoint.ProcessMemory',
                       return_value=SimpleNamespace(tree_sample=lambda:memory)):
                with self.assertRaisesRegex(CampaignEntrypointError,'campaign_entry_controller_bound'):
                    post_validation(runtime,{})


if __name__=='__main__':unittest.main()
