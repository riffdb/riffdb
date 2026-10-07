# req: END-006, END-007, END-008, END-009, END-010
"""The frozen campaign clock permits one bounded fault and six final quiet hours."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_campaign_timeline import CampaignTimeline, TimelineError

SECOND = 1000000000


def plans():
    return [
        {"kind": kind, "start_s": start, "duration_s": duration}
        for kind, start, duration in (
            ("sink_outage", 300, 300), ("consumer_outage", 1500, 300),
            ("capability_rotation", 2700, 1), ("additive_deploy", 3000, 1),
            ("graceful_restart", 3300, 1), ("process_kill", 3600, 1),
            ("backup_restore", 3900, 1))]


class Timeline(unittest.TestCase):
    def test_frozen_faults_are_dispatched_in_order_and_all_real_phases_are_required(self):
        source = plans()
        timeline = CampaignTimeline(source, duration_s=28800)
        source[0]["kind"] = "changed"
        for plan in plans():
            start = plan["start_s"] * SECOND
            self.assertIsNone(timeline.due(start - 1))
            self.assertEqual(timeline.due(start), plan["kind"])
            timeline.start(plan["kind"], start)
            release = start + plan["duration_s"] * SECOND
            timeline.release(plan["kind"], release)
            timeline.finish(plan["kind"], release + 2 * SECOND, reconciled=True)
        self.assertIsNone(timeline.due(7200 * SECOND))
        timeline.finish_campaign(28800 * SECOND)
        self.assertEqual(len(timeline.actions), 7)
        self.assertTrue(all(action["recovery_s"] == 2 for action in timeline.actions))
        timeline.actions[0]["kind"] = "changed"
        self.assertEqual(timeline.actions[0]["kind"], "sink_outage")

    def test_late_injection_and_recovery_never_admit_a_following_fault(self):
        timeline = CampaignTimeline(plans(), duration_s=28800)
        with self.assertRaisesRegex(TimelineError, "timeline_start_late"):
            timeline.start("sink_outage", 301 * SECOND)
        timeline = CampaignTimeline(plans(), duration_s=28800)
        timeline.start("sink_outage", 300 * SECOND)
        with self.assertRaisesRegex(TimelineError, "timeline_injection_overrun"):
            timeline.due(601 * SECOND)
        timeline = CampaignTimeline(plans(), duration_s=28800)
        timeline.start("sink_outage", 300 * SECOND)
        timeline.release("sink_outage", 600 * SECOND)
        with self.assertRaisesRegex(TimelineError, "timeline_recovery_overrun"):
            timeline.due(1200 * SECOND + 1)
        with self.assertRaisesRegex(TimelineError, "timeline_failed"):
            timeline.finish("sink_outage", 1200 * SECOND, reconciled=True)
        self.assertEqual(timeline.actions, [])

    def test_restart_deadline_includes_injection_and_ceilings_do_not_hide_elapsed_time(self):
        timeline = CampaignTimeline(plans(), duration_s=28800)
        for plan in plans()[:2]:
            start = plan["start_s"] * SECOND
            timeline.start(plan["kind"], start)
            timeline.release(plan["kind"], start + 300 * SECOND)
            timeline.finish(plan["kind"], start + 301 * SECOND, reconciled=True)
        timeline.start("capability_rotation", 2700 * SECOND)
        timeline.release("capability_rotation", 2701 * SECOND + SECOND // 2)
        with self.assertRaisesRegex(TimelineError, "timeline_recovery_overrun"):
            timeline.finish("capability_rotation", 2760 * SECOND + 1, reconciled=True)

    def test_missing_changed_unreconciled_overlapping_or_final_period_faults_refuse(self):
        for bad in (plans()[:-1], plans() + [plans()[0]],
                    [{**plan, "kind": "unknown"} for plan in plans()],
                    [{**plan, "start_s": 7200} if plan["kind"] == "backup_restore" else plan
                     for plan in plans()],
                    [{**plan, "start_s": 1200} if plan["kind"] == "consumer_outage" else plan
                     for plan in plans()]):
            with self.assertRaises(TimelineError):
                CampaignTimeline(bad, duration_s=28800)
        timeline = CampaignTimeline(plans(), duration_s=28800)
        with self.assertRaisesRegex(TimelineError, "timeline_incomplete"):
            timeline.finish_campaign(28800 * SECOND)
        timeline = CampaignTimeline(plans(), duration_s=28800)
        timeline.start("sink_outage", 300 * SECOND)
        timeline.release("sink_outage", 600 * SECOND)
        with self.assertRaisesRegex(TimelineError, "timeline_unreconciled"):
            timeline.finish("sink_outage", 601 * SECOND, reconciled=False)
        with self.assertRaisesRegex(TimelineError, "timeline_failed"):
            timeline.finish("sink_outage", 601 * SECOND, reconciled=True)

    def test_missing_release_or_clock_rewind_fences_the_controller_permanently(self):
        timeline = CampaignTimeline(plans(), duration_s=28800)
        timeline.start("sink_outage", 300 * SECOND)
        with self.assertRaisesRegex(TimelineError, "timeline_release_required"):
            timeline.finish("sink_outage", 600 * SECOND, reconciled=True)
        with self.assertRaisesRegex(TimelineError, "timeline_failed"):
            timeline.release("sink_outage", 600 * SECOND)
        timeline = CampaignTimeline(plans(), duration_s=28800)
        timeline.due(100 * SECOND)
        with self.assertRaisesRegex(TimelineError, "timeline_clock"):
            timeline.due(99 * SECOND)
        with self.assertRaisesRegex(TimelineError, "timeline_failed"):
            timeline.due(300 * SECOND)


if __name__ == "__main__":
    unittest.main()
