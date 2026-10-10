# req: END-006, END-007, END-008, END-010
"""Published intervals include actual original-clock dispatch/release delay."""
import unittest
from lib.ticketdesk_lifecycle_campaign_timeline import CampaignTimeline, TimelineError
from test_campaign_timeline import plans, SECOND


class IntervalClockTests(unittest.TestCase):
    def test_recorded_fault_end_covers_completion_across_a_window_boundary(self):
        timeline = CampaignTimeline(plans(),duration_s=28800)
        timeline.start('sink_outage',300*SECOND+3*SECOND//4)
        timeline.release('sink_outage',600*SECOND+9*SECOND//10)
        ended = 900*SECOND+SECOND//10
        timeline.finish('sink_outage',ended,reconciled=True)
        action = timeline.actions[0]
        recorded_end = (action['start_s']+action['duration_s']+action['recovery_s'])*SECOND
        self.assertGreaterEqual(recorded_end,ended,'recorded fault interval omits actual completion')
        self.assertLess(recorded_end-ended,SECOND)
        self.assertEqual(action['recovery_s'],301)

    def rotation(self):
        timeline = CampaignTimeline(plans(),duration_s=28800)
        for plan in plans()[:2]:
            start=plan['start_s']*SECOND
            timeline.start(plan['kind'],start)
            timeline.release(plan['kind'],start+300*SECOND)
            timeline.finish(plan['kind'],start+301*SECOND,reconciled=True)
        timeline.start('capability_rotation',2700*SECOND+3*SECOND//4)
        timeline.release('capability_rotation',2701*SECOND+9*SECOND//10)
        return timeline

    def test_recorded_nonoutage_interval_includes_both_quantization_delays(self):
        timeline=self.rotation()
        timeline.finish('capability_rotation',2759*SECOND+SECOND//10,reconciled=True)
        action=timeline.actions[-1]
        self.assertEqual(action['duration_s']+action['recovery_s'],60)

    def test_quantization_cannot_extend_the_original_published_sixty_second_limit(self):
        timeline=self.rotation()
        with self.assertRaisesRegex(TimelineError,'timeline_recovery_overrun'):
            timeline.finish('capability_rotation',2760*SECOND+SECOND//10,reconciled=True)
        self.assertEqual(len(timeline.actions),2)
        with self.assertRaisesRegex(TimelineError,'timeline_failed'):
            timeline.finish('capability_rotation',2760*SECOND+SECOND//10,reconciled=True)
