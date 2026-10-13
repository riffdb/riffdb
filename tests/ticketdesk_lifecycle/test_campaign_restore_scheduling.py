# req: END-001, END-008, REC-001
"""Actual controller dispatch order; no native timing or campaign qualification."""
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib import ticketdesk_lifecycle_campaign_execution as execution
from lib.ticketdesk_lifecycle_campaign_runtime import CampaignRuntime, SECOND
from lib.ticketdesk_lifecycle_campaign_timeline import CampaignTimeline


class EndDispatch(Exception):
    pass


class RestoreSchedulingTests(unittest.TestCase):
    def test_durable_backup_precedes_the_unchanged_scheduled_primary_loss(self):
        faults = [dict(kind=kind, start_s=start, duration_s=duration) for kind, start, duration in (
            ('consumer_outage', 600, 300), ('sink_outage', 1800, 300),
            ('capability_rotation', 3000, 45), ('additive_deploy', 3600, 45),
            ('graceful_restart', 4200, 45), ('process_kill', 4800, 45),
            ('backup_restore', 5400, 45))]
        timeline = CampaignTimeline(faults, duration_s=28800)
        for fault in faults[:-1]:
            started = fault['start_s'] * SECOND
            timeline.start(fault['kind'], started)
            released = started + fault['duration_s'] * SECOND
            timeline.release(fault['kind'], released)
            timeline.finish(fault['kind'], released + SECOND, reconciled=True)
        clock = SimpleNamespace(now=5340 * SECOND, prepared=False)
        events = []
        runtime = object.__new__(CampaignRuntime)
        runtime.origin, runtime.children = None, {}
        runtime.duration, runtime.faults = 28800, faults
        runtime.elapsed = lambda: clock.now
        runtime.prepare_population = lambda: setattr(runtime, 'origin', 0)
        runtime.tick = lambda: setattr(clock, 'now', 5400 * SECOND)
        runtime.close = Mock()
        test = self

        class Actions:
            def __init__(self, actual_runtime, actual_timeline):
                test.assertIs(actual_runtime, runtime)
                test.assertIs(actual_timeline, timeline)

            def prepare_restore_if_due(self, elapsed):
                if clock.prepared:
                    return False
                test.assertEqual(elapsed, 5340 * SECOND)
                events.append(('durable_backup', elapsed))
                clock.prepared, clock.now = True, 5350 * SECOND
                return True

            def execute(self, fault):
                test.assertTrue(clock.prepared,
                    'durable backup must precede scheduled primary loss')
                test.assertEqual(fault, faults[-1])
                timeline.start(fault['kind'], clock.now)
                events.append(('primary_loss', clock.now))
                raise EndDispatch()

            def close(self):
                pass

        with patch.object(execution, 'CampaignTimeline', return_value=timeline), \
                patch.object(execution, 'CampaignActions', Actions), \
                patch.object(execution.threading, 'Event'), \
                self.assertRaises(EndDispatch):
            execution.execute(runtime)
        self.assertEqual(events, [('durable_backup', 5340 * SECOND),
                                  ('primary_loss', 5400 * SECOND)])
        runtime.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
