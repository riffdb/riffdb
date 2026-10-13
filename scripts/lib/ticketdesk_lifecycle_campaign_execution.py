# req: END-003, END-006, END-007, END-008, END-009, END-010
"""Run the actual seven faults and final barrier on one offered monotonic clock.

This executes an unqualified campaign component. Final source CI, frozen
artifacts, host and conformance observations, interference and independent
resource reconciliation remain caller prerequisites for a qualifying receipt.
"""
import json
import threading

from .ticketdesk_lifecycle_campaign_actions import CampaignActions
from .ticketdesk_lifecycle_campaign_finalization import CampaignFinalization
from .ticketdesk_lifecycle_campaign_runtime import CampaignRuntime, SECOND, private_write, require
from .ticketdesk_lifecycle_campaign_timeline import CampaignTimeline


def execute(runtime):
    require(isinstance(runtime, CampaignRuntime) and runtime.origin is None
            and not runtime.children, "campaign_execution_state")
    timeline = CampaignTimeline(runtime.faults,duration_s=runtime.duration)
    actions = CampaignActions(runtime,timeline)
    by_kind = {plan["kind"]:plan for plan in runtime.faults}
    try:
        runtime.prepare_population()
        while runtime.elapsed() < runtime.duration*SECOND:
            elapsed = runtime.elapsed()
            if actions.prepare_restore_if_due(elapsed):
                # Preparation consumes the original offered clock. Re-read it
                # before dispatch; a late backup cannot move the fixed fault.
                continue
            kind = timeline.due(elapsed)
            if kind is not None:
                actions.execute(by_kind[kind])
                continue
            runtime.tick()
            # This is an actual measured sampling cadence, never correctness
            # synchronization or an accelerated/replaced offered clock.
            remaining=(runtime.duration*SECOND-runtime.elapsed())/SECOND
            if remaining>0:threading.Event().wait(min(.02,remaining))
        result=CampaignFinalization(runtime,actions,timeline).finish()
        private_write(runtime.root,"campaign-execution.json",json.dumps(result,sort_keys=True).encode()+b"\n")
        return result
    finally:
        try:
            actions.close()
        finally:
            runtime.close()
