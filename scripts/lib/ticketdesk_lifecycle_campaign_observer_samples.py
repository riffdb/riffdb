# req: END-003, END-007, END-008, END-010
"""Retain original observer messages on the campaign's one monotonic clock.

These are actual observations, not campaign qualification. Independent checks
must bind them to the original owner, actions, native observations and terminal
state. A stale, replaced or discontinuous observer cannot supply progress.
"""
import json
import os
from pathlib import Path

SECOND = 10**9
FIELDS = frozenset({"schema", "elapsed_s", "events_checked", "duplicates", "consumer_checkpoint",
    "live_head", "live_updates", "history_incarnation", "live_renewals", "release_generation",
    "auxiliary_slots", "consumer_paused", "observer_quiesced"})
FLAGS = frozenset({"consumer_paused", "observer_quiesced"})
COUNTERS = FIELDS - FLAGS - {"schema"}


class ObserverSamplesError(ValueError):
    """Closed diagnostics without worker output or private identities."""


def require(condition, code):
    if not condition: raise ObserverSamplesError(code)


def checked_observation(value, previous=None):
    require(type(value) is dict and value.keys() == FIELDS
            and value["schema"] == "riffdb.ticketdesk-lifecycle-observation/v1"
            and all(type(value[key]) is int and 0 <= value[key] < 2**64 for key in COUNTERS)
            and all(type(value[key]) is bool for key in FLAGS)
            and value["duplicates"] == 0 and value["auxiliary_slots"] == 2
            and value["history_incarnation"] in (1,2)
            and 1 <= value["release_generation"] <= 16
            and value["events_checked"] <= 8640000+65536, "campaign_observer_sample_shape")
    if previous is not None:
        require(value["elapsed_s"] > previous["elapsed_s"]
                and all(value[key] >= previous[key] for key in COUNTERS-{"elapsed_s"}),
                "campaign_observer_sample_rewind")
    return dict(value)


class CampaignObserverSamples:
    def __init__(self, root, *, duration_s, origin_ns):
        require(type(duration_s) is int and duration_s%300 == 0
                and 73 <= duration_s//300 <= 288
                and type(origin_ns) is int and origin_ns > 0, "campaign_observer_sample_clock")
        self.root,self.duration,self.origin=Path(root),duration_s*SECOND,origin_ns
        self.output=self.previous=self.received=self.last_ns=None
        self.count=0
        self.failed=False

    def __enter__(self):
        require(self.output is None and not self.failed, "campaign_observer_sample_state")
        descriptor=os.open(self.root/"campaign-observer-observations.jsonl",
            os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        self.output=os.fdopen(descriptor,"wb")
        return self

    def __exit__(self,_kind,_error,_traceback):
        if self.output is not None:self.output.close()
        return False

    def observe(self, now_ns, received_ns, value):
        require(not self.failed and self.output is not None and not self.output.closed,
                "campaign_observer_sample_state")
        try:
            require(type(now_ns) is int and type(received_ns) is int
                    and self.origin <= now_ns and 0 < received_ns <= now_ns
                    and now_ns-received_ns <= 5*SECOND, "campaign_observer_sample_stale")
            elapsed=now_ns-self.origin
            require(self.last_ns is None or elapsed >= self.last_ns, "campaign_observer_sample_clock")
            self.last_ns=elapsed
            if self.received == received_ns:
                require(value == self.previous, "campaign_observer_sample_changed")
                return
            require(self.received is None or received_ns > self.received,
                    "campaign_observer_sample_rewind")
            checked=checked_observation(value,self.previous)
            if elapsed >= self.duration:return
            require(self.count < 86475, "campaign_observer_sample_bound")
            record=dict(schema="riffdb.ticketdesk-lifecycle-campaign-observer/v1",
                elapsed_ns=elapsed,observation_age_ns=now_ns-received_ns,observation=checked)
            encoded=json.dumps(record,sort_keys=True).encode()+b"\n"
            require(len(encoded)<=4096, "campaign_observer_sample_bound")
            self.output.write(encoded);self.output.flush()
            self.previous,self.received=checked,received_ns
            self.count+=1
        except BaseException:
            self.failed=True
            raise
