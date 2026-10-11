# req: END-003, END-007, END-008, END-010
"""Independently check measured observer and native progress on one clock.

The organization-zero consumer legitimately has windows without matching events.
Its exact terminal event count, monotonic acknowledgements and actual original
live-query renewals are checked separately from database-wide native progress.
This component supplies no source, host, interference or campaign qualification.
"""
from .ticketdesk_lifecycle_campaign_observer_samples import checked_observation
from .ticketdesk_lifecycle_campaign_owner_reconciliation import read_private, decode
from .ticketdesk_lifecycle_native_observer import NativeSamples, FIELDS as NATIVE_FIELDS

SECOND=10**9
GAP=75*SECOND
OBSERVER_FIELDS=frozenset({"schema","elapsed_ns","observation_age_ns","observation"})


class ProgressReconciliationError(ValueError):
    """Closed refusal without observations, private paths or identities."""


def require(condition, code):
    if not condition:raise ProgressReconciliationError(code)


def number(value):
    require(type(value) is int and 0 <= value < 2**64,"campaign_progress_number")
    return value


def read_rows(root,name,*,maximum,rows):
    data,identity=read_private(root,name,maximum=maximum)
    lines=data.splitlines(keepends=True)
    require(0 < len(lines) <= rows and all(len(line)<=4096 and line.endswith(b"\n") for line in lines),
            "campaign_progress_bound")
    return [decode(line) for line in lines],identity


def reconcile(observer_rows,native_rows,*,duration_s,identity,terminal_observer,terminal_native):
    require(type(duration_s) is int and duration_s%300 == 0 and 73 <= duration_s//300 <= 288,
            "campaign_progress_duration")
    require(type(observer_rows) is list and 0 < len(observer_rows)<=86475
            and type(native_rows) is list and 0 < len(native_rows)<=4096,"campaign_progress_bound")
    windows=[dict(observer_samples=0,native_samples=0,events_checked=0,consumer_checkpoint=0,
        live_head=0,live_updates=0,live_renewals=0,sampled_head=0,applied_through=0,
        outbox_backlog=0,projection_backlog=0) for _ in range(duration_s//300)]
    previous=last_ns=None
    for row in observer_rows:
        require(type(row) is dict and row.keys()==OBSERVER_FIELDS
                and row["schema"]=="riffdb.ticketdesk-lifecycle-campaign-observer/v1",
                "campaign_progress_observer_shape")
        elapsed=number(row["elapsed_ns"]);number(row["observation_age_ns"])
        require(elapsed<duration_s*SECOND and row["observation_age_ns"]<=5*SECOND
                and elapsed-(last_ns or 0)<=GAP and (last_ns is None or elapsed>last_ns),
                "campaign_progress_gap")
        try:value=checked_observation(row["observation"],previous)
        except ValueError as error:raise ProgressReconciliationError("campaign_progress_observer") from error
        window=windows[elapsed//(300*SECOND)];window["observer_samples"]+=1
        for key in ("events_checked","consumer_checkpoint","live_head","live_updates","live_renewals"):
            window[key]=value[key]
        previous,last_ns=value,elapsed
    require(duration_s*SECOND-last_ns<=GAP,"campaign_progress_gap")
    try:terminal=checked_observation(terminal_observer)
    except ValueError as error:raise ProgressReconciliationError("campaign_progress_terminal") from error
    commands=duration_s*100
    expected=1024+commands//65536*1024+min(commands%65536,1024)
    require(terminal["events_checked"]==expected and terminal["history_incarnation"]==2
            and terminal["release_generation"]==6 and not terminal["consumer_paused"]
            and not terminal["observer_quiesced"] and terminal["live_updates"]>0 and terminal["live_renewals"]>0
            and all(terminal[key]>=previous[key] for key in previous
                    if type(previous[key]) is int),"campaign_progress_terminal")
    validator=NativeSamples(identity);prior_native=None;last_ns=None;incarnation=version=1
    for row in native_rows:
        require(type(row) is dict and row.keys()==NATIVE_FIELDS|{"elapsed_ns"},"campaign_progress_native_shape")
        elapsed=number(row["elapsed_ns"])
        require(elapsed<duration_s*SECOND and elapsed-(last_ns or 0)<=GAP
                and (last_ns is None or elapsed>last_ns),"campaign_progress_gap")
        raw={key:value for key,value in row.items() if key!="elapsed_ns"}
        require(type(raw["contract_version"]) is int and version<=raw["contract_version"]<=2
                and type(raw["history_incarnation"]) is int and incarnation<=raw["history_incarnation"]<=2,
                "campaign_progress_release")
        if prior_native is not None and raw["index"]==0:
            validator=NativeSamples(identity)
        try:checked=validator.observe(raw,version=raw["contract_version"])
        except ValueError as error:raise ProgressReconciliationError("campaign_progress_native") from error
        if prior_native is not None:
            floors=["sampled_head","statistics_head"]
            if checked["history_incarnation"]==prior_native["history_incarnation"]:
                floors.extend(("applied_through","projection_generation"))
            require(all(checked[key]>=prior_native[key] for key in floors),
                "campaign_progress_native_rewind")
        incarnation,version=checked["history_incarnation"],checked["contract_version"]
        window=windows[elapsed//(300*SECOND)];window["native_samples"]+=1
        for key in ("sampled_head","applied_through","outbox_backlog","projection_backlog"):
            window[key]=checked[key]
        prior_native,last_ns=checked,elapsed
    require(duration_s*SECOND-last_ns<=GAP and incarnation==version==2,"campaign_progress_gap")
    require(type(terminal_native) is dict and terminal_native.keys()==NATIVE_FIELDS|{"elapsed_ns"},
            "campaign_progress_terminal")
    endpoint_ns=number(terminal_native["elapsed_ns"])
    require(duration_s*SECOND<=endpoint_ns<=duration_s*SECOND+GAP,"campaign_progress_terminal")
    raw={key:value for key,value in terminal_native.items() if key!="elapsed_ns"}
    try:endpoint=validator.observe(raw,version=2)
    except ValueError as error:raise ProgressReconciliationError("campaign_progress_terminal") from error
    require(endpoint["history_incarnation"]==2 and endpoint["outbox_backlog"]==0
            and endpoint["outbox_degraded"] is False and endpoint["projection_backlog"]==0
            and terminal["live_head"]>=endpoint["sampled_head"],"campaign_progress_terminal")
    require(all(w["observer_samples"]>0 and w["native_samples"]>0 for w in windows),
            "campaign_progress_coverage")
    require(all(b["sampled_head"]>a["sampled_head"] and b["applied_through"]>a["applied_through"]
                and b["live_updates"]>a["live_updates"] and b["live_renewals"]>a["live_renewals"]
                for a,b in zip(windows,windows[1:])),"campaign_progress_stalled")
    return dict(schema="riffdb.ticketdesk-lifecycle-progress-reconciliation/v1",duration_s=duration_s,
        windows=windows,terminal_observer=terminal,terminal_native=terminal_native,
        exact_consumer_events=expected,qualifies_complete_campaign=False)
