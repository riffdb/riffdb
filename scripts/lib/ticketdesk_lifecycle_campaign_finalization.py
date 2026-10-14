# req: END-003, END-007, END-008, END-009, END-010
"""Reconcile the original completed load before releasing its finite barrier.

This controller component cannot qualify the campaign. Resource, source,
interference, four-language and CI evidence still require independent checks.
"""
import json
import os
from pathlib import Path
import re
import stat

from .ticketdesk_lifecycle_campaign_runtime import SECOND, private_write, require
from .ticketdesk_lifecycle_load_stream import unique_object, parse_integer, reject_number


def checked_terminal(value, *, generation, version, incarnation, duration_s):
    require(type(generation) is int and 1 <= generation <= 16
            and type(version) is int and version in (1, 2)
            and type(incarnation) is int and incarnation in (1, 2)
            and type(duration_s) is int and duration_s % 300 == 0
            and 73 <= duration_s//300 <= 288, "campaign_terminal_identity")
    expected = dict(schema="riffdb.ticketdesk-lifecycle-terminal/v1", generation=generation,
        contract_version=version, history_incarnation=incarnation,
        completed_commands=duration_s*100, completed_reads=duration_s*400)
    require(type(value) is dict and value.keys() == expected.keys()
            and all(type(value[key]) is type(item) and value[key] == item
                    for key, item in expected.items()), "campaign_terminal_identity")
    return dict(value)


def read_terminal(runtime):
    descriptor = os.open(runtime.root/"load.terminal", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        before = os.fstat(source.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                and before.st_uid == os.geteuid() and 0 < before.st_size <= 4096,
                "campaign_terminal_custody")
        encoded = source.read(4097)
        after = os.fstat(source.fileno())
    named = (runtime.root/"load.terminal").lstat()
    identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    require(len(encoded) == before.st_size and encoded.endswith(b"\n")
            and identity(before) == identity(after) == identity(named), "campaign_terminal_changed")
    value = json.loads(encoded, object_pairs_hook=unique_object, parse_int=parse_integer,
                       parse_float=reject_number, parse_constant=reject_number)
    return checked_terminal(value, generation=runtime.generation, version=runtime.version,
        incarnation=runtime.incarnation, duration_s=runtime.duration)


def checked_observer(value, *, completed, generation, incarnation, head):
    require(type(value) is dict
            and all(type(value.get(key)) is int for key in
                ("events_checked", "duplicates", "release_generation", "history_incarnation",
                 "live_head", "live_updates", "live_renewals"))
            and all(type(value.get(key)) is bool for key in ("observer_quiesced", "consumer_paused")),
            "campaign_terminal_observer")
    expected = 1024 + completed//65536*1024 + min(completed % 65536, 1024)
    require(value["events_checked"] <= expected and value["duplicates"] == 0
            and value["release_generation"] == generation
            and value["history_incarnation"] == incarnation
            and not value["observer_quiesced"] and not value["consumer_paused"],
            "campaign_terminal_observer")
    return (value["events_checked"] == expected and value["live_head"] >= head
            and value["live_updates"] > 0 and value["live_renewals"] > 0)


def checked_projection(value, identity, *, head, completed):
    require(type(value) is dict and value.get("schema") == "riffdb.cli.output/v1"
            and value.get("command") == "projection.query" and value.get("ok") is True
            and value.keys() == {"schema", "command", "ok", "result"},
            "campaign_terminal_projection")
    result = value["result"]
    require(type(result) is dict
            and {"status","observed_fence","frontier","rows"} <= result.keys()
            and result.keys() <= {"status","observed_fence","frontier","rows","next_cursor"},
            "campaign_terminal_projection")
    expected_count = completed//65536*1024 + min(completed % 65536, 1024)
    fence = result["observed_fence"]
    require(type(fence) is dict and fence.keys() == {"identity","generation","frontier"}
            and type(fence["identity"]) is dict
            and type(fence["identity"].get("projection_id")) is int
            and type(result["frontier"]) is dict
            and result["frontier"].keys() == {"type","sequence"}, "campaign_terminal_projection")
    require(result["status"] == "ready" and fence["identity"] == dict(
        contract_lineage="TicketDeskLifecycle", **identity)
        and type(fence["generation"]) is str
        and re.fullmatch(r"[1-9][0-9]{0,19}", fence["generation"]) is not None
        and 0 < int(fence["generation"]) < 2**64
        and result["frontier"]["type"] == "applied_through"
        and type(result["frontier"]["sequence"]) is str
        and re.fullmatch(r"[1-9][0-9]{0,19}",result["frontier"]["sequence"]) is not None
        and head <= int(result["frontier"]["sequence"]) < 2**64
        and fence["frontier"] == result["frontier"]
        and result["rows"] == [{"group":[{"type":"uuid",
            "value":"01900000-0000-7000-8001-000000000000"}],
            "values":{"fields":[{"field_id":1,"value":{"type":"i64","value":str(expected_count)}}]}}]
        and result.get("next_cursor") is None, "campaign_terminal_projection")
    require(type(result["rows"][0]["values"]["fields"][0]["field_id"]) is int,
            "campaign_terminal_projection")


class CampaignFinalization:
    complete_campaign_checks_proved = False

    def __init__(self, runtime, actions, timeline):
        self.runtime, self.actions, self.timeline = runtime, actions, timeline
        self.failed = self.complete = False

    @staticmethod
    def _close_pipes(child):
        for stream in (child.stdin, child.stdout):
            if stream is not None: stream.close()

    def finish(self):
        require(not self.failed and not self.complete, "campaign_finalization_closed")
        try:
            result = self._finish()
            self.complete = True
            return result
        except BaseException:
            self.failed = True
            raise

    def _finish(self):
        runtime = self.runtime
        deadline = (runtime.duration + 55)*SECOND
        require(runtime.elapsed() >= runtime.duration*SECOND
                and runtime.version == runtime.incarnation == 2
                and runtime.active_archive_outage is None, "campaign_finalization_phase")
        load = runtime.children["load"]
        self.actions.wait(lambda: runtime.outputs["load"].terminal is not None
            and (runtime.root/"load.terminal").is_file(), deadline)
        terminal = read_terminal(runtime)
        stopped = runtime.outputs["load"].terminal
        require(stopped == dict(schema="riffdb.ticketdesk-lifecycle-load-stopped/v1",
            completed_updates=terminal["completed_commands"], completed_reads=terminal["completed_reads"],
            post_duration_updates=0, post_duration_reads=0), "campaign_terminal_load")
        self.actions.replay(deadline)

        # This endpoint is explicitly outside the offered period. It cannot
        # create another measured native sample or change a window's latency.
        endpoint = runtime.native.sample(runtime.elapsed(), version=runtime.version)
        require(endpoint["history_incarnation"] == runtime.incarnation
                and endpoint["sampled_head"] >= runtime.epochs.native_last["sampled_head"],
                "campaign_terminal_native")
        def drained():
            nonlocal endpoint
            ready = (endpoint["outbox_backlog"] == 0 and endpoint["outbox_degraded"] is False
                     and endpoint["projection_backlog"] == 0)
            observed = runtime.observer()
            if ready and checked_observer(observed, completed=terminal["completed_commands"],
                    generation=runtime.generation, incarnation=runtime.incarnation,
                    head=endpoint["sampled_head"]): return True
            endpoint = runtime.native.sample(runtime.elapsed(), version=runtime.version)
            runtime.remaining(deadline)
            return False
        self.actions.wait(drained, deadline)
        private_write(runtime.root, "campaign-terminal-native.json", json.dumps(endpoint, sort_keys=True).encode()+b"\n")
        private_write(runtime.root, "campaign-terminal-observer.json",
            json.dumps(runtime.observer(), sort_keys=True).encode()+b"\n")
        query = {"leading_components":[{"type":"uuid","value":"01900000-0000-7000-8001-000000000000"}]}
        private_write(runtime.root, "campaign-final-projection-input.json", json.dumps(query, sort_keys=True).encode())
        projection = self.actions.call("projection.query", ["projection","query",str(runtime.identity["projection_id"]),
            "--contract-lineage","TicketDeskLifecycle","--contract-version",str(runtime.version),
            "--input",str(runtime.root/"campaign-final-projection-input.json"),
            "--after",str(endpoint["sampled_head"]),"--wait-nanos","30000000000","--limit","1"], deadline)
        checked_projection(projection, runtime.identity, head=endpoint["sampled_head"],
                           completed=terminal["completed_commands"])
        runtime.tick(native=False)
        runtime.memory.finish()
        memory = runtime.roster.sample(runtime.elapsed())
        private_write(runtime.root, "campaign-terminal-memory.json", json.dumps(memory,sort_keys=True).encode()+b"\n")
        disk = runtime.disk.poll(runtime.elapsed(), terminal=True)
        components = runtime.epochs.finish(runtime.elapsed())
        runtime.close_extent_incarnation()
        self.timeline.finish_campaign(runtime.elapsed())
        runtime.remaining(deadline)

        # Publication releases only the original load owner. No replacement or
        # post-period application work can supply the measured completion count.
        descriptor = os.open(runtime.root/"load.phase", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            require(stat.S_ISREG(os.fstat(source.fileno()).st_mode), "campaign_terminal_custody")
            phase = source.read(257)
        require(phase == f"{runtime.generation} 2 2 {runtime.credential}\n".encode(),
                "campaign_terminal_identity")
        private_write(runtime.root, "load.terminal-release", phase)
        require(load.wait(timeout=min(10,runtime.remaining(deadline))) == 0, "campaign_terminal_exit")
        runtime.outputs["load"].join(min(5,runtime.remaining(deadline)))
        require(runtime.outputs["load"].parser.complete, "campaign_terminal_load")
        runtime.roster.retire("load", load, expected_exit=0)
        self._close_pipes(load); del runtime.children["load"]

        state = runtime.operator.run_worker(str(runtime.artifacts/"lifecycle"), "reconcile",
            runtime.worker_environment(RIFFDB_LIFECYCLE_COMPLETED_COMMANDS=str(terminal["completed_commands"]),
                RIFFDB_LIFECYCLE_RECONCILIATION_WITNESS=str(runtime.root/"updates.witness")),
            elapsed_ns=runtime.elapsed(), timeout_s=60)
        require(state["updates"] == terminal["completed_commands"], "campaign_state_reconciliation")
        observer = runtime.children["observer"]
        private_write(runtime.root,"observer.stop",b"stop\n")
        require(observer.wait(timeout=15) == 0, "campaign_terminal_exit")
        runtime.outputs["observer"].join()
        runtime.roster.retire("observer",observer,expected_exit=0)
        self._close_pipes(observer); del runtime.children["observer"]
        sampler = runtime.native.process
        runtime.native.close()
        runtime.roster.retire("native_sampler",sampler,expected_exit=0)
        runtime.native=None; del runtime.children["native_sampler"]
        server = runtime.children["server"]
        server.terminate()
        require(server.wait(timeout=30) == 0,"campaign_terminal_exit")
        runtime.outputs["server"].join()
        runtime.roster.retire("server",server,expected_exit=0)
        self._close_pipes(server); del runtime.children["server"]
        require(not runtime.roster.children, "campaign_archive_not_offline")
        archive = runtime.operator.run_final_archive(str(runtime.artifacts/"lifecycle"),
            runtime.worker_environment(),elapsed_ns=runtime.elapsed(),duration_s=runtime.duration,timeout_s=900)
        require(archive["records_checked"] == 65536+terminal["completed_commands"], "campaign_archive_reconciliation")
        extents = runtime.reconcile_extent_incarnations()
        require(len(extents[-1]["observed_native_retired_generations"]) >= 3,
                "campaign_measured_retirement_gate")
        final_files, _, _ = runtime.disk._snapshot(None)
        require(all(final_files[key] <= runtime.disk.limits[key] for key in final_files),
                "campaign_disk_limit")
        private_write(runtime.root,"campaign-post-validation-disk.json",
            json.dumps(dict(resources=final_files,elapsed_ns=runtime.elapsed(),
                measured=False),sort_keys=True).encode()+b"\n")
        runtime.epochs.__exit__(None,None,None)
        runtime.memory.__exit__(None,None,None)
        runtime.disk.output.close(); runtime.disk.output=None
        os.close(runtime.disk.archive); runtime.disk.archive=None
        require(self.actions.restore_receipt is not None and self.actions.restore_clock.complete
                and self.actions.restore_read_admission is not None,
                'campaign_restore_clock_incomplete')
        return dict(schema="riffdb.ticketdesk-lifecycle-finalization/v2", terminal=terminal,
            native_endpoint=endpoint, components=components, terminal_memory=memory,
            terminal_disk=disk, state=state, archive=archive, measured_extents=extents,
            actions=self.timeline.actions, restore_clock=self.actions.restore_receipt,
            restore_read_admission=self.actions.restore_read_admission,
            qualifies_complete_campaign=False)
