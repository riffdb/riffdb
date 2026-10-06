# req: END-007, END-008, END-010
"""Observe the native aggregate pool without confusing it with process memory."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_provider_budget import ProviderBudgetError, parse_sample


PREFIX = b"[riffdbd-diag] trigger=exact_provider_lifecycle "


def line(**changes):
    fields = dict(available="true", limit_bytes=1073741824, fixed_owner_bytes=512,
        owned_bytes=8192, reserved_bytes=4096, reserved_work=2, allocations=9,
        released_allocations=3, active_owners=4, retiring_owners=2,
        pinned_owners=1, pins=3, released_owners=5)
    fields.update(changes)
    return PREFIX + " ".join(f"{key}={value}" for key, value in fields.items()).encode() + b"\n"


class NativeProviderBudget(unittest.TestCase):
    def test_charge_includes_fixed_owner_owned_and_unpublished_reserved_work(self):
        sample = parse_sample(line())
        self.assertEqual(sample["charged_bytes"], 512 + 8192 + 4096)
        self.assertEqual(sample["limit_bytes"], 1073741824)
        self.assertEqual(sample["pinned_owners"], 1)
        self.assertNotIn("allocator_bytes", sample)
        self.assertNotIn("rss_bytes", sample)

    def test_aggregate_limit_accepts_equality_but_refuses_even_one_extra_byte(self):
        value = parse_sample(line(owned_bytes=1073741824-512-4096))
        self.assertEqual(value["charged_bytes"], value["limit_bytes"])
        with self.assertRaisesRegex(ProviderBudgetError, "^provider_budget_limit$"):
            parse_sample(line(owned_bytes=1073741824-512-4096+1))
        with self.assertRaises(ProviderBudgetError):
            parse_sample(line(owned_bytes=2**64-1))

    def test_unavailable_unknown_duplicate_partial_and_malformed_samples_refuse(self):
        examples = [PREFIX+b"available=false\n", line()+b"private input",
            line().replace(b"limit_bytes=1073741824", b"limit_bytes=2147483648"),
            line().replace(b"fixed_owner_bytes=512", b"fixed_owner_bytes=0"),
            line().replace(b"owned_bytes=8192", b"owned_bytes=8192 owned_bytes=1"),
            line().replace(b"reserved_work=2 ", b""),
            line().replace(b"pins=3", b"pins=03"),
            line().replace(b"pins=3", b"pins=-1"),
            line().replace(b"pins=3", b"pins="+str(2**64).encode()),
            line().replace(b"pins=3", b"pins=true"),
            PREFIX+b"available=true "+b"x"*2048+b"\n"]
        for encoded in examples:
            with self.subTest(encoded=encoded[:60]):
                with self.assertRaises(ProviderBudgetError) as observed:
                    parse_sample(encoded)
                self.assertNotIn("private", str(observed.exception))
                self.assertLess(len(str(observed.exception)), 64)

    def test_nonprovider_diagnostics_are_ignored_without_interpreting_their_payload(self):
        self.assertIsNone(parse_sample(b"[riffdbd-diag] trigger=unrelated private input\n"))

    def test_impossible_lifecycle_and_allocation_counters_refuse(self):
        for changes in (dict(released_allocations=10), dict(pinned_owners=7),
                        dict(pins=0), dict(reserved_work=0),
                        dict(owned_bytes=0), dict(allocations=3)):
            with self.subTest(changes=changes):
                with self.assertRaisesRegex(ProviderBudgetError, "^provider_budget_accounting$"):
                    parse_sample(line(**changes))


class ProviderSampleCoverage(unittest.TestCase):
    def test_samples_record_native_charges_and_refuse_missing_or_stale_coverage(self):
        import tempfile
        from lib.ticketdesk_lifecycle_provider_budget import ProviderSamples
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with ProviderSamples(root, 900) as samples:
                for elapsed in range(0, 900, 60):
                    samples.observe(line(), elapsed * 1000000000)
                summary = samples.finish(900000000000)
                self.assertEqual(summary["samples"], 15)
                self.assertEqual(summary["peak_charged_bytes"], 12800)
                self.assertIs(summary["qualifies_complete_resource_model"], False)
            self.assertEqual((root / "provider-budget-samples.jsonl").stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                with ProviderSamples(root, 900):
                    pass
        for timings in ((0, 76), (76,), (0, 0), (60, 59)):
            with self.subTest(timings=timings), tempfile.TemporaryDirectory() as directory:
                with ProviderSamples(Path(directory), 900) as samples:
                    with self.assertRaises(ProviderBudgetError):
                        for elapsed in timings:
                            samples.observe(line(), elapsed * 1000000000)
                        samples.finish(900000000000)

    def test_counter_rewind_requires_a_new_pool_and_does_not_look_like_retirement(self):
        import tempfile
        from lib.ticketdesk_lifecycle_provider_budget import ProviderSamples
        with tempfile.TemporaryDirectory() as directory:
            with ProviderSamples(Path(directory), 300) as samples:
                samples.observe(line(), 0)
                with self.assertRaisesRegex(ProviderBudgetError, "^provider_budget_counter_rewind$"):
                    samples.observe(line(allocations=8), 60000000000)
                samples.observe(line(owned_bytes=4096, released_allocations=6), 60000000000)
                self.assertEqual(samples.last["charged_bytes"], 8704)

    def test_absent_last_minute_and_incomplete_window_fail_with_preserved_samples(self):
        import tempfile
        from lib.ticketdesk_lifecycle_provider_budget import ProviderSamples
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with ProviderSamples(root, 300) as samples:
                for elapsed in (0, 60, 120, 180):
                    samples.observe(line(), elapsed * 1000000000)
                with self.assertRaisesRegex(ProviderBudgetError, "^provider_budget_sample_gap$"):
                    samples.finish(300000000000)
            self.assertEqual(len((root / "provider-budget-samples.jsonl").read_bytes().splitlines()), 4)


class ProviderLogCollection(unittest.TestCase):
    def test_collector_ignores_preload_samples_and_keeps_an_incomplete_line_until_complete(self):
        import tempfile
        from lib.ticketdesk_lifecycle_provider_budget import ProviderLog
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "server.stderr"
            path.write_bytes(line())
            with ProviderLog(root, 300) as collector:
                collector.poll(0)
                self.assertEqual(collector.samples.count, 0)
                with path.open("ab") as output:
                    output.write(line()[:-1])
                collector.poll(1000000000)
                self.assertEqual(collector.samples.count, 0)
                with path.open("ab") as output:
                    output.write(b"\n")
                collector.poll(2000000000)
                self.assertEqual(collector.samples.count, 1)
                self.assertEqual(collector.samples.last_ns, 2000000000)
            self.assertEqual(len((root / "provider-budget-samples.jsonl").read_bytes().splitlines()), 1)

    def test_alias_nonregular_oversized_and_excessive_records_refuse(self):
        import tempfile
        from lib.ticketdesk_lifecycle_provider_budget import ProviderLog
        for mutation in ("alias", "directory", "oversized", "many"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "server.stderr"
                if mutation == "alias":
                    (root / "alternate").write_bytes(b"")
                    path.symlink_to("alternate")
                elif mutation == "directory":
                    path.mkdir()
                else:
                    path.write_bytes(b"")
                with self.assertRaises(ProviderBudgetError):
                    with ProviderLog(root, 300) as collector:
                        with path.open("ab") as output:
                            output.write(b"x"*4097 if mutation == "oversized" else b"other\n"*65)
                        collector.poll(0)

    def test_truncation_and_path_replacement_are_not_an_empty_successful_sample(self):
        import tempfile
        from lib.ticketdesk_lifecycle_provider_budget import ProviderLog
        for mutation in ("truncate", "replace"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "server.stderr"
                path.write_bytes(line())
                with ProviderLog(root, 300) as collector:
                    if mutation == "truncate":
                        path.write_bytes(b"")
                    else:
                        path.rename(root / "previous")
                        path.write_bytes(line())
                    with self.assertRaises(ProviderBudgetError):
                        collector.poll(0)


if __name__ == "__main__":
    unittest.main()
