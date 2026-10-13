# req: END-001, END-007, END-008
"""Bound actual pre-fault files; never interpret native durable formats.

The public operation supplies backup durability and the public health response
supplies the included application frontier. These pinned read-only descriptors
bind the selected bytes and complete acknowledged prefix to fault injection.
Native restore independently verifies every durable format and content check.
"""
import hashlib
import os
from pathlib import Path
import stat

from .ticketdesk_lifecycle_campaign_restore_clock import (
    MAX_ARTIFACTS, MAX_BACKUP_BYTES, artifact_binding,
)


class RestoreInputError(ValueError):
    """Closed refusal without paths or witness contents."""


def require(condition, code):
    if not condition:
        raise RestoreInputError(code)


def identity(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


class PinnedInputs:
    def __init__(self, parent, name):
        self.parent = self.directory = None
        self.files = {}
        self.failed = False
        try:
            self.parent = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            self.parent_identity = identity(os.fstat(self.parent))[:2]
            self.parent_path, self.name = Path(parent), name
            self.directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                     dir_fd=self.parent)
            info = os.fstat(self.directory)
            require(info.st_uid == os.geteuid(), 'restore_input_owner')
            self.directory_identity = identity(info)[:2]
        except BaseException:
            self.close()
            raise

    def open_file(self, name, maximum):
        require(not self.failed and name not in self.files, 'restore_input_state')
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=self.directory)
        try:
            info = os.fstat(descriptor)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                    and info.st_nlink == 1 and 0 < info.st_size <= maximum,
                    'restore_input_file')
            self.files[name] = descriptor, identity(info)
            return descriptor, info.st_size
        except BaseException:
            os.close(descriptor)
            raise

    def verify(self):
        try:
            require(not self.failed and self.directory is not None, 'restore_input_state')
            require(identity(self.parent_path.lstat())[:2] == self.parent_identity
                    and identity(os.fstat(self.parent))[:2] == self.parent_identity
                    and identity(os.stat(self.name, dir_fd=self.parent,
                                         follow_symlinks=False))[:2] == self.directory_identity
                    and identity(os.fstat(self.directory))[:2] == self.directory_identity,
                    'restore_input_directory_changed')
            for name, (descriptor, expected) in self.files.items():
                actual = os.fstat(descriptor)
                named = os.stat(name, dir_fd=self.directory, follow_symlinks=False)
                require(actual.st_nlink == named.st_nlink == 1
                        and stat.S_ISREG(named.st_mode)
                        and identity(actual) == identity(named) == expected,
                        'restore_input_changed')
        except BaseException:
            self.failed = True
            raise

    def close(self):
        self.failed = True
        for descriptor, _ in self.files.values():
            os.close(descriptor)
        self.files.clear()
        for name in ('directory', 'parent'):
            descriptor = getattr(self, name, None)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, name, None)


def hash_prefix(descriptor, size, checkpoint):
    digest = hashlib.sha256()
    for offset in range(0, size, 65536):
        checkpoint()
        chunk = os.pread(descriptor, min(65536, size - offset), offset)
        require(len(chunk) == min(65536, size - offset), 'restore_input_short_read')
        digest.update(chunk)
    return digest.hexdigest()


class AcknowledgedPrefix(PinnedInputs):
    method = 'acknowledged_prefix_v1'

    def __init__(self, root, *, commands, duration_s, frontier, version, incarnation, checkpoint):
        root = Path(root)
        super().__init__(root.parent, root.name)
        try:
            require(all(type(n) is int for n in
                        (commands, duration_s, frontier, version, incarnation))
                    and 0 < commands <= duration_s * 100 <= 86400 * 100
                    and 0 < frontier < 2**64 and version in (1, 2) and incarnation in (1, 2),
                    'restore_input_prefix_bound')
            descriptor, size = self.open_file('updates.witness', 16 + 32 * 86400 * 100)
            require(size == 16 + 32 * duration_s * 100, 'restore_input_witness_size')
            os.fsync(descriptor)
            header = os.pread(descriptor, 16, 0)
            require(header == b'RDBTW001' + (duration_s * 100).to_bytes(8, 'big'),
                    'restore_input_witness_header')
            digest = hashlib.sha256(header)
            maximum_sequence = 0
            for ordinal in range(0, commands, 2048):
                checkpoint()
                count = min(2048, commands - ordinal)
                chunk = os.pread(descriptor, count * 32, 16 + ordinal * 32)
                require(len(chunk) == count * 32, 'restore_input_short_read')
                digest.update(chunk)
                for index in range(count):
                    row = chunk[index * 32:(index + 1) * 32]
                    observed, sequence, contract, history = (
                        int.from_bytes(row[pos:pos + 8], 'big') for pos in range(0, 32, 8))
                    require(observed == ordinal + index and 0 < sequence <= frontier
                            and 1 <= contract <= version and 1 <= history <= incarnation,
                            'restore_input_witness_record')
                    # Parallel acknowledgements need not sort by commit sequence.
                    maximum_sequence = max(maximum_sequence, sequence)
            self.commands, self.prefix_bytes = commands, 16 + commands * 32
            self.sha256, self.maximum_sequence = digest.hexdigest(), maximum_sequence
            self.verify()
        except BaseException:
            self.close()
            raise

    def verify_prefix(self, commands, checkpoint):
        try:
            self.verify()
            require(type(commands) is int and commands == self.commands,
                    'restore_input_acknowledgement_gap')
            descriptor, _ = self.files['updates.witness']
            require(hash_prefix(descriptor, self.prefix_bytes, checkpoint) == self.sha256,
                    'restore_input_prefix_changed')
            self.verify()
            return self.sha256
        except BaseException:
            self.failed = True
            raise


class SelectedBackup(PinnedInputs):
    def __init__(self, root, name, *, checkpoint):
        super().__init__(Path(root) / 'backups', name)
        try:
            self.artifacts = {}
            total = 0
            names = self.inventory()
            artifact_binding({n: dict(bytes=1, sha256='0' * 64) for n in names}, '0' * 64)
            for artifact in names:
                maximum = 32 * 1024**2 if artifact == 'manifest.riffdb' else MAX_BACKUP_BYTES
                descriptor, size = self.open_file(artifact, maximum)
                total += size
                require(total <= MAX_BACKUP_BYTES, 'restore_input_backup_bound')
                self.artifacts[artifact] = dict(bytes=size,
                    sha256=hash_prefix(descriptor, size, checkpoint))
            manifest = self.artifacts.get('manifest.riffdb')
            require(manifest is not None, 'restore_input_manifest')
            self.manifest_sha256 = manifest['sha256']
            artifact_binding(self.artifacts, self.manifest_sha256)
            self.verify_selection()
        except BaseException:
            self.close()
            raise

    def inventory(self):
        names = set()
        with os.scandir(self.directory) as entries:
            for entry in entries:
                require(len(names) < MAX_ARTIFACTS, 'restore_input_inventory_bound')
                names.add(entry.name)
        return sorted(names)

    def verify_selection(self):
        try:
            self.verify()
            require(set(self.inventory()) == self.artifacts.keys(), 'restore_input_inventory_changed')
            return self.manifest_sha256
        except BaseException:
            self.failed = True
            raise
