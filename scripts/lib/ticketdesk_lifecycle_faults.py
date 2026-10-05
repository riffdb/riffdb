# req: END-003, END-007, END-008
"""Owned filesystem faults; these never open or modify a database engine."""
import os
from pathlib import Path
import stat


class FaultError(ValueError):
    """Closed diagnostics contain no path, identity, credential or input text."""


def require(condition, code):
    if not condition:
        raise FaultError(code)


class LocalArchiveOutage:
    """Temporarily deny native custody of the same private archive directory.

    The native connector checks custody before admitting a delivery. Retained
    read-only file descriptors here permit identity checks and reliable cleanup;
    they confer no archive acceptance or authoritative storage access.
    """

    def __init__(self, directory):
        self.path = Path(directory)
        self.directory = None
        self.file = None
        self.state = "ready"
        try:
            require(self.path.is_absolute() and self.path.resolve() == self.path,
                    "sink_custody")
            self.directory = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            self.file = os.open("archive.redb", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                dir_fd=self.directory)
            self.directory_identity = self.identity(os.fstat(self.directory))
            self.file_identity = self.identity(os.fstat(self.file))
            self.verify()
        except (OSError, FaultError) as error:
            self.release_descriptors()
            raise FaultError("sink_custody") from error

    @staticmethod
    def identity(info):
        return info.st_dev, info.st_ino

    def verify(self):
        try:
            directory = os.fstat(self.directory)
            file = os.fstat(self.file)
            named_directory = self.path.lstat()
            named_file = os.stat("archive.redb", dir_fd=self.directory, follow_symlinks=False)
            require(stat.S_ISDIR(directory.st_mode)
                    and stat.S_ISDIR(named_directory.st_mode)
                    and directory.st_uid == os.geteuid()
                    and stat.S_IMODE(directory.st_mode) == 0o700
                    and self.identity(directory) == self.directory_identity
                    and self.identity(named_directory) == self.directory_identity
                    and stat.S_ISREG(file.st_mode) and stat.S_ISREG(named_file.st_mode)
                    and file.st_uid == os.geteuid() and file.st_nlink == 1
                    and stat.S_IMODE(file.st_mode) == 0o600
                    and self.identity(file) == self.file_identity
                    and self.identity(named_file) == self.file_identity,
                    "sink_custody")
        except OSError as error:
            raise FaultError("sink_custody") from error

    def pause(self):
        require(self.state == "ready", "sink_fault_state")
        self.verify()
        try:
            os.fchmod(self.directory, 0)
            self.state = "paused"
        except OSError as error:
            raise FaultError("sink_fault_access") from error

    def resume(self):
        require(self.state == "paused", "sink_fault_state")
        try:
            require(self.identity(self.path.lstat()) == self.directory_identity,
                    "sink_custody")
            os.fchmod(self.directory, 0o700)
            self.verify()
            self.state = "recovered"
        except OSError as error:
            raise FaultError("sink_custody") from error

    def release_descriptors(self):
        for name in ("file", "directory"):
            descriptor = getattr(self, name)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, name, None)

    def __enter__(self):
        require(self.state == "ready", "sink_fault_state")
        return self

    def __exit__(self, _kind, _error, _traceback):
        try:
            if self.state == "paused":
                # Restore the opened object, even if its pathname was replaced.
                os.fchmod(self.directory, 0o700)
        except OSError as error:
            raise FaultError("sink_fault_cleanup") from error
        finally:
            self.release_descriptors()
            self.state = "closed"
        return False
