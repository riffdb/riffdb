"""Race-resistant bounded file I/O for release evidence scripts."""

from __future__ import annotations

import os
from pathlib import Path
import stat


def _absolute_parts(path: Path) -> tuple[Path, tuple[str, ...]]:
    if ".." in path.parts:
        raise ValueError(f"evidence path contains a parent traversal: {path}")
    absolute = path if path.is_absolute() else Path.cwd() / path
    return absolute, tuple(part for part in absolute.parts if part not in (absolute.anchor, "."))


def _open_directory(parts: tuple[str, ...], *, create: bool) -> int:
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in parts:
            try:
                child = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=descriptor,
                )
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, mode=0o700, dir_fd=descriptor)
                child = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=descriptor,
                )
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def bounded_read(path: Path, maximum: int, label: str) -> bytes:
    absolute, parts = _absolute_parts(path)
    if not parts:
        raise ValueError(f"{label} has no file name: {absolute}")
    parent = _open_directory(parts[:-1], create=False)
    try:
        descriptor = os.open(
            parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent
        )
    finally:
        os.close(parent)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{label} is not a regular file: {absolute}")
        if not 0 < metadata.st_size <= maximum:
            raise ValueError(f"{label} exceeds its byte bound: {absolute}")
        chunks: list[bytes] = []
        retained = 0
        while retained <= maximum:
            chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - retained))
            if not chunk:
                break
            chunks.append(chunk)
            retained += len(chunk)
        encoded = b"".join(chunks)
        if not 0 < len(encoded) <= maximum or os.read(descriptor, 1):
            raise ValueError(f"{label} exceeds its byte bound: {absolute}")
        return encoded
    finally:
        os.close(descriptor)


def exclusive_write(path: Path, encoded: bytes, maximum: int, label: str) -> Path:
    if not 0 < len(encoded) <= maximum:
        raise ValueError(f"{label} exceeds its byte bound")
    absolute, parts = _absolute_parts(path)
    if not parts:
        raise ValueError(f"{label} has no file name: {absolute}")
    parent = _open_directory(parts[:-1], create=True)
    try:
        descriptor = os.open(
            parts[-1],
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent,
        )
        try:
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short evidence write")
                view = view[written:]
            os.fsync(descriptor)
            os.fsync(parent)
        finally:
            os.close(descriptor)
    finally:
        os.close(parent)
    return absolute


def exclusive_empty(path: Path, label: str) -> Path:
    absolute, parts = _absolute_parts(path)
    if not parts:
        raise ValueError(f"{label} has no file name: {absolute}")
    parent = _open_directory(parts[:-1], create=True)
    try:
        descriptor = os.open(
            parts[-1],
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent,
        )
        try:
            os.fsync(descriptor)
            os.fsync(parent)
        finally:
            os.close(descriptor)
    finally:
        os.close(parent)
    return absolute


def bounded_append(path: Path, encoded: bytes, maximum: int, label: str) -> None:
    absolute, parts = _absolute_parts(path)
    if not encoded or len(encoded) > maximum or not parts:
        raise ValueError(f"{label} append is invalid")
    parent = _open_directory(parts[:-1], create=False)
    try:
        descriptor = os.open(
            parts[-1], os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size + len(encoded) > maximum:
                raise ValueError(f"{label} exceeds its byte bound: {absolute}")
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short evidence append")
                view = view[written:]
            os.fsync(descriptor)
            os.fsync(parent)
        finally:
            os.close(descriptor)
    finally:
        os.close(parent)


def exclusive_copy(source: Path, destination: Path, maximum: int, label: str) -> Path:
    return exclusive_write(destination, bounded_read(source, maximum, label), maximum, label)


def reject_symlink_components(path: Path, *, allow_missing_leaf: bool) -> Path:
    """Static path check for shell callers; bounded_read/exclusive_write stay race-safe."""
    absolute, parts = _absolute_parts(path)
    current = Path("/")
    for index, part in enumerate(parts):
        current /= part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if allow_missing_leaf and index == len(parts) - 1:
                return absolute
            raise
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"evidence path contains a symbolic link: {current}")
    return absolute


# req: PERF-018
def run_self_test(root: Path) -> None:
    root.mkdir(mode=0o700)
    evidence = root / "nested" / "receipt.json"
    exclusive_write(evidence, b"{}\n", 16, "fixture output")
    assert bounded_read(evidence, 16, "fixture input") == b"{}\n"
    for refused in (evidence,):
        try:
            exclusive_write(refused, b"{}\n", 16, "existing fixture output")
        except FileExistsError:
            pass
        else:
            raise AssertionError("existing evidence output was overwritten")
    linked_parent = root / "linked-parent"
    linked_parent.symlink_to(root / "nested", target_is_directory=True)
    linked_leaf = root / "linked.json"
    linked_leaf.symlink_to(evidence)
    for refused in (linked_parent / "receipt.json", linked_leaf):
        try:
            bounded_read(refused, 16, "linked fixture input")
        except OSError:
            pass
        else:
            raise AssertionError("linked evidence input was followed")
    try:
        bounded_read(root / "missing" / "receipt.json", 16, "missing fixture input")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("missing intermediate evidence path was accepted")

    growing = root / "growing.json"
    growing.write_bytes(b"12345678")
    append_descriptor = os.open(growing, os.O_WRONLY | os.O_APPEND | os.O_CLOEXEC)
    original_read = os.read
    first = True

    def append_before_read(descriptor: int, count: int) -> bytes:
        nonlocal first
        if first:
            first = False
            os.write(append_descriptor, b"9")
            os.fsync(append_descriptor)
        return original_read(descriptor, count)

    os.read = append_before_read
    try:
        try:
            bounded_read(growing, 8, "growing fixture input")
        except ValueError:
            pass
        else:
            raise AssertionError("evidence growth after fstat exceeded its cap")
    finally:
        os.read = original_read
        os.close(append_descriptor)
