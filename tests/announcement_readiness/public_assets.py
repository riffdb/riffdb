"""Anonymous release inspection. Never reads a GitHub token or machine credentials."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tarfile
import urllib.request

MAX_DOWNLOAD = 256 * 1024 * 1024


def anonymous_download(url: str, destination: Path, limit: int = MAX_DOWNLOAD) -> str:
    # An empty proxy map prevents ambient authenticated proxy configuration from
    # turning an anonymous publication test into a credential-assisted success.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, headers={"User-Agent": "riffdb-announcement-readiness"})
    digest = hashlib.sha256()
    size = 0
    with opener.open(request, timeout=30) as response, destination.open("xb") as output:
        if response.status != 200:
            raise ValueError("anonymous_download_status")
        while chunk := response.read(1024 * 1024):
            size += len(chunk)
            if size > limit:
                raise ValueError("anonymous_download_size")
            digest.update(chunk)
            output.write(chunk)
    return digest.hexdigest()


def checksum_entries(content: bytes) -> dict[str, str]:
    if len(content) > 4 * 1024 * 1024:
        raise ValueError("publication_checksum_size")
    entries = {}
    for line in content.decode("ascii").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.{1,2048})", line)
        if match is None or match[2] in entries or len(entries) >= 16384:
            raise ValueError("publication_checksum_shape")
        path = PurePosixPath(match[2])
        if (path.is_absolute() or ".." in path.parts or str(path) != match[2] or "\\" in match[2]
                or any(ord(character) < 32 or ord(character) == 127 for character in match[2])):
            raise ValueError("publication_checksum_path")
        entries[match[2]] = match[1]
    return entries


def archive_identity(path: Path) -> dict:
    """Inspect bounded regular members without extracting executable content."""
    binaries = []
    binary_sha256 = {}
    driver_revision, driver_clean, driver_binaries = None, False, []
    revision = None
    roots = set()
    names = set()
    total = 0
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            if (name.is_absolute() or ".." in name.parts or "\\" in member.name
                    or member.name in names or len(names) >= 16384
                    or not (member.isfile() or member.isdir())):
                raise ValueError("publication_archive_shape")
            names.add(member.name)
            if not name.parts:
                raise ValueError("publication_archive_shape")
            roots.add(name.parts[0])
            total += member.size
            if member.size > MAX_DOWNLOAD or total > 1024 * 1024 * 1024:
                raise ValueError("publication_archive_size")
            if member.isfile() and len(name.parts) == 3 and name.parts[1] == "bin":
                binaries.append(name.parts[2])
                with archive.extractfile(member) as source:
                    binary_sha256[name.parts[2]] = hashlib.file_digest(source, "sha256").hexdigest()
            if member.isfile() and len(name.parts) == 4 and name.parts[1] == "bin":
                driver_binaries.append(name.parts[3])
            if member.isfile() and len(name.parts) == 3 and name.parts[1:] == ("metadata", "artifacts-v1.json"):
                if member.size > 4*1024*1024:
                    raise ValueError("publication_driver_metadata_size")
                metadata = json.loads(archive.extractfile(member).read(4*1024*1024 + 1))
                driver_revision, driver_clean = metadata.get("revision"), metadata.get("source_clean") is True
            if member.isfile() and len(name.parts) == 2 and name.parts[1] == "REVISION":
                if member.size > 128:
                    raise ValueError("publication_revision_size")
                revision = archive.extractfile(member).read(129).decode("ascii").strip()
    if len(roots) != 1:
        raise ValueError("publication_archive_roots")
    return {"binaries": sorted(binaries), "source_revision": revision, "binary_sha256": binary_sha256,
            "driver_source_revision": driver_revision, "driver_source_clean": driver_clean,
            "driver_binaries": sorted(driver_binaries)}


def public_release_assets_are_anonymously_available_and_checksum_complete(version: str, revision: str, scratch: Path, engine="podman") -> dict:
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("publication_version")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("publication_revision")
    target = "x86_64-unknown-linux-gnu"
    prefix = f"https://github.com/riffdb/riffdb/releases/download/v{version}/"
    product = f"riffdb-{version}-{target}.tar.gz"
    driver = f"riffdb-driver-distribution-{version}-{target}.tar.gz"
    scratch.mkdir(mode=0o700)
    anonymous_download(prefix + "SHA256SUMS", scratch / "SHA256SUMS", 4 * 1024 * 1024)
    entries = checksum_entries((scratch / "SHA256SUMS").read_bytes())
    product_digest = anonymous_download(prefix + product, scratch / product)
    driver_digest = anonymous_download(prefix + driver, scratch / driver)
    identity = archive_identity(scratch / product)
    driver_identity = archive_identity(scratch / driver)
    checks = {
        "anonymous_downloads": True,
        "product_inventory_complete": identity["binaries"] == ["riffdb", "riffdb-driverd", "riffdb-mcp", "riffdbd"],
        "product_checksum_complete": entries.get(product) == product_digest,
        "driver_checksum_complete": entries.get(driver) == driver_digest,
        "candidate_revision_matches": identity["source_revision"] == revision,
        "driver_revision_matches": driver_identity["driver_source_revision"] == revision and driver_identity["driver_source_clean"],
        "driver_inventory_complete": driver_identity["driver_binaries"] == ["riffdb", "riffdb-driverd"],
        "image_revision_matches": False,
        "image_binary_checksums_match": False,
    }
    from processes import Process, private_environment, private_write
    import os
    environment = private_environment(scratch)
    for name, fallback in (("XDG_DATA_HOME", Path.home() / ".local/share"),
                           ("XDG_CONFIG_HOME", Path.home() / ".config")):
        environment[name] = os.environ.get(name, str(fallback))
    if "XDG_RUNTIME_DIR" in os.environ:
        environment["XDG_RUNTIME_DIR"] = os.environ["XDG_RUNTIME_DIR"]
    auth = scratch / "empty-registry-auth.json"
    private_write(auth, '{"auths":{}}\n')
    image = f"ghcr.io/riffdb/riffdb:{version}"
    config = scratch / "docker-config"
    config.mkdir(mode=0o700)
    private_write(config / "config.json", '{"auths":{}}\n')
    environment["DOCKER_CONFIG"] = str(config)
    command = [engine, "pull"] + (["--authfile", auth] if engine == "podman" else []) + [image]
    pull = Process(command, cwd=scratch, env=environment)
    pull.wait(timeout=120, expected=None)
    checks["anonymous_image_pull"] = pull.process.returncode == 0
    pull.save_private_output(scratch, "anonymous-image-pull")
    image_digest = None
    if checks["anonymous_image_pull"]:
        from processes import run
        output, _ = run([engine, "image", "inspect", image], cwd=scratch, env=environment)
        descriptor = json.loads(output)[0]
        image_digest = descriptor["Id"]
        labels = descriptor.get("Labels") or descriptor.get("Config", {}).get("Labels", {})
        checks["image_revision_matches"] = labels.get("org.opencontainers.image.revision") == revision
        matches = []
        for binary in ("riffdb", "riffdbd"):
            output, _ = run([engine, "run", "--rm", "--pull=never", "--network=none", image_digest,
                             "sha256sum", f"/usr/local/bin/{binary}"], cwd=scratch, env=environment)
            matches.append(output.decode("ascii").split()[0] == identity["binary_sha256"].get(binary))
        checks["image_binary_checksums_match"] = all(matches)
    return {"checks": checks, "product_sha256": product_digest,
            "driver_sha256": driver_digest, "published": identity,
            "published_driver": driver_identity, "image_digest": image_digest}
