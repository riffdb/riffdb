"""Generic checksum-pinned Collector fixture support; no adapter dependencies."""
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
from public_assets import anonymous_download

def sha256(path, limit=256*1024*1024):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise RuntimeError("fixture_artifact_type_or_bound")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def prepare_collector(binary: Path, fixtures: Path):
    pin = json.loads((fixtures / "collector-v1.json").read_text())
    archive_path = binary.parent / pin["archive"]
    url = f"https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/{pin['release_tag']}/{pin['archive']}"
    if anonymous_download(url, archive_path, pin["maximum_archive_bytes"]) != pin["archive_sha256"]:
        raise RuntimeError("collector_archive_checksum")
    with tarfile.open(archive_path, "r|gz") as archive:
        count, found = 0, False
        for member in archive:
            count += 1
            if count > 32 or not member.isfile() or member.size > pin["maximum_binary_bytes"]:
                raise RuntimeError("collector_archive_shape")
            if Path(member.name).name != "otelcol-contrib":
                continue
            if found:
                raise RuntimeError("collector_archive_duplicate")
            found = True
            with archive.extractfile(member) as source, binary.open("xb") as destination:
                shutil.copyfileobj(source, destination, length=1024*1024)
    if not found or sha256(binary, pin["maximum_binary_bytes"]) != pin["binary_sha256"]:
        raise RuntimeError("collector_binary_checksum")
    binary.chmod(0o755)
