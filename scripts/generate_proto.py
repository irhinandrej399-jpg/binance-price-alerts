#!/usr/bin/env python3
"""Generate pinned Protobuf Go code; --check compares without writing the tree."""

import argparse
import hashlib
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile


ROOT = Path(__file__).resolve().parents[1]
PROTOC = "36.2"
GO_PLUGIN = "v1.36.12"
GRPC_PLUGIN = "v1.6.2"
# Official release asset digests for protocolbuffers/protobuf v36.2.
ARCHIVES = {
    ("Darwin", "arm64"): ("osx-aarch_64", "9cd98a532c5c5e0c4161314de0225de27e4c8a323917b6ea7b1b714d3ae23466"),
    ("Darwin", "x86_64"): ("osx-x86_64", "228cc7add4616cc14ca5e80dee83209d44449a7aee95a914ae748fa374efb078"),
    ("Linux", "aarch64"): ("linux-aarch_64", "8b8f18bd2b30346efbc698dd5a73dd7c805f3ef8380f6dfc95c768f3f1852f6a"),
    ("Linux", "x86_64"): ("linux-x86_64", "121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278"),
}


def run(*args, env=None):
    subprocess.run(args, cwd=ROOT, env=env, check=True, timeout=180)


def generated_files(directory):
    return {p.relative_to(directory): p.read_bytes() for p in directory.rglob("*.pb.go")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    system = (platform.system(), platform.machine())
    if system not in ARCHIVES:
        parser.error(f"unsupported platform: {system}; use Linux x86_64/arm64 or macOS x86_64/arm64")
    suffix, digest = ARCHIVES[system]
    with tempfile.TemporaryDirectory(prefix="binance-proto-") as temporary:
        work = Path(temporary)
        archive = work / "protoc.zip"
        url = f"https://github.com/protocolbuffers/protobuf/releases/download/v{PROTOC}/protoc-{PROTOC}-{suffix}.zip"
        with urllib.request.urlopen(url, timeout=60) as response:
            archive.write_bytes(response.read())
        if hashlib.sha256(archive.read_bytes()).hexdigest() != digest:
            raise SystemExit("protoc archive checksum mismatch")
        compiler = work / "protoc"
        with zipfile.ZipFile(archive) as source:
            # Only the executable is needed: schemas currently import no well-known types.
            compiler.write_bytes(source.read("bin/protoc"))
        compiler.chmod(0o755)
        tools = work / "tools"
        tools.mkdir()
        env = dict(os.environ, GOBIN=str(tools), GOTOOLCHAIN="local")
        run("go", "install", f"google.golang.org/protobuf/cmd/protoc-gen-go@{GO_PLUGIN}", env=env)
        run("go", "install", f"google.golang.org/grpc/cmd/protoc-gen-go-grpc@{GRPC_PLUGIN}", env=env)
        output = work / "generated"
        output.mkdir()
        schemas = sorted(str(p.relative_to(ROOT / "proto")) for p in (ROOT / "proto").rglob("*.proto"))
        if not schemas:
            raise SystemExit("no schemas found")
        run(str(compiler), f"--proto_path={ROOT / 'proto'}",
            f"--plugin=protoc-gen-go={tools / 'protoc-gen-go'}",
            f"--plugin=protoc-gen-go-grpc={tools / 'protoc-gen-go-grpc'}",
            f"--go_out={output}", "--go_opt=paths=source_relative",
            f"--go-grpc_out={output}", "--go-grpc_opt=paths=source_relative", *schemas)
        destination = ROOT / "internal/gen"
        expected = generated_files(output)
        current = generated_files(destination)
        if args.check:
            changed = sorted(str(p) for p in expected.keys() | current.keys() if expected.get(p) != current.get(p))
            if changed:
                raise SystemExit("generated code is stale; run python3 scripts/generate_proto.py:\n" + "\n".join(changed))
            print("Generated Protobuf code is current.")
        else:
            for path in current.keys() - expected.keys():
                (destination / path).unlink()
            for path in expected:
                target = destination / path
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(output / path, target)
            print(f"Generated {len(expected)} files (protoc {PROTOC}, Go {GO_PLUGIN}, gRPC {GRPC_PLUGIN}).")


if __name__ == "__main__":
    main()
