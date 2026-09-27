"""Exercise the generated Swift API and XCFramework against a local mock server.

Run after `cargo xtask swift build-framework --release --sequentially`:
    python3 bindings/apple/Tests/TimelineEventInspection/check.py
"""

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

started = threading.Event()
requests = []

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, status, value):
        body = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        path = unquote(urlsplit(self.path).path)
        if path == "/started":
            return self.reply(200, started.is_set())
        requests.append(("GET", path))
        if path == "/_matrix/client/versions":
            return self.reply(200, {"versions": ["v1.12"], "unstable_features": {}})
        if "/event/$cancel:example.org" in path:
            started.set()
            threading.Event().wait(30)
        if "/event/$swift:example.org" in path:
            return self.reply(200, {
                "type": "m.room.message", "event_id": "$swift:example.org",
                "room_id": "!inspection:example.org", "sender": "@alice:example.org",
                "origin_server_ts": 1000,
                "content": {"msgtype": "m.text", "body": "* Исправление",
                            "m.relates_to": {"rel_type": "m.replace", "event_id": "$original:example.org"},
                            "m.new_content": {"msgtype": "m.text", "body": "Исправление"}},
                "unsigned": {"age": 5, "transaction_id": "swift-smoke"}})
        return self.reply(404, {"errcode": "M_NOT_FOUND", "error": "Missing test event"})

    def do_POST(self):
        path = unquote(urlsplit(self.path).path)
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        requests.append(("POST", path))
        if path.endswith("/join"):
            return self.reply(200, {"room_id": "!inspection:example.org"})
        if path.endswith("/keys/upload"):
            return self.reply(200, {"one_time_key_counts": {}})
        return self.reply(404, {"errcode": "M_UNRECOGNIZED", "error": "Unexpected test request"})

PACKAGE = '''// swift-tools-version:5.7
import PackageDescription
let package = Package(
    name: "InspectionSmoke",
    platforms: [.macOS(.v12)],
    targets: [
        .binaryTarget(name: "MatrixSDKFFI", path: "MatrixSDKFFI.xcframework"),
        .target(name: "MatrixRustSDK", dependencies: ["MatrixSDKFFI"]),
        .executableTarget(name: "InspectionSmoke", dependencies: ["MatrixRustSDK"],
                          linkerSettings: [.linkedLibrary("sqlite3")]),
    ]
)
'''


def main():
    source = Path(__file__).resolve().parent
    generated = source.parents[1] / "generated"
    framework = generated / "MatrixSDKFFI.xcframework"
    if not framework.is_dir():
        raise SystemExit("Build the XCFramework with cargo xtask swift build-framework first")

    with tempfile.TemporaryDirectory(prefix="matrix-inspection-swift-") as directory:
        package = Path(directory)
        (package / "Package.swift").write_text(PACKAGE)
        shutil.copytree(generated / "swift", package / "Sources" / "MatrixRustSDK")
        # Instrument only the temporary binding copy, retaining the real Rust
        # deallocator. Tests share this file to access the private async bridge.
        binding = package / "Sources" / "MatrixRustSDK" / "matrix_sdk_ffi.swift"
        swift = binding.read_text()
        deallocate = "    func deallocate() {\n"
        if swift.count(deallocate) != 1:
            raise SystemExit("Unexpected RustBuffer deallocator; update the ownership checks")
        swift = swift.replace(deallocate, deallocate +
                              "        InspectionBufferTracker.shared.willDeallocate(self)\n")
        swift += "\n" + (source / "CancellationBufferChecks.swift").read_text()
        binding.write_text(swift)
        shutil.copytree(framework, package / "MatrixSDKFFI.xcframework")
        executable = package / "Sources" / "InspectionSmoke"
        executable.mkdir()
        shutil.copyfile(source / "InspectionSmoke.swift", executable / "main.swift")

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            subprocess.run(["swift", "run", "--package-path", str(package), "InspectionSmoke",
                            f"http://127.0.0.1:{server.server_port}"], check=True, timeout=300)
            assert not any("$never-requested" in path for _, path in requests), requests
            assert not any("/context/" in path or path.endswith("/messages")
                           for _, path in requests), requests
            print("PASS: cancelled-before-start and inspection make no unwanted lookup requests")
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    main()
