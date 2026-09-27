import Foundation
import MatrixRustSDK

struct CheckFailure: Error { let message: String }

func check(_ condition: Bool, _ message: String) throws {
    if !condition { throw CheckFailure(message: message) }
}

@main
struct InspectionSmoke {
    static func main() async {
        do {
            try await run()
        } catch {
            print("FAIL: \(error)")
            exit(1)
        }
    }

    static func run() async throws {
        try await runInspectionCancellationBufferChecks()
        let baseURL = CommandLine.arguments[1]
        let client = try await ClientBuilder()
            .homeserverUrl(url: baseURL)
            .inMemoryStore()
            .requestConfig(config: RequestConfig(retryLimit: 0, timeout: 5000,
                                                  maxConcurrentRequests: nil, maxRetryTime: nil))
            .build()
        try await client.restoreSession(session: Session(
            accessToken: "test", refreshToken: nil, userId: "@alice:example.org",
            deviceId: "TEST", homeserverUrl: baseURL, oauthData: nil, slidingSyncVersion: .none))
        let room = try await client.joinRoomById(roomId: "!inspection:example.org")

        let result = try await room.inspectTimelineEvent(eventId: "$swift:example.org")
        // Joining without sync leaves the room version unknown. This must be conservative.
        try check(result.disposition == .indeterminate, "Unknown room version must be indeterminate")
        try check(result.decryptionFailure == nil, "Plaintext must not have a UTD reason")
        try check(result.event.eventId == "$swift:example.org", "Wrong original event ID")
        try check(result.event.roomId == "!inspection:example.org", "Wrong room ID")
        let raw = try JSONSerialization.jsonObject(with: Data(result.event.rawJson.utf8)) as! [String: Any]
        let content = raw["content"] as! [String: Any]
        let relation = content["m.relates_to"] as! [String: Any]
        try check(relation["event_id"] as? String == "$original:example.org", "Missing edit relation")
        try check(content["m.new_content"] != nil && raw["unsigned"] != nil, "Lost full JSON")
        print("PASS: linked inspection call, original identity and complete raw JSON")

        do {
            _ = try await room.inspectTimelineEvent(eventId: "invalid")
            throw CheckFailure(message: "Invalid event ID unexpectedly succeeded")
        } catch ClientError.Generic {}
        do {
            _ = try await room.inspectTimelineEvent(eventId: "$missing:example.org")
            throw CheckFailure(message: "Missing event unexpectedly succeeded")
        } catch ClientError.MatrixApi(_, let code, _, _) {
            try check(code == "M_NOT_FOUND", "Lost Matrix API error code")
        }
        print("PASS: validation and lookup errors cross UniFFI")

        let pending = Task { try await room.inspectTimelineEvent(eventId: "$cancel:example.org") }
        var requestStarted = false
        for _ in 0..<100 {
            let (data, _) = try await URLSession.shared.data(from: URL(string: baseURL + "/started")!)
            if String(decoding: data, as: UTF8.self) == "true" {
                requestStarted = true
                break
            }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        try check(requestStarted, "Cancellation test did not start the HTTP request")
        let cancelledAt = Date()
        pending.cancel()
        do {
            _ = try await pending.value
            throw CheckFailure(message: "Cancelled inspection unexpectedly succeeded")
        } catch is CancellationError {}
        try check(Date().timeIntervalSince(cancelledAt) < 2, "Cancellation waited for request timeout")
        print("PASS: Swift task cancellation terminates the UniFFI inspection promptly")

        let alreadyCancelled = Task {
            while !Task.isCancelled { await Task.yield() }
            return try await room.inspectTimelineEvent(eventId: "$never-requested:example.org")
        }
        alreadyCancelled.cancel()
        do {
            _ = try await alreadyCancelled.value
            throw CheckFailure(message: "Already cancelled inspection unexpectedly succeeded")
        } catch is CancellationError {}

        // Exercise cancel/complete/free races with actual UniFFI handles.
        for _ in 0..<100 {
            let racing = Task { try await room.inspectTimelineEvent(eventId: "$swift:example.org") }
            try await Task.sleep(nanoseconds: 500_000)
            racing.cancel()
            do { _ = try await racing.value } catch is CancellationError {}
        }
        print("PASS: cancellation before inspection and 100 completion/cancellation races")

        let cases: [RoomTimelineEventDisposition] = [.visible, .hidden, .unableToDecrypt, .indeterminate]
        try check(Set(cases).count == 4, "Disposition enum is incomplete")
        let record = RoomTimelineEventInspection(event: result.event, disposition: .unableToDecrypt,
                                                decryptionFailure: .unknown)
        try check(record.decryptionFailure == .unknown, "Existing EncryptedMessage type mismatch")
        print("PASS: public disposition and typed decryption failure contract")
    }
}
