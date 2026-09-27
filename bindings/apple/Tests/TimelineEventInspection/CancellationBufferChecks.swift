// Appended only to the temporary copy of matrix_sdk_ffi.swift by check.py.
// Exercise the production bridge with controlled future completion and real
// RustBuffer allocations from the linked XCFramework. The temporary copy also
// observes deallocate(), without replacing the actual Rust buffer free call.

fileprivate struct InspectionBufferCheckFailure: Error, CustomStringConvertible {
    let description: String
}

fileprivate struct InspectionBufferExpectedError: Error {}

fileprivate final class InspectionBufferTracker: @unchecked Sendable {
    static let shared = InspectionBufferTracker()
    private let lock = NSLock()
    private var frees: [UInt: Int] = [:]

    func reset() {
        lock.lock()
        defer { lock.unlock() }
        frees.removeAll()
    }

    func allocate() -> RustBuffer {
        let buffer = FfiConverterString.lower(String(repeating: "x", count: 65_536))
        lock.lock()
        defer { lock.unlock() }
        frees[UInt(bitPattern: buffer.data!)] = 0
        return buffer
    }

    func willDeallocate(_ buffer: RustBuffer) {
        guard let data = buffer.data else { return }
        lock.lock()
        defer { lock.unlock() }
        let address = UInt(bitPattern: data)
        if let count = frees[address] { frees[address] = count + 1 }
    }

    func check(expectedAllocations: Int, scenario: String) throws {
        lock.lock()
        defer { lock.unlock() }
        guard frees.count == expectedAllocations, frees.values.allSatisfy({ $0 == 1 }) else {
            throw InspectionBufferCheckFailure(
                description: "\(scenario): expected \(expectedAllocations) buffers freed once, got \(Array(frees.values))")
        }
    }
}

fileprivate enum InspectionCancellationPoint {
    case none, beforeReady, duringComplete, duringLift, duringErrorLift, whilePending
}

fileprivate final class InspectionBufferFuture: @unchecked Sendable {
    private let lock = NSLock()
    private let code: Int8
    private let point: InspectionCancellationPoint
    private var pending: (UniffiRustFutureContinuationCallback, UInt64)?
    private var result = RustBuffer(capacity: 0, len: 0, data: nil)
    private var status = RustCallStatus()
    private var completeCount = 0
    private var freeCount = 0

    init(code: Int8, point: InspectionCancellationPoint) {
        self.code = code
        self.point = point
    }

    func poll(_ callback: @escaping UniffiRustFutureContinuationCallback, _ data: UInt64) {
        lock.lock()
        if point == .whilePending {
            pending = (callback, data)
        } else {
            status.code = code
            if code == CALL_SUCCESS {
                result = InspectionBufferTracker.shared.allocate()
            } else if code == CALL_ERROR || code == CALL_UNEXPECTED_ERROR {
                status.errorBuf = InspectionBufferTracker.shared.allocate()
            }
        }
        lock.unlock()
        if point == .beforeReady || point == .whilePending {
            withUnsafeCurrentTask { $0!.cancel() }
        }
        if point != .whilePending { callback(data, UNIFFI_RUST_FUTURE_POLL_READY) }
    }

    func cancel() {
        lock.lock()
        let callback = pending
        if callback != nil {
            pending = nil
            status.code = CALL_CANCELLED
        }
        lock.unlock()
        if let (callback, data) = callback { callback(data, UNIFFI_RUST_FUTURE_POLL_READY) }
    }

    func complete(_ outStatus: UnsafeMutablePointer<RustCallStatus>) -> RustBuffer {
        lock.lock()
        defer { lock.unlock() }
        completeCount += 1
        if point == .duringComplete { withUnsafeCurrentTask { $0!.cancel() } }
        outStatus.pointee = status
        return result
    }

    func free() {
        lock.lock()
        defer { lock.unlock() }
        freeCount += 1
        // Like UniFFI 0.31, releasing the future does not destroy a lowered buffer.
    }

    func check(scenario: String) throws {
        lock.lock()
        defer { lock.unlock() }
        guard completeCount == 1, freeCount == 1, pending == nil else {
            throw InspectionBufferCheckFailure(
                description: "\(scenario): complete=\(completeCount), free=\(freeCount), pending=\(pending != nil)")
        }
    }
}

public func runInspectionCancellationBufferChecks() async throws {
    let cases: [(String, Int8, InspectionCancellationPoint)] = [
        ("cancel with ready result", CALL_SUCCESS, .beforeReady),
        ("cancel with ready SDK error", CALL_ERROR, .beforeReady),
        ("cancel with ready panic", CALL_UNEXPECTED_ERROR, .beforeReady),
        ("cancel during completion", CALL_SUCCESS, .duringComplete),
        ("cancel during result conversion", CALL_SUCCESS, .duringLift),
        ("cancel during error conversion", CALL_ERROR, .duringErrorLift),
        ("cancel pending future", CALL_CANCELLED, .whilePending),
        ("Rust cancellation status", CALL_CANCELLED, .none),
        ("normal result", CALL_SUCCESS, .none),
        ("normal SDK error", CALL_ERROR, .none),
        ("normal panic", CALL_UNEXPECTED_ERROR, .none),
    ]
    for (scenario, code, point) in cases {
        InspectionBufferTracker.shared.reset()
        let future = InspectionBufferFuture(code: code, point: point)
        let task = Task {
            try await uniffiInspectionCallAsync(
                rustFutureFunc: { 1 },
                pollFunc: { _, callback, data in future.poll(callback, data) },
                completeFunc: { _, status in future.complete(status) },
                freeFunc: { _ in future.free() },
                cancelFunc: { _ in future.cancel() },
                liftFunc: { buffer in
                    let result = try FfiConverterString.lift(buffer)
                    if point == .duringLift { withUnsafeCurrentTask { $0!.cancel() } }
                    return result
                },
                errorHandler: { buffer in
                    _ = try FfiConverterString.lift(buffer)
                    if point == .duringErrorLift { withUnsafeCurrentTask { $0!.cancel() } }
                    return InspectionBufferExpectedError()
                })
        }
        let outcome = await task.result
        // Check ownership before checking the outcome so the old bridge fails
        // deterministically on the missing free, rather than a timing assertion.
        let allocations = point == .whilePending || code == CALL_CANCELLED ? 0 : 1
        try InspectionBufferTracker.shared.check(expectedAllocations: allocations, scenario: scenario)
        try future.check(scenario: scenario)
        let cancelled = point != .none || code == CALL_CANCELLED
        switch outcome {
        case .success(let value):
            guard !cancelled, code == CALL_SUCCESS, value.count == 65_536 else {
                throw InspectionBufferCheckFailure(description: "\(scenario): unexpected success")
            }
        case .failure(let error):
            let expected: Bool
            if cancelled { expected = error is CancellationError }
            else if code == CALL_ERROR { expected = error is InspectionBufferExpectedError }
            else if code == CALL_UNEXPECTED_ERROR, case UniffiInternalError.rustPanic = error {
                expected = true
            } else { expected = false }
            guard expected else {
                throw InspectionBufferCheckFailure(description: "\(scenario): unexpected error \(error)")
            }
        }
    }
    print("PASS: 11 deterministic cancellation/result/error cases free every Rust buffer exactly once")
}
