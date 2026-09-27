
// Inspection-specific cancellation bridge for UniFFI 0.31.
// Uses UniFFI's standard rust_future_cancel/complete/free protocol. Other async SDK
// methods retain their generated behavior. Remove when the generator supports
// native Swift task cancellation and the linked cancellation tests pass.

fileprivate final class UniffiInspectionFuture: @unchecked Sendable {
    private let lock = NSLock()
    private var handle: UInt64?
    private var completing = false
    private let cancelFunc: @Sendable (UInt64) -> Void
    private let freeFunc: @Sendable (UInt64) -> Void

    init(handle: UInt64, cancelFunc: @escaping @Sendable (UInt64) -> Void,
         freeFunc: @escaping @Sendable (UInt64) -> Void) {
        self.handle = handle
        self.cancelFunc = cancelFunc
        self.freeFunc = freeFunc
    }

    func cancel() {
        lock.lock()
        defer { lock.unlock() }
        guard let handle, !completing else { return }
        cancelFunc(handle)
    }

    func complete<T>(_ body: () -> T) -> T {
        lock.lock()
        defer { lock.unlock() }
        // Complete even after cancellation: a ready result/error owns a lowered
        // RustBuffer that rust_future_free alone does not destroy in UniFFI 0.31.
        // Serialize completion with cancellation, then handle the buffers below.
        completing = true
        return body()
    }

    func free() {
        lock.lock()
        defer { lock.unlock() }
        guard let handle else { return }
        self.handle = nil
        // Serialize with cancel so that it can never use a freed Rust handle.
        freeFunc(handle)
    }
}

fileprivate func uniffiInspectionCallAsync<T>(
    rustFutureFunc: () -> UInt64,
    pollFunc: (UInt64, @escaping UniffiRustFutureContinuationCallback, UInt64) -> Void,
    completeFunc: (UInt64, UnsafeMutablePointer<RustCallStatus>) -> RustBuffer,
    freeFunc: @escaping @Sendable (UInt64) -> Void,
    cancelFunc: @escaping @Sendable (UInt64) -> Void,
    liftFunc: (RustBuffer) throws -> T,
    errorHandler: ((RustBuffer) throws -> Swift.Error)?
) async throws -> T {
    try Task.checkCancellation()
    uniffiEnsureMatrixSdkFfiInitialized()
    let handle = rustFutureFunc()
    let future = UniffiInspectionFuture(handle: handle, cancelFunc: cancelFunc, freeFunc: freeFunc)
    defer { future.free() }

    return try await withTaskCancellationHandler {
        var ready = false
        while !ready {
            let result = await withUnsafeContinuation { continuation in
                pollFunc(handle, { handle, result in
                    uniffiFutureContinuationCallback(handle: handle, pollResult: result)
                }, uniffiContinuationHandleMap.insert(obj: continuation))
            }
            ready = result == UNIFFI_RUST_FUTURE_POLL_READY
        }
        let (buffer, status) = future.complete {
            var status = RustCallStatus()
            let buffer = completeFunc(handle, &status)
            return (buffer, status)
        }
        if Task.isCancelled || status.code == CALL_CANCELLED {
            // Cancellation may race a successful result, a typed SDK error, or
            // a panic. Release both buffer slots before discarding the outcome.
            // Do not pass CALL_CANCELLED to UniFFI's Swift status checker: it
            // calls fatalError because its default helper has no cancellation.
            buffer.deallocate()
            status.errorBuf.deallocate()
            throw CancellationError()
        }
        if status.code != CALL_SUCCESS {
            // Error results return an empty/default value buffer. The status
            // checker consumes the error buffer, including typed errors/panics.
            buffer.deallocate()
        }
        do {
            try uniffiCheckCallStatus(callStatus: status, errorHandler: errorHandler)
            let result = try liftFunc(buffer)
            try Task.checkCancellation()
            return result
        } catch {
            // Conversion has already consumed its buffer. Prefer cancellation
            // if it arrived while converting a result or an SDK error.
            try Task.checkCancellation()
            throw error
        }
    } onCancel: {
        // Swift invokes this handler while holding a task status lock. UniFFI
        // can hold its scheduler lock while resuming a Swift continuation.
        // Calling into Rust here would invert those locks during a wake/cancel
        // race. The handle guard also makes a late queued cancellation safe.
        DispatchQueue.global().async { future.cancel() }
    }
}
