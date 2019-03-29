//
//  Created by Matt Greenfield on 29/07/16.
//  Copyright © 2016 Big Paua. All rights reserved.
//

import os.log
import CloudKit

public extension NSNotification.Name {
    static let updatedCloudKitQueueProgress = Notification.Name("updatedCloudKitQueueProgress")
}

public typealias CKFetchCompletion = (_ ckRecord: CKRecord?, _ error: Error?) -> Void
public typealias CKSaveCompletion = (_ ckRecord: CKRecord?, _ error: Error?) -> Void
public typealias CKDeleteCompletion = (_ error: Error?) -> Void

public class CloudKitQueue {

    public let database: CKDatabase

    // MARK: - Init

    public init(for database: CKDatabase) { self.database = database }

    deinit { pthread_mutex_destroy(&mutex) }

    // MARK: - Settings

    public var isPaused = false
    public var batchSize = 200

    // MARK: - CKOperation properties

    private var fetchOperation: CKFetchRecordsOperation?
    private var saveOperation: CKModifyRecordsOperation?
    private var deleteOperation: CKModifyRecordsOperation?
    
    private var slowFetchOperation: CKFetchRecordsOperation?
    private var slowSaveOperation: CKModifyRecordsOperation?
    private var slowDeleteOperation: CKModifyRecordsOperation?

    // MARK: - Queue properties

    private var recordsToFetch: [CKRecord.ID: [CKFetchCompletion]] = [:]
    private var recordsToSave: [CKRecord: [CKSaveCompletion]] = [:]
    private var recordsToDelete: [CKRecord.ID: [CKDeleteCompletion]] = [:]
    
    private var recordsToSlowFetch: [CKRecord.ID: [CKFetchCompletion]] = [:]
    private var recordsToSlowSave: [CKRecord: [CKSaveCompletion]] = [:]
    private var recordsToSlowDelete: [CKRecord.ID: [CKDeleteCompletion]] = [:]

    private var queuedFetches = 0 { didSet { needsProgressUpdate() } }
    private var queuedSaves = 0 { didSet { needsProgressUpdate() } }
    private var queuedDeletes = 0 { didSet { needsProgressUpdate() } }
    
    private var queuedSlowFetches = 0 { didSet { needsProgressUpdate() } }
    private var queuedSlowSaves = 0 { didSet { needsProgressUpdate() } }
    private var queuedSlowDeletes = 0 { didSet { needsProgressUpdate() } }

    public private(set) var quotaExceeded = false
    public private(set) var timeoutUntil: Date?

    private var progressUpdateTimer: Timer?

    private let queue = DispatchQueue(label: "CloudKitQueue")

    private var mutex: pthread_mutex_t = {
        var mutex = pthread_mutex_t()
        var attr = pthread_mutexattr_t()
        pthread_mutexattr_init(&attr)
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE)
        pthread_mutex_init(&mutex, &attr)
        return mutex
    }()

    // MARK: - Queue content checking

    public func isFetching(_ recordId: CKRecord.ID) -> Bool {
        return sync { self.recordsToFetch[recordId] != nil || self.recordsToSlowFetch[recordId] != nil }
    }
    
    public func isSaving(_ record: CKRecord) -> Bool {
        return sync { self.recordsToSave[record] != nil || self.recordsToSlowSave[record] != nil }
    }
    
    // MARK: - Fast queue

    public func fetch(_ recordId: CKRecord.ID, completion: @escaping CKFetchCompletion) {
        sync {
            if var existing = self.recordsToFetch[recordId] {
                existing.append(completion)
                self.recordsToFetch[recordId] = existing
                
            } else {
                self.recordsToFetch[recordId] = [completion]
                self.queuedFetches += 1
            }
        }
        runFetches()
    }
    
    public func save(_ record: CKRecord, completion: @escaping CKSaveCompletion) {
        sync {
            if var existing = recordsToSave[record] {
                existing.append(completion)
                self.recordsToSave[record] = existing
                
            } else {
                self.recordsToSave[record] = [completion]
                self.queuedSaves += 1
            }
        }
        runSaves()
    }
    
    public func delete(_ recordId: CKRecord.ID, completion: CKDeleteCompletion? = nil) {
        sync {
            if var existing = self.recordsToDelete[recordId] {
                if let completion = completion {
                    existing.append(completion)
                    self.recordsToDelete[recordId] = existing
                }
                
            } else {
                if let completion = completion {
                    self.recordsToDelete[recordId] = [completion]
                } else {
                    self.recordsToDelete[recordId] = []
                }
                self.queuedDeletes += 1
            }
        }
        runDeletes()
    }

    // MARK: - Slow queue

    public func slowFetch(_ recordId: CKRecord.ID, completion: @escaping CKFetchCompletion) {
        sync {
            if var existing = self.recordsToSlowFetch[recordId] {
                existing.append(completion)
                self.recordsToSlowFetch[recordId] = existing
                
            } else {
                self.recordsToSlowFetch[recordId] = [completion]
                self.queuedSlowFetches += 1
            }
        }
        runSlowFetches()
    }
    
    public func slowSave(_ record: CKRecord, completion: @escaping CKSaveCompletion) {
        sync {
            if var existing = self.recordsToSlowSave[record] {
                existing.append(completion)
                self.recordsToSlowSave[record] = existing
                
            } else {
                self.recordsToSlowSave[record] = [completion]
                self.queuedSlowSaves += 1
            }
        }
        runSlowSaves()
    }
    
    public func slowDelete(_ recordId: CKRecord.ID, completion: CKDeleteCompletion? = nil) {
        sync {
            if var existing = self.recordsToSlowDelete[recordId] {
                if let completion = completion {
                    existing.append(completion)
                    self.recordsToSlowDelete[recordId] = existing
                }
                
            } else {
                if let completion = completion {
                    self.recordsToSlowDelete[recordId] = [completion]
                } else {
                    self.recordsToSlowDelete[recordId] = []
                }
                self.queuedSlowDeletes += 1
            }
        }
        runSlowDeletes()
    }

    // MARK: - Queue counts

    public var queueTotal: Int { return sync { queuedFetches + queuedSaves + queuedDeletes } }
    public var slowQueueTotal: Int { return sync { queuedSlowFetches + queuedSlowSaves + queuedSlowDeletes } }
    
    public var queueRemaining: Int { return sync { recordsToFetch.count + recordsToSave.count + recordsToDelete.count } }
    public var slowQueueRemaining: Int { return sync { recordsToSlowFetch.count + recordsToSlowSave.count + recordsToSlowDelete.count } }

    // MARK: - Queue progress

    public var progress: Double {
        return sync {
            guard queueTotal > 0 else { return 1 }
            return (1.0 - Double(queueRemaining) / Double(queueTotal)).clamped(min: 0, max: 1)
        }
    }

    public var slowProgress: Double {
        return sync {
            guard slowQueueTotal > 0 else { return 1 }
            return (1.0 - Double(slowQueueRemaining) / Double(slowQueueTotal)).clamped(min: 0, max: 1)
        }
    }

    private func needsProgressUpdate() {
        onMain {
            guard self.progressUpdateTimer == nil else { return }

            self.progressUpdateTimer = Timer.scheduledTimer(withTimeInterval: 0.3, repeats: false) { _ in
                self.sendProgressUpdate()
            }
        }
    }

    private func sendProgressUpdate() {
        progressUpdateTimer = nil
        let note = Notification(name: .updatedCloudKitQueueProgress, object: self, userInfo: nil)
        NotificationCenter.default.post(note)
    }

    // MARK: - Running the fast queue

    private func runFetches() {
        queue.async {
            if self.isPaused { return }

            guard self.fetchOperation == nil else { return }
            
            guard !self.timedout else {
                self.afterTimeout { self.runFetches() }
                return
            }
          
            var recordIds = self.sync { Array(self.recordsToFetch.keys) }
            
            let batch = recordIds.count <= self.batchSize ? recordIds : Array(recordIds[0 ..< self.batchSize])
            
            let operation = CKFetchRecordsOperation(recordIDs: batch)
            operation.configuration.qualityOfService = .userInitiated
            operation.configuration.allowsCellularAccess = true
            self.fetchOperation = operation
            
            operation.perRecordCompletionBlock = { ckRecord, ckRecordId, error in
                guard let ckRecordId = ckRecordId else { return }
                
                self.queue.async {
                    guard let completions = self.sync(execute: { self.recordsToFetch[ckRecordId] }) else { return }

                    self.sync { self.recordsToFetch.removeValue(forKey: ckRecordId) }

                    for completion in completions {
                        completion(ckRecord, error)
                    }

                    self.needsProgressUpdate()
                }
            }
            
            operation.fetchRecordsCompletionBlock = { ckRecords, error in
                if let error = error as NSError? {
                    if !self.rateLimited(error) && error.code != CKError.Code.partialFailure.rawValue {
                        os_log("fetchRecordsCompletionBlock error: %@", type: .error, error.localizedDescription)
                    }
                }
                
                self.queue.async {
                    self.fetchOperation = nil
                    
                    if self.recordsToFetch.isEmpty {
                        self.queuedFetches = 0

                    } else {
                        self.runFetches()
                    }
                }
            }
            
            self.database.add(operation)
        }
    }
    
    private func runSaves() {
        queue.async {
            if self.isPaused { return }

            guard self.saveOperation == nil else { return }
            
            guard !self.timedout else {
                self.afterTimeout { self.runSaves() }
                return
            }
            
            let records = self.sync { Array(self.recordsToSave.keys) }
            
            let batch = records.count <= self.batchSize ? records : Array(records[0 ..< self.batchSize])
            
            let operation = CKModifyRecordsOperation(recordsToSave: batch, recordIDsToDelete: nil)
            operation.configuration.qualityOfService = .userInitiated
            operation.configuration.allowsCellularAccess = true
            operation.isAtomic = false
            self.saveOperation = operation
            
            operation.perRecordCompletionBlock = { ckRecord, error in
                self.queue.async {
                    if let error = error as NSError?, error.code == CKError.Code.quotaExceeded.rawValue {
                        self.quotaExceeded = true
                    }

                    guard let completions = self.sync(execute: { self.recordsToSave[ckRecord] }) else { return }

                    self.sync { self.recordsToSave.removeValue(forKey: ckRecord) }

                    for completion in completions {
                        completion(ckRecord, error)
                    }

                    self.needsProgressUpdate()
                }
            }

            operation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIds, error in
                if let error = error as NSError? {
                    if error.code == CKError.Code.quotaExceeded.rawValue { self.quotaExceeded = true }

                    if !self.rateLimited(error) && error.code != CKError.Code.partialFailure.rawValue {
                        os_log("modifyRecordsCompletionBlock error: %@", type: .error, error.localizedDescription)

                        self.sync {
                            for ckRecord in batch {
                                guard let completions = self.recordsToSave[ckRecord] else { continue }

                                self.recordsToSave.removeValue(forKey: ckRecord)

                                for completion in completions {
                                    completion(ckRecord, error)
                                }
                            }
                        }
                    }
                }

                self.queue.async {
                    self.saveOperation = nil

                    if self.recordsToSave.isEmpty {
                        self.queuedSaves = 0

                    } else {
                        self.runSaves()
                    }
                }
            }
            
            self.database.add(operation)
        }
    }
    
    private func runDeletes() {
        queue.async {
            if self.isPaused { return }

            guard self.deleteOperation == nil else { return }
            
            guard !self.timedout else {
                self.afterTimeout { self.runDeletes() }
                return
            }
            
            let recordIds = self.sync { Array(self.recordsToDelete.keys) }
            let batch = recordIds.count <= self.batchSize ? recordIds : Array(recordIds[0 ..< self.batchSize])
            
            let operation = CKModifyRecordsOperation(recordsToSave: nil, recordIDsToDelete: batch)
            operation.configuration.qualityOfService = .userInitiated
            operation.configuration.allowsCellularAccess = true
            operation.isAtomic = false
            self.deleteOperation = operation
            
            operation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIds, error in
                if let error = error as NSError? {
                    if !self.rateLimited(error) && error.code != CKError.Code.partialFailure.rawValue {
                        os_log("modifyRecordsCompletionBlock error: %@", type: .error, error.localizedDescription)
                    }
                }
                
                self.queue.async {
                    self.deleteOperation = nil
                    
                    if let deletedRecordIds = deletedRecordIds {
                        for recordId in deletedRecordIds {
                            guard let completions = self.sync(execute: { self.recordsToDelete[recordId] }) else { return }

                            self.sync { self.recordsToDelete.removeValue(forKey: recordId) }

                            for completion in completions {
                                completion(error)
                            }
                        }
                    }
                    
                    if self.recordsToDelete.isEmpty {
                        self.queuedDeletes = 0

                    } else {
                        self.runDeletes()
                    }
                }
            }
            
            self.database.add(operation)
        }
    }

    // MARK: - Running the slow queue

    private func runSlowFetches() {
        queue.async {
            if self.isPaused { return }

            guard self.slowFetchOperation == nil else { return }

            guard !self.timedout else {
                self.afterTimeout { self.runSlowFetches() }
                return
            }
            
            let recordIds = self.sync { Array(self.recordsToSlowFetch.keys) }
            let batch = recordIds.count <= self.batchSize ? recordIds : Array(recordIds[0 ..< self.batchSize])
            
            let operation = CKFetchRecordsOperation(recordIDs: batch)
            operation.configuration.qualityOfService = .background
            operation.configuration.allowsCellularAccess = false
            self.slowFetchOperation = operation
            
            operation.perRecordCompletionBlock = { ckRecord, ckRecordId, error in
                guard let ckRecordId = ckRecordId else { return }
                
                self.queue.async {
                    guard let completions = self.sync(execute: { self.recordsToSlowFetch[ckRecordId] }) else { return }

                    self.sync { self.recordsToSlowFetch.removeValue(forKey: ckRecordId) }

                    for completion in completions {
                        completion(ckRecord, error)
                    }

                    self.needsProgressUpdate()
                }
            }
            
            operation.fetchRecordsCompletionBlock = { ckRecords, error in
                if let error = error as NSError? {
                    if !self.rateLimited(error) && error.code != CKError.Code.partialFailure.rawValue {
                        os_log("fetchRecordsCompletionBlock error: %@", type: .error, error.localizedDescription)
                    }
                }
                
                self.queue.async {
                    self.slowFetchOperation = nil
                    
                    if self.recordsToSlowFetch.isEmpty {
                        self.queuedSlowFetches = 0

                    } else {
                        self.runSlowFetches()
                    }
                }
            }
            
            self.database.add(operation)
        }
    }
    
    private func runSlowSaves() {
        queue.async {
            if self.isPaused { return }

            guard self.slowSaveOperation == nil else { return }
            
            guard !self.timedout else {
                self.afterTimeout { self.runSlowSaves() }
                return
            }

            let records = self.sync { Array(self.recordsToSlowSave.keys) }
            
            let batch = records.count <= self.batchSize ? records : Array(records[0 ..< self.batchSize])
            
            let operation = CKModifyRecordsOperation(recordsToSave: batch, recordIDsToDelete: nil)
            operation.configuration.qualityOfService = .background
            operation.configuration.allowsCellularAccess = false
            operation.isAtomic = false
            self.slowSaveOperation = operation
            
            operation.perRecordCompletionBlock = { ckRecord, error in
                self.queue.async {
                    if let error = error as NSError?, error.code == CKError.Code.quotaExceeded.rawValue {
                        self.quotaExceeded = true
                    }

                    guard let completions = self.sync(execute: { self.recordsToSlowSave[ckRecord] }) else { return }

                    self.sync { self.recordsToSlowSave.removeValue(forKey: ckRecord) }

                    for completion in completions {
                        completion(ckRecord, error)
                    }

                    self.needsProgressUpdate()
                }
            }

            operation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIds, error in
                if let error = error as NSError? {
                    if error.code == CKError.Code.quotaExceeded.rawValue { self.quotaExceeded = true }

                    if !self.rateLimited(error) && error.code != CKError.Code.partialFailure.rawValue {
                        os_log("modifyRecordsCompletionBlock error: %@", type: .error, error.localizedDescription)

                        self.sync {
                            for ckRecord in batch {
                                if let completions = self.recordsToSlowSave[ckRecord] {
                                    self.recordsToSlowSave.removeValue(forKey: ckRecord)

                                    for completion in completions {
                                        completion(ckRecord, error)
                                    }
                                }
                            }
                        }
                    }
                }

                self.queue.async {
                    self.slowSaveOperation = nil

                    if self.recordsToSlowSave.isEmpty {
                        self.queuedSlowSaves = 0

                    } else {
                        self.runSlowSaves()
                    }
                }
            }
            
            self.database.add(operation)
        }
    }
    
    private func runSlowDeletes() {
        queue.async {
            if self.isPaused { return }

            guard self.slowDeleteOperation == nil else { return }
            
            guard !self.timedout else {
                self.afterTimeout { self.runSlowDeletes() }
                return
            }
            
            let recordIds = self.sync { Array(self.recordsToSlowDelete.keys) }
            
            let batch = recordIds.count <= self.batchSize ? recordIds : Array(recordIds[0 ..< self.batchSize])
            
            let operation = CKModifyRecordsOperation(recordsToSave: nil, recordIDsToDelete: batch)
            operation.configuration.qualityOfService = .background
            operation.configuration.allowsCellularAccess = false
            operation.isAtomic = false
            self.slowDeleteOperation = operation
            
            operation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIds, error in
                if let error = error as NSError? {
                    if !self.rateLimited(error) && error.code != CKError.Code.partialFailure.rawValue {
                        os_log("modifyRecordsCompletionBlock error: %@", type: .error, error.localizedDescription)
                    }
                }
                
                self.queue.async {
                    self.slowDeleteOperation = nil
                    
                    if let deletedRecordIds = deletedRecordIds {
                        for recordId in deletedRecordIds {
                            guard let completions = self.sync(execute: { self.recordsToSlowDelete[recordId] }) else { return }

                            self.sync { self.recordsToSlowDelete.removeValue(forKey: recordId) }

                            for completion in completions {
                                completion(error)
                            }
                        }
                    }
                    
                    if self.recordsToSlowDelete.isEmpty {
                        self.queuedSlowDeletes = 0

                    } else {
                        self.runSlowDeletes()
                    }
                }
            }
            
            self.database.add(operation)
        }
    }

    // MARK: - Rate limit management

    func rateLimited(_ error: NSError) -> Bool {
        let rateLimitErrors = [CKError.requestRateLimited.rawValue, CKError.zoneBusy.rawValue]

        guard rateLimitErrors.contains(error.code) else { return false }

        sync {
            if let timeout = error.userInfo[CKErrorRetryAfterKey] as? TimeInterval {
                os_log("CloudKit timeout: %.1f", type: .debug, timeout)
                self.timeoutUntil = Date(timeIntervalSinceNow: timeout)

            } else {
                os_log("CloudKit timeout", type: .debug)
                self.timeoutUntil = Date(timeIntervalSinceNow: 60)
            }
        }

        return true
    }

    public var timedout: Bool {
        guard let untilTimeoutOver = timeoutUntil?.timeIntervalSinceNow else { return false }
        return untilTimeoutOver > 0
    }

    func afterTimeout(_ closure: @escaping () -> ()) {
        let timeout = sync { self.timeoutUntil }

        if let timeout = timeout {
            delay(timeout.timeIntervalSinceNow) {
                closure()
            }
        } else {
            closure()
        }
    }

    // MARK: - Mutex, etc

    @discardableResult private func sync<R>(execute work: () throws -> R) rethrows -> R {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }
        return try work()
    }

}

// MARK: - Helpers

func onMain(_ closure: @escaping () -> ()) {
    if Thread.isMainThread { closure() }
    else { DispatchQueue.main.async(execute: closure) }
}

internal func delay(_ delay: Double, closure: @escaping () -> ()) {
    DispatchQueue.main.asyncAfter(
        deadline: DispatchTime.now() + Double(Int64(delay * Double(NSEC_PER_SEC))) / Double(NSEC_PER_SEC),
        execute: closure)
}

internal extension Comparable {

    mutating func clamp(min: Self, max: Self) {
        if self < min { self = min }
        if self > max { self = max }
    }

    func clamped(min: Self, max: Self) -> Self {
        var result = self
        if result < min { result = min }
        if result > max { result = max }
        return result
    }

}
