# CloudKitQueues

A queue manager for simplifying batch operations on CloudKit records.

## Seamless Individual and Batch Operations 

CloudKitQueues provides methods for performing fetch, save, and delete actions on individual CKRecords 
and CKRecordIDs. The actions are then intelligently batched together, to get the fastest results in the 
least amount of CloudKit requests. 

This makes it possible to get the best possible performance out of CloudKit, with the minimum 
number of requests within CloudKit's request limits. 

## Individual Completion Closures 

Every fetch, save, and delete request takes an optional completion closure, which will be executed
once the record's database action has completed. This allows the batching of hundreds of concurrent
database operations together, while being able to easily perform completion actions on each record
individually. 

```swift
queue.fetch(ckRecordId) { ckRecord, error in
    if let ckRecord = ckRecord {
        print("ckRecord: " + ckRecord)
    }
}
```

## Automatic Rate Limit Management

CloudKitQueues observes and respects CloudKit's rate limit timeouts, and automatifally retries once the 
timeouts permit. 

## Fast and Slow Queues

CloudKitQueues manages two sets of queues: one for fast operations (eg actions for which the user is 
waiting to see the result of in the UI), and one slow operations (eg record backups, restores, and 
deletes that the user is not expecting to see the immediate results of).

So for example if fetching a record, you can choose to either `fetch(ckRecordId)` or `slowFetch(ckRecordId)`.
Or if saving changes to a record, you can choose to either `save(ckRecord)` or 
`slowSave(ckRecord)`.

CloudKitQueues manages these queues internally so that "fast" actions are batched together and 
happen as quickly as possible, and "slow" actions are batched together and performed when  
CloudKit determines that the energy and network conditions best suit.

## Setup

```ruby
pod 'CloudKitQueues'
```

Or just drop [CloudKitQueue.swift](https://github.com/sobri909/CloudKitQueues/blob/master/Source/CloudKitQueue.swift) into your project. 

## Examples

#### Fast Queue Actions

```swift
let publicQueue = CloudKitQueue(for: CKContainer.default().publicCloudDatabase)
let privateQueue = CloudKitQueue(for: CKContainer.default().privateCloudDatabase)
```

```swift
// save the CKRecords for all cars
for car in cars { 
    publicQueue.save(car.ckRecord) 
}
```

```swift
// fetch the CKRecords for all cars, and assign the fetched records
// to their car objects on fetch completion
for car in cars { 
    publicQueue.fetch(car.ckRecordId) { ckRecord, error in
        car.ckRecord = ckRecord
    }
}
```

```swift
// delete the remote CKRecords for all cars
for car in cars { 
    publicQueue.delete(car.ckRecordId)
}
```

#### Slow Queue Actions

```swift
// slow save the CKRecords for all cars
for car in cars { 
    publicQueue.slowSave(car.ckRecord) 
}
```

```swift
// slow fetch the CKRecords for all cars, and assign the fetched records
// to their car objects on fetch completion
for car in cars { 
    publicQueue.slowFetch(car.ckRecordId) { ckRecord, error in
        car.ckRecord = ckRecord
    }
}
```

```swift
// slow delete the remote CKRecords for all cars
for car in cars { 
    publicQueue.slowDelete(car.ckRecordId)
}
```
