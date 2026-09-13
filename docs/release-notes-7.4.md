# ServiceMq 7.4.0

ServiceMq, ServiceMq.Sqlite, and ServiceMq.SharpCoreDb advance together to 7.4.0.
This release includes the SharpCoreDB 2.0.0.3 upgrade and async APIs contributed in
PR #2, with the following reliability fixes:

- Async receives honor cancellation while waiting. Once a message or batch is
  dequeued, completion finishes and returns it even if cancellation is requested,
  preventing cancellation from losing messages already removed from storage.
- Async acknowledgement retains the visibility lease until storage completion
  succeeds. Completion cannot expire the lease mid-operation, and storage failures
  leave it available for redelivery. Cancellation before completion leaves it unchanged.
- Async storage deletion and movement update configured message and byte limits and
  wake blocked writers. Capacity-sensitive reads and purges use the same synchronization
  as synchronous operations.
- Async file writes and appends using `FlushToDisk` flush operating-system buffers
  before succeeding, matching synchronous durability.

The publishing workflow tests the solution and packs all three libraries. Core and
SQLite continue to target .NET Standard 2.0 and .NET 8; SharpCoreDB requires .NET 10.

See [Async API](async.md) for cancellation and acknowledgement behavior. Applications
requiring recovery until processing finishes should use `AcceptAsync` followed by
`AcknowledgeAsync`.
