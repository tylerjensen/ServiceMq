# Security

[Back to the user guide](user-guide.md)

ServiceMq protects messages at rest when you configure a storage protector. Transport trust and access control remain deployment responsibilities.

## Protect durable payloads

Use `AesStorageProtector` when queue files or database values must be unreadable without an application-held key:

```csharp
var encodedKey = Environment.GetEnvironmentVariable("SERVICEMQ_STORAGE_KEY")
    ?? throw new InvalidOperationException("SERVICEMQ_STORAGE_KEY is required.");

var options = new MessageQueueOptions
{
    Name = "orders",
    Address = new Address("orders-pipe"),
    Storage = new StorageOptions
    {
        RootPath = @"D:\ServiceMq\orders",
        Protector = new AesStorageProtector(Convert.FromBase64String(encodedKey))
    }
};

using var queue = new MessageQueue(options);
```

Generate the key with a cryptographically secure source, keep it in a secret manager, and restrict which identities can retrieve it. Losing the key makes protected messages unrecoverable. Reusing a key broadly increases the impact of a compromise.

The built-in protector provides authenticated encryption for stored values. It does not encrypt:

- queue names and directory structure;
- operational counts and timestamps exposed through health APIs;
- network traffic between ServiceWire peers;
- payloads intentionally written to external application logs.

## Secure the transport

Named pipes are appropriate for trusted processes on the same machine. Apply operating-system controls to the service account and host so untrusted processes cannot reach the endpoint or read the storage root.

The ServiceMq TCP wrapper does not add TLS or peer authentication. Run TCP endpoints only on trusted private networks, behind appropriate firewall rules, or through a mutually authenticated encrypted tunnel. Do not expose an unauthenticated queue endpoint directly to the public internet.

## Trust serialized input

Receiving a message causes application-controlled data to be deserialized. Only connect to trusted peers, keep message contracts narrow, and validate payload values before using them. Avoid treating type metadata from an untrusted sender as authorization.

Typed messages should be simple data contracts. Keep privileged decisions—such as tenant identity, account ownership, or permission checks—in trusted application logic rather than trusting fields supplied by a message.

## Restrict storage access

- Grant the service identity read/write access only to its queue root.
- Keep other users and services out of that directory or database.
- Protect backups with the same controls as live queue data.
- Remember that sent/read audit records and dead letters may retain complete payloads.
- Choose retention periods that match both diagnostic needs and data-minimization requirements.
- Avoid logging message bodies, encryption keys, or raw corrupt records unless an approved diagnostic process requires them.

## Rotate an encryption key

ServiceMq does not silently rewrite all stored records with a new protector. A safe rotation workflow is application-specific:

1. Pause producers and drain or administratively account for active records.
2. Back up the store and verify that the old key can restore it.
3. Re-create or migrate retained records under the new protector.
4. Start the queue with the new key and validate delivery.
5. Retain the old key only as long as required for approved backups.

Test this process with representative data before rotating a production store.

## Security boundary summary

| Concern | Built in | Deployment responsibility |
|---|---:|---:|
| Authenticated encryption at rest | Optional | Key storage and rotation |
| Corrupt-record quarantine | Yes, file provider | Alerting and investigation |
| Named-pipe host isolation | Uses the host mechanism | OS identities and permissions |
| TCP confidentiality | No | TLS/VPN/tunnel and firewall |
| Peer authentication/authorization | No | Network and application architecture |
| Payload validation | No | Message handlers |

Next: [Operations](operations.md) or [Migration to v7](migrating-to-v7.md).
