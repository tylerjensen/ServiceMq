# Messages and delivery

ServiceMq supports typed JSON messages, caller-supplied text, and raw bytes. Every
received value is wrapped in a `Message` envelope containing identity and delivery
metadata.

![The outbound and inbound delivery lifecycle](images/delivery-lifecycle.svg)

---

## Typed objects

```csharp
Guid id = sender.Send(destination, new InvoiceReady
{
    InvoiceId = 123,
    PdfUri = "https://files.example/invoices/123.pdf"
});

Message message = receiver.Receive(5_000);
InvoiceReady invoice = message.To<InvoiceReady>();
```

The sender records `typeof(T).FullName` in `MessageTypeName` and serializes the value
with Newtonsoft.Json. `Message.To<T>()` deserializes the stored JSON into the requested
type. ServiceMq does not dynamically load `MessageTypeName`; the consumer chooses `T`.

Reference loops are ignored by the built-in settings. Prefer stable message contracts
with public properties and avoid sending domain entities whose shape changes often.

---

## Text messages

Use the non-generic overload when serialization is managed elsewhere:

```csharp
sender.Send(
    destination,
    messageType: "application/vnd.example.invoice-ready+json;v=2",
    message: json);
```

The text is stored exactly as supplied. Version 7 records encode fields safely, so tabs
and newlines in caller-supplied text do not corrupt the queue format.

---

## Binary messages

```csharp
sender.SendBytes(
    destination,
    pdfBytes,
    messageType: "application/pdf");

Message message = receiver.Receive(5_000);
byte[] pdf = message.MessageBytes;
```

File storage represents bytes as Base64 inside the record, which costs approximately
one third more space than the original byte array. Consider object storage plus a small
reference message for very large payloads.

---

## The `Message` envelope

| Member | Meaning |
| --- | --- |
| `Id` | Stable identity assigned by the sender; use it for idempotency |
| `From` | Sender address |
| `Sent` | Time the sender created the outbound message |
| `Received` | Time the destination stored the inbound message |
| `SendAttempt` | Delivery attempt that reached this receiver |
| `MessageTypeName` | CLR type name or caller-defined media/type name |
| `MessageString` | JSON or caller-supplied text |
| `MessageBytes` | Binary payload, mutually exclusive with `MessageString` |

---

## `Receive` versus `Accept`

### Immediate consumption

```csharp
Message message = queue.Receive(timeoutMs: 1_000);
```

`Receive` writes the read audit record, if enabled, and removes the incoming durable
record before returning. Use it when downstream processing is idempotent or the message
can safely be considered complete at read time.

### Explicit acknowledgment

```csharp
Message message = queue.Accept(timeoutMs: 1_000);
if (message != null)
{
    try
    {
        Process(message);
        queue.Acknowledge(message);
    }
    catch
    {
        queue.ReEnqueue(message);
    }
}
```

`Accept` removes the message from the in-memory ready queue but leaves its durable
record. `Acknowledge` audits and deletes it. `ReEnqueue` makes it ready again in the same
process.

If the process stops while a message is accepted, the durable record is loaded again on
the next start. With `VisibilityTimeout` configured, ServiceMq also requeues it in the
running process after the timeout (checked about once per second).

---

## Bulk operations

```csharp
IList<Message> messages = queue.AcceptBulk(
    maxMessagesToReceive: 100,
    timeoutMs: 5_000);

foreach (Message message in messages)
{
    Process(message);
    queue.Acknowledge(message);
}
```

`ReceiveBulk` immediately completes every returned message. `AcceptBulk` retains each
record until it is acknowledged or re-enqueued.

---

## Broadcasts

```csharp
Guid broadcastId = sender.Broadcast(
    new[] { accounting, analytics, notifications },
    new OrderPlaced { OrderId = 42 });
```

Each destination receives a separate stored copy with the same `Id` and `Sent` value.
Delivery and retries proceed independently for each destination.

---

## Delivery guarantee

ServiceMq is at-least-once, not exactly-once. The critical window is:

1. The receiver stores the message.
2. The receiver returns success over ServiceWire.
3. The sender deletes its outgoing record.

If the sender stops between steps 2 and 3, it will retry the same `Id` after restart.
Keep a processed-ID table or make the business operation naturally idempotent.

---

[← Back to the user guide](user-guide.md)
