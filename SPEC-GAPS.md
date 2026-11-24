# Specification gaps

Places where the [library specification](https://snodra.damoreira.com/library-authors/) is silent. The Kotlin library
is the reference: this library does on the wire what the Kotlin library does, and where an entry is settled its
behaviour is the specification. The code carries a `# TODO(spec-gap): SG-<n>` comment at the spot.

## Format of an entry

### SG-<n>: short title

- **Specification:** what the specification says, or that it is silent.
- **Kotlin library (the reference):** what it does, as `snodra-messaging@<sha>:<path>:<line>`.
- **Other libraries:** what the Go and Rust libraries do where they differ.
- **This library:** what the Python code does, and in which file.
- **Question:** the open question, or "settled: follows the Kotlin library".

## Entries

### SG-1: the error texts a caller reads and a handler writes

- **Specification:** `messages.md` (Handler errors) says an `Error.message` is a short text, "never a stack trace", and
  that the Kotlin library writes `<ExceptionSimpleName>: <message>`. It gives no text for the codes the hub itself
  produces, and no rule for which exception a handler raises to answer a given code.
- **Kotlin library (the reference):** the caller reads fixed texts for `NO_HANDLER`, `TIMEOUT`, `HANDLER_DISCONNECTED`
  and `RESOURCE_EXHAUSTED` (`snodra-messaging@ecc4207:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/MessagingExceptions.kt:7`,
  `:10`, `:13`, `:65`), `cancelled by the hub` for an empty `CANCELLED` message and the wire message for every other code
  (`.../hub/HubErrors.kt:26-38`). A handler's exception becomes `UNAUTHENTICATED` or `PERMISSION_DENIED` with its own
  message, an `IllegalArgumentException` a `HANDLER_ERROR` with its plain message and the details `[]`, anything else a
  `HANDLER_ERROR` `<SimpleName>: <message>` (`.../handler/HandlerInvoker.kt:231-246`, `:249`).
- **Other libraries:** not compared.
- **This library:** `snodra/_errors.py` (`error_from_wire`, `error_to_wire`) uses the same texts. `ValueError` plays
  `IllegalArgumentException`, which is Python's own choice; `HandlerError(message, details=...)` and `InvalidRequest`
  have no Kotlin counterpart that a handler raises (the Kotlin library answers `INVALID_REQUEST` itself), so they map to
  `HANDLER_ERROR` with JSON details and to `INVALID_REQUEST`. Every line break in a message becomes one space (the conformance contract's rule for `L1-HANDLER-ERRORS`, not Kotlin's); details
  are set only with `HANDLER_ERROR`; a failure of a call the handler made is its own `HANDLER_ERROR`.
- **Question:** settled: follows the Kotlin library for the texts and the exception mapping; the line-break rule comes
  from the conformance contract.

### SG-2: the forms of a hub address

- **Specification:** `protocol.md` and `writing-a-library.md` name the hubs a library connects to, `host:port` targets
  and one DNS name whose addresses are the hubs ([Several hubs](https://snodra.damoreira.com/library-authors/protocol/)),
  but no syntax for the configured addresses.
- **Kotlin library (the reference):** `host:port` or `static://host:port`, a comma list of them (blank entries and
  repeats dropped, order kept), or exactly one `dns:///name:port` with a port from 1 to 65535
  (`snodra-messaging@ecc4207:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/hub/HubTargets.kt:12-50`).
  Any other prefix is not a form it knows: the entry goes to gRPC as a target, which cannot reach a hub with it.
- **Other libraries:** the Go and Rust libraries also take `http://host:port` and `https://host:port`, `https://`
  meaning TLS to that hub with the system's roots when no TLS is configured.
- **This library:** `snodra/_settings.py` (`hub_targets`, run when `Settings` is built) takes Kotlin's forms and refuses
  every other `scheme://` entry, `http://` and `https://` included, with a `ConfigurationError` that names
  `SNODRA_SERVER_ADDRESS` and the accepted forms. TLS is a setting of its own, for every hub.
- **Question:** settled: follows the Kotlin library; an address Kotlin cannot connect to is refused at startup.

### SG-3: the texts of a request that was never sent

- **Specification:** `protocol.md` ("Several hubs") quotes the beginnings of the texts, "no hub connected within ..."
  and "no hub took the request within ...", and the whole `'<type>' not written: 9 hub streams lost before it was`. It
  does not say how the duration is written.
- **Kotlin library (the reference):** `no hub connected within <wait>` when no hub was tried, `no hub took the request
  within <wait>` when one was (`snodra-messaging@ecc4207:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/hub/HubConnections.kt:375-377`),
  and `'<type>' not written: <n> hub streams lost before it was` (`.../hub/HubConnections.kt:395`). `<wait>` is a
  `java.time.Duration` in its ISO-8601 form, for example `PT5S`.
- **Other libraries:** the Go library writes `no hub took '<type>' within <duration>` in Go's form (`5s`).
- **This library:** `snodra/_send.py` (`Sender._route`) uses Kotlin's texts with the duration in milliseconds:
  `no hub connected within 300 ms`, `no hub took the request within 300 ms`, and
  `'orders.PlaceOrder' not written: 9 hub streams lost before it was`. A call made once the shutdown drained the
  handlers fails at once with `'<type>' not sent: the app is shutting down`.
- **Question:** settled: follows the Kotlin library, the duration written in milliseconds as Python has no standard
  text form of a duration.

### SG-7: JSON details of the default payload codec

- **Specification:** `messages.md` (Payloads) fixes camelCase names, UTC ISO 8601 timestamps, string ids, unknown fields
  ignored and `{}` for a command without a result. It is silent on numbers, `null` against an absent field, enum and
  date-time forms, big integers and bytes.
- **Kotlin library (the reference):** Jackson with the Kotlin module (`snodra-messaging@ecc4207:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/codec/JsonCodec.kt:11-31`):
  a `null` field is written as `null`; the JSON literal `null` as a whole payload fails, an empty payload decodes to
  `Unit`; an enum is its name; an `Instant` is written with 0, 3, 6 or 9 fraction digits and always `Z`; a byte array is
  standard base64; with Jackson's default coercions it also reads `1.0`, `1.5` (truncated to 1) and the text `"1"` into an integer field,
  a number or a boolean into a string field, `1` and `"true"` into a boolean field, and `"1.5"` into a double. A field
  typed as nullable and absent reads as `null`.
- **Other libraries:** not compared.
- **This library:** `snodra/_codec.py`. It writes `null` fields, fails on the whole-payload `null`, and reads an empty
  payload as no value only into `None`. A `datetime` is written in UTC with 0, 3 or 6 fraction digits (a
  naive `datetime` is refused) and read with or without up to 9 fraction digits (the digits beyond microseconds are
  dropped) and a `Z` or an offset, never without a zone. `bytes` is standard base64. It is stricter than Kotlin on
  coercions: an integer field takes only a JSON integer (never `1.0`, `1.5`, `"1"` or `true`), a float field an integer or
  a float (never a string), a string field only a string, a boolean field only a boolean. An absent field of an optional
  type (`X | None`) reads as `None` as the Kotlin reader does; any other absent field without a default fails. A number
  too large for a float (`1e999`, a 400-digit integer) is refused on read, `NaN` and `Infinity` on read and write. An
  enum is written by its member value: it is readable by a Kotlin service only when the value equals the name of its enum
  constant. A `set` is written sorted (or, when its elements do not sort, by their JSON text) so equal sets give equal
  bytes. One trailing underscore of a field name is dropped (`type_` is `type`). Python integers have no range, so a value beyond signed
  64 bits is written as it is.
- **Question:** open: should the numeric, textual and boolean coercions the Kotlin reader accepts (see above) be read too?
