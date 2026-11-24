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
  `HANDLER_ERROR` with JSON details and to `INVALID_REQUEST`. Every line break in a message becomes one space; details
  are set only with `HANDLER_ERROR`; a failure of a call the handler made is its own `HANDLER_ERROR`.
- **Question:** settled: follows the Kotlin library.

### SG-7: JSON details of the default payload codec

- **Specification:** `messages.md` (Payloads) fixes camelCase names, UTC ISO 8601 timestamps, string ids, unknown fields
  ignored and `{}` for a command without a result. It is silent on numbers, `null` against an absent field, enum and
  date-time forms, big integers and bytes.
- **Kotlin library (the reference):** Jackson with the Kotlin module (`snodra-messaging@ecc4207:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/codec/JsonCodec.kt:11-31`):
  a `null` field is written as `null`; the JSON literal `null` as a whole payload fails, an empty payload decodes to
  `Unit`; an enum is its name; an `Instant` is written with 0, 3, 6 or 9 fraction digits and always `Z`; a byte array is
  standard base64; with Jackson's default coercions it also reads `1.0` or the text `"1"` into an integer field.
- **Other libraries:** not compared.
- **This library:** `snodra/_codec.py`. It writes `null` fields, fails on the whole-payload `null`, and reads an empty
  payload as no value only into `None`. An enum is its value (a Python enum's members carry values, not names; the names
  of Kotlin enums equal the values used on the wire). A `datetime` is written in UTC with 0, 3 or 6 fraction digits (a
  naive `datetime` is refused) and read with or without up to 9 fraction digits (the digits beyond microseconds are
  dropped) and a `Z` or an offset, never without a zone. `bytes` is standard base64. It is stricter than Kotlin on
  numbers: an integer field takes only a JSON integer (never `1.0`, `"1"` or `true`), a float field an integer or a float
  (never a string). `NaN` and `Infinity` are refused both ways. Python integers have no range, so a value beyond signed
  64 bits is written as it is.
- **Question:** settled: follows the Kotlin library, except the stricter numbers, which the owner may relax.
