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
