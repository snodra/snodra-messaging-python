"""OpenTelemetry instrumentation for the Snodra messaging suite."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Final

from opentelemetry import metrics, propagate, trace
from opentelemetry.metrics import CallbackOptions, MeterProvider, Observation
from opentelemetry.trace import SpanKind, TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from snodra import Gauges, Handling, NoTelemetry

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from opentelemetry.context.context import Context
    from opentelemetry.propagators.textmap import TextMapPropagator

SCOPE_NAME: Final = "snodra"

# TODO(spec-gap): SG-6
_REQUEST_DURATION: Final = "snodra.hub.client.request.duration"
_HANDLERS_IN_FLIGHT: Final = "snodra.hub.client.handlers.in.flight"
_CONNECTED_HUBS: Final = "snodra.hub.client.connected.hubs"
_TOKEN_FETCHES: Final = "snodra.hub.client.token.fetches"
_CERTIFICATE_ROTATIONS: Final = "snodra.hub.client.certificate.rotations"
_CREDENTIALS_REFRESHES: Final = "snodra.hub.client.credentials.refreshes.sent"

_DURATION_BOUNDARIES: Final = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)

_TOKEN_RESULTS: Final = ("ok", "http_error", "oauth_error", "io_error", "bad_response")


class Observability(NoTelemetry):
    """The :class:`snodra.Telemetry` on OpenTelemetry: metrics and traces with the standard Snodra instrument names."""

    def __init__(
        self,
        *,
        meter_provider: MeterProvider | None = None,
        tracer_provider: TracerProvider | None = None,
        propagator: TextMapPropagator | None = None,
    ) -> None:
        mp = meter_provider or metrics.get_meter_provider()
        tp = tracer_provider or trace.get_tracer_provider()
        self._meter = mp.get_meter(SCOPE_NAME)
        self._tracer = tp.get_tracer(SCOPE_NAME)
        self._propagator = propagator

        self._duration = self._meter.create_histogram(
            _REQUEST_DURATION,
            unit="s",
            description="Commands and queries sent through the hub, from the call to its result (retries included), "
            "per kind, type and outcome",
            explicit_bucket_boundaries_advisory=_DURATION_BOUNDARIES,
        )
        self._tokens = self._meter.create_counter(
            _TOKEN_FETCHES,
            description="Access token fetches by result",
        )
        self._rotations = self._meter.create_counter(
            _CERTIFICATE_ROTATIONS,
            description="Client certificate pairs adopted from the files",
        )
        self._refreshes = self._meter.create_counter(
            _CREDENTIALS_REFRESHES,
            description="ClientMessage.credentials written to the hubs",
        )

        for result in _TOKEN_RESULTS:
            self._tokens.add(0, {"result": result})
        self._rotations.add(0)
        self._refreshes.add(0)

        self._read: Callable[[], Gauges] | None = None

    def _resolved_propagator(self) -> TextMapPropagator:
        if self._propagator is not None:
            return self._propagator
        p = propagate.get_global_textmap()
        if p.fields:
            return p
        return TraceContextTextMapPropagator()

    def request_done(self, kind: str, type_: str, outcome: str, seconds: float) -> None:
        self._duration.record(
            seconds,
            attributes={"kind": kind, "type": type_, "outcome": outcome},
        )

    def token_fetched(self, result: str) -> None:
        self._tokens.add(1, attributes={"result": result})

    def certificate_rotated(self) -> None:
        self._rotations.add(1)

    def credentials_refresh_sent(self) -> None:
        self._refreshes.add(1)

    def observe_gauges(self, read: Callable[[], Gauges]) -> None:
        self._read = read

        def observe_handlers(options: CallbackOptions) -> Iterable[Observation]:
            if self._read is not None:
                try:
                    yield Observation(self._read().handlers_in_flight)
                except Exception:
                    yield Observation(0)

        def observe_hubs(options: CallbackOptions) -> Iterable[Observation]:
            if self._read is not None:
                try:
                    yield Observation(self._read().connected_hubs)
                except Exception:
                    yield Observation(0)

        self._meter.create_observable_gauge(
            _HANDLERS_IN_FLIGHT,
            callbacks=[observe_handlers],
            description="Incoming hub requests being handled or waiting for a handler permit",
        )
        self._meter.create_observable_gauge(
            _CONNECTED_HUBS,
            callbacks=[observe_hubs],
            description="Hubs with an acknowledged stream",
        )

    @contextmanager
    def handling(self, h: Handling) -> Iterator[None]:
        parent_ctx: Context | None = None
        if h.traceparent is not None:
            carrier = {"traceparent": h.traceparent}
            if h.tracestate is not None:
                carrier["tracestate"] = h.tracestate
            parent_ctx = self._resolved_propagator().extract(carrier=carrier)
        with self._tracer.start_as_current_span(
            h.type,
            context=parent_ctx,
            kind=SpanKind.SERVER,
            record_exception=True,
            set_status_on_exception=True,
        ):
            yield

    def inject(self, headers: dict[str, str]) -> None:
        self._resolved_propagator().inject(headers)


__all__ = ["SCOPE_NAME", "Observability"]
