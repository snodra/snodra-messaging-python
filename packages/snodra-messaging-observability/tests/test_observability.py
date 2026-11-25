"""Tests of the OpenTelemetry observability distribution."""

from __future__ import annotations

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    Gauge,
    Histogram,
    HistogramDataPoint,
    InMemoryMetricReader,
    NumberDataPoint,
    Sum,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from snodra import Gauges, Handling
from snodra_observability import SCOPE_NAME, Observability


def test_instrument_names_and_labels() -> None:
    reader = InMemoryMetricReader()
    mp = MeterProvider(metric_readers=[reader])
    obs = Observability(meter_provider=mp)

    obs.request_done("command", "orders.PlaceOrder", "ok", 0.03)
    obs.request_done("query", "orders.GetOrder", "timeout", 2.0)
    obs.token_fetched("ok")
    obs.token_fetched("ok")
    obs.token_fetched("oauth_error")
    obs.certificate_rotated()
    obs.credentials_refresh_sent()
    obs.credentials_refresh_sent()

    calls = 0

    def read_gauges() -> Gauges:
        nonlocal calls
        calls += 1
        return Gauges(handlers_in_flight=3, connected_hubs=2)

    obs.observe_gauges(read_gauges)

    data = reader.get_metrics_data()
    assert data is not None

    by_name = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            assert sm.scope.name == SCOPE_NAME
            for m in sm.metrics:
                by_name[m.name] = m

    # 1. Histogram
    h_metric = by_name["snodra.hub.client.request.duration"]
    assert h_metric.unit == "s"
    assert h_metric.description == (
        "Commands and queries sent through the hub, from the call to its result (retries included), "
        "per kind, type and outcome"
    )
    expected_bounds = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
    assert isinstance(h_metric.data, Histogram)
    data_points = list(h_metric.data.data_points)
    assert len(data_points) == 2
    for dp in data_points:
        assert isinstance(dp, HistogramDataPoint)
        assert tuple(dp.explicit_bounds) == expected_bounds
        assert dp.count == 1
        if dp.attributes == {"kind": "command", "type": "orders.PlaceOrder", "outcome": "ok"}:
            assert abs(dp.sum - 0.03) < 1e-6
        elif dp.attributes == {"kind": "query", "type": "orders.GetOrder", "outcome": "timeout"}:
            assert abs(dp.sum - 2.0) < 1e-6
        else:
            pytest.fail(f"unexpected attributes {dp.attributes}")

    # 2. Gauges
    handlers_gauge = by_name["snodra.hub.client.handlers.in.flight"]
    assert handlers_gauge.description == "Incoming hub requests being handled or waiting for a handler permit"
    assert handlers_gauge.unit == ""
    assert isinstance(handlers_gauge.data, Gauge)
    assert len(handlers_gauge.data.data_points) == 1
    dp_handlers = handlers_gauge.data.data_points[0]
    assert isinstance(dp_handlers, NumberDataPoint)
    assert dp_handlers.value == 3

    hubs_gauge = by_name["snodra.hub.client.connected.hubs"]
    assert hubs_gauge.description == "Hubs with an acknowledged stream"
    assert hubs_gauge.unit == ""
    assert isinstance(hubs_gauge.data, Gauge)
    assert len(hubs_gauge.data.data_points) == 1
    dp_hubs = hubs_gauge.data.data_points[0]
    assert isinstance(dp_hubs, NumberDataPoint)
    assert dp_hubs.value == 2

    # 3. Counters
    tokens_metric = by_name["snodra.hub.client.token.fetches"]
    assert tokens_metric.description == "Access token fetches by result"
    assert isinstance(tokens_metric.data, Sum)
    token_points: dict[str, int] = {}
    for pt in tokens_metric.data.data_points:
        assert isinstance(pt, NumberDataPoint)
        assert pt.attributes is not None
        assert isinstance(pt.value, int)
        token_points[str(pt.attributes["result"])] = pt.value
    assert token_points == {
        "ok": 2,
        "oauth_error": 1,
        "http_error": 0,
        "io_error": 0,
        "bad_response": 0,
    }

    rotations_metric = by_name["snodra.hub.client.certificate.rotations"]
    assert rotations_metric.description == "Client certificate pairs adopted from the files"
    assert isinstance(rotations_metric.data, Sum)
    assert len(rotations_metric.data.data_points) == 1
    dp_rotations = rotations_metric.data.data_points[0]
    assert isinstance(dp_rotations, NumberDataPoint)
    assert dp_rotations.value == 1

    refreshes_metric = by_name["snodra.hub.client.credentials.refreshes.sent"]
    assert refreshes_metric.description == "ClientMessage.credentials written to the hubs"
    assert isinstance(refreshes_metric.data, Sum)
    assert len(refreshes_metric.data.data_points) == 1
    dp_refreshes = refreshes_metric.data.data_points[0]
    assert isinstance(dp_refreshes, NumberDataPoint)
    assert dp_refreshes.value == 2


def test_the_handler_span_is_a_child_of_the_incoming_traceparent() -> None:
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    parent_id = "00f067aa0ba902b7"
    incoming_tp = f"00-{trace_id}-{parent_id}-01"

    exporter = InMemorySpanExporter()
    tp = TracerProvider()
    tp.add_span_processor(SimpleSpanProcessor(exporter))
    propagator = TraceContextTextMapPropagator()

    obs = Observability(
        tracer_provider=tp,
        meter_provider=MeterProvider(),
        propagator=propagator,
    )

    h = Handling(
        type="orders.PlaceOrder",
        kind="command",
        traceparent=incoming_tp,
        tracestate="v=1",
    )

    headers: dict[str, str] = {}
    with obs.handling(h):
        obs.inject(headers)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "orders.PlaceOrder"
    assert span.kind == SpanKind.SERVER
    assert format(span.context.trace_id, "032x") == trace_id
    assert span.parent is not None
    assert format(span.parent.span_id, "016x") == parent_id
    assert span.parent.is_remote is True
    child_span_id = format(span.context.span_id, "016x")
    assert headers["traceparent"] == f"00-{trace_id}-{child_span_id}-01"
    assert headers["tracestate"] == "v=1"


def test_a_failed_handling_marks_its_span() -> None:
    exporter = InMemorySpanExporter()
    tp = TracerProvider()
    tp.add_span_processor(SimpleSpanProcessor(exporter))

    obs = Observability(
        tracer_provider=tp,
        meter_provider=MeterProvider(),
    )

    h = Handling(
        type="orders.GetOrder",
        kind="query",
        traceparent=None,
        tracestate=None,
    )

    with pytest.raises(TimeoutError), obs.handling(h):
        raise TimeoutError("handler timed out")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "orders.GetOrder"
    assert span.status.status_code == StatusCode.ERROR
    assert "TimeoutError" in (span.status.description or "")
