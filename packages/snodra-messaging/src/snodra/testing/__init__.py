"""The conformance kits of the Snodra libraries.

Every store add-on runs the kit of its port, a third party's included: the kit needs only its prepared source and a
way to make the add-on, and uses only the standard library. ``snodra.testing`` imports no test framework.

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-conformance-kit.
"""

from snodra.testing.checks import (
    EVENT_STORE_CHECKS,
    Check,
    a_joining_instance_announces_its_first_assignment,
    an_instance_left_without_segments_closes_its_change,
    every_processor_gets_every_event,
    headers_round_trip,
    instances_share_a_processor,
    paused_subscription_receives_nothing_and_resumes_in_place,
    redelivered_after_commit_is_skipped,
    same_key_arrives_in_publish_order,
    unacked_delivery_is_redelivered,
)
from snodra.testing.eventstore import (
    EVENT_TYPE,
    JOIN_WINDOW,
    QUIET,
    SLICE,
    WAIT,
    EventStoreKit,
    MarkerStore,
    ProcessedEvents,
    Reader,
    bounded,
    run_event_store_conformance,
)

__all__ = [
    "EVENT_STORE_CHECKS",
    "EVENT_TYPE",
    "JOIN_WINDOW",
    "QUIET",
    "SLICE",
    "WAIT",
    "Check",
    "EventStoreKit",
    "MarkerStore",
    "ProcessedEvents",
    "Reader",
    "a_joining_instance_announces_its_first_assignment",
    "an_instance_left_without_segments_closes_its_change",
    "bounded",
    "every_processor_gets_every_event",
    "headers_round_trip",
    "instances_share_a_processor",
    "paused_subscription_receives_nothing_and_resumes_in_place",
    "redelivered_after_commit_is_skipped",
    "run_event_store_conformance",
    "same_key_arrives_in_publish_order",
    "unacked_delivery_is_redelivered",
]
