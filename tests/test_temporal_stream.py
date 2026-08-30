"""Prueba adicional con TestStream para documentar una revisión tardía aceptada."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.utils.timestamp import Timestamp


def _contains_accumulated_late_value(values):
    observed = list(values)
    assert 30 in observed, f"se esperaba una revisión acumulada de 30; observado={observed}"


def test_teststream_accepts_late_event_inside_allowed_lateness(solution):
    start = Timestamp.from_rfc3339("2026-07-24T13:00:00Z")
    first = Timestamp.from_rfc3339("2026-07-24T13:00:05Z")
    late = Timestamp.from_rfc3339("2026-07-24T13:00:42Z")
    after_window = Timestamp.from_rfc3339("2026-07-24T13:01:01Z")

    stream = (
        TestStream()
        .advance_watermark_to(start)
        .add_elements([beam.window.TimestampedValue(("m-a", 10), first)])
        .advance_watermark_to(after_window)
        .add_elements([beam.window.TimestampedValue(("m-a", 20), late)])
        .advance_watermark_to_infinity()
    )

    options = PipelineOptions(streaming=True)
    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | stream
            | solution.build_trigger_policy(
                window_seconds=60,
                allowed_lateness_seconds=120,
            )
            | beam.CombinePerKey(sum)
            | beam.Map(lambda item: item[1])
        )
        assert_that(output, _contains_accumulated_late_value)
