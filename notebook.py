import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook contiene la implementación completa solicitada para la Tarea 3,
    manteniendo los contratos del proyecto base y priorizando resultados
    deterministas, estado acotado e idempotencia observable.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 a ``datetime`` timezone-aware.

        El dataset usa el sufijo ``Z`` (UTC). También se aceptan offsets ISO-8601
        explícitos para que el contrato sea robusto ante datos equivalentes.
        """
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError("timestamp vacío o no textual")

        normalized = raw_value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"timestamp ISO-8601 inválido: {raw_value!r}") from exc

        if parsed.utcoffset() is None:
            raise ValueError(f"timestamp sin zona horaria: {raw_value!r}")
        return parsed

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites ``[inicio, fin)`` de una ventana fija.

        Las ventanas se alinean al epoch, de modo que con 60 segundos coinciden
        con minutos naturales en UTC.
        """
        if timestamp.utcoffset() is None:
            raise ValueError("timestamp debe ser timezone-aware")
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser mayor que cero")

        start_epoch = int(timestamp.timestamp() // size_seconds) * size_seconds
        start = datetime.fromtimestamp(start_epoch, tz=timestamp.tzinfo)
        end = datetime.fromtimestamp(start_epoch + size_seconds, tz=timestamp.tzinfo)
        return start, end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds no puede ser negativo")

        totals_by_window: dict[tuple[str, str, str], int] = {}
        seen_by_merchant: dict[str, set[str]] = {}
        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = str(event["event_id"])
            merchant_id = str(event["merchant_id"])
            event_time = parse_utc(str(event["event_time"]))
            arrival_time = parse_utc(str(event["arrival_time"]))
            window_start, window_end = assign_fixed_window(event_time, window_seconds)
            delay_seconds = (arrival_time - event_time).total_seconds()

            duplicate = False
            too_late = False
            accepted = False
            revision = False
            reason = "accepted"

            if event.get("status") != "CONFIRMED":
                reason = "not_confirmed"
            else:
                seen = seen_by_merchant.setdefault(merchant_id, set())
                duplicate = deduplicate and event_id in seen

                if duplicate:
                    reason = "duplicate"
                else:
                    if deduplicate:
                        seen.add(event_id)

                    too_late = delay_seconds > allowed_lateness_seconds
                    if too_late:
                        reason = "too_late"
                    else:
                        accepted = True
                        revision = arrival_time >= window_end
                        key = (
                            merchant_id,
                            window_start.isoformat(),
                            window_end.isoformat(),
                        )
                        totals_by_window[key] = totals_by_window.get(key, 0) + int(
                            event["amount"]
                        )

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (merchant_id, window_start, window_end), total in sorted(
                totals_by_window.items()
            )
        ]
        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para el dataset provisto y la configuración por defecto entran **9 eventos**:
    se aceptan **5 pagos CONFIRMED únicos dentro de 120 s**, se materializan
    **4 totales por comercio/minuto**, un duplicado se descarta, dos estados no
    confirmados se filtran y `p-007` queda auditado como `too_late`.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")

        def to_timestamped(event: dict[str, Any]):
            event_time = parse_utc(str(event["event_time"]))
            return beam.window.TimestampedValue(event, event_time.timestamp())

        def add_window_metadata(
            item: tuple[str, int],
            window=beam.DoFn.WindowParam,
        ) -> dict[str, Any]:
            merchant_id, total = item
            return {
                "merchant_id": merchant_id,
                "window_start": parse_utc(window.start.to_rfc3339()).isoformat(),
                "window_end": parse_utc(window.end.to_rfc3339()).isoformat(),
                "total": total,
            }

        return (
            pipeline
            | "CreatePayments" >> beam.Create(events)
            | "AssignEventTime" >> beam.Map(to_timestamped)
            | "ConfirmedOnly" >> beam.Filter(
                lambda event: event.get("status") == "CONFIRMED"
            )
            | "FixedMinuteWindows"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "KeyByMerchant"
            >> beam.Map(lambda event: (str(event["merchant_id"]), int(event["amount"])))
            | "SumByMerchant" >> beam.CombinePerKey(sum)
            | "AttachWindowMetadata" >> beam.Map(add_window_metadata)
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar ``event_id`` repetidos por comercio y ventana.

        El estado es por clave (``merchant_id``) y por ventana. Un timer en
        tiempo de evento evita que ``seen_ids`` crezca indefinidamente.
        """

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            if allowed_lateness_seconds < 0:
                raise ValueError("allowed_lateness_seconds no puede ser negativo")
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            merchant_id, event = element
            event_id = str(event["event_id"])

            if event_id in set(seen_ids.read()):
                return

            seen_ids.add(event_id)
            expiry.set(window.end + self.allowed_lateness_seconds)
            yield merchant_id, event

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds no puede ser negativo")

        return beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=beam.trigger.AfterWatermark(
                early=beam.trigger.Repeatedly(beam.trigger.AfterProcessingTime(30)),
                late=beam.trigger.AfterCount(1),
            ),
            accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=allowed_lateness_seconds,
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir ``merchant_id|window_start`` para un resultado lógico."""
        try:
            merchant_id = str(result["merchant_id"])
            window_start = str(result["window_start"])
        except KeyError as exc:
            raise ValueError(f"resultado sin campo requerido: {exc.args[0]}") from exc
        return f"{merchant_id}|{window_start}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts <= 0:
            raise ValueError("attempts debe ser mayor que cero")

        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        operation = "UPSERT" if idempotent else "POST"

        for result in results:
            idempotency_key = make_idempotency_key(result)
            materialized_row = {**result, "idempotency_key": idempotency_key}

            for attempt in range(1, attempts + 1):
                audit.append(
                    {
                        **materialized_row,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )
                if idempotent:
                    upsert_sink[idempotency_key] = materialized_row.copy()
                else:
                    append_sink.append(materialized_row.copy())

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(
    Any,
    DeduplicatePayments,
    beam,
    build_trigger_policy,
    make_idempotency_key,
    parse_utc,
):
    def build_advanced_payments_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Pipeline integrado: tiempo, filtro, trigger, estado, agregado y pane."""

        def to_timestamped(event: dict[str, Any]):
            event_time = parse_utc(str(event["event_time"]))
            return beam.window.TimestampedValue(event, event_time.timestamp())

        def to_result(
            item: tuple[str, int],
            window=beam.DoFn.WindowParam,
            pane=beam.DoFn.PaneInfoParam,
        ) -> dict[str, Any]:
            merchant_id, total = item
            window_start = parse_utc(window.start.to_rfc3339()).isoformat()
            window_end = parse_utc(window.end.to_rfc3339()).isoformat()
            timing_names = {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}
            result = {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
                "pane_timing": timing_names.get(int(pane.timing), "UNKNOWN"),
                "pane_index": pane.index,
            }
            return {**result, "idempotency_key": make_idempotency_key(result)}

        return (
            pipeline
            | "AdvancedCreate" >> beam.Create(events)
            | "AdvancedEventTime" >> beam.Map(to_timestamped)
            | "AdvancedConfirmed"
            >> beam.Filter(lambda event: event.get("status") == "CONFIRMED")
            | "AdvancedWindowPolicy"
            >> build_trigger_policy(
                window_seconds=window_seconds,
                allowed_lateness_seconds=allowed_lateness_seconds,
            )
            | "AdvancedKeyByMerchant"
            >> beam.Map(lambda event: (str(event["merchant_id"]), event))
            | "AdvancedDeduplicate"
            >> beam.ParDo(DeduplicatePayments(allowed_lateness_seconds))
            | "AdvancedAmount"
            >> beam.Map(lambda item: (item[0], int(item[1]["amount"])))
            | "AdvancedSum" >> beam.CombinePerKey(sum)
            | "AdvancedPaneMetadata" >> beam.Map(to_result)
        )

    return (build_advanced_payments_pipeline,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    En el proyecto base estas pruebas fallaban con `NotImplementedError`. En
    esta versión implementada deben quedar verdes las siguientes garantías:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
