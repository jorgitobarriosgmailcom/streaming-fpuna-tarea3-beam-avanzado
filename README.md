# Tarea 3 — Estado, duplicados e idempotencia con Apache Beam

Solución de la Tarea 3 de **Streaming de datos y sus aplicaciones**. El proyecto conserva la estructura del repositorio base y completa `notebook.py` para producir totales de pagos confirmados por comercio y minuto usando tiempo de evento, ventanas, estado por clave, timers, triggers y una salida idempotente.

## Objetivo implementado

El pipeline aplica las siguientes reglas:

- usa `event_time` como timestamp del dominio;
- utiliza ventanas fijas de **60 segundos** con intervalo `[inicio, fin)`;
- acepta hasta **120 segundos** de atraso;
- descarta eventos cuyo `status` sea distinto de `CONFIRMED`;
- deduplica `event_id` **dentro de cada `merchant_id`**;
- elimina el estado de deduplicación con un timer de event time al finalizar la ventana más el `allowed_lateness`;
- configura panes **EARLY → ON_TIME → LATE** en modo `ACCUMULATING`;
- conserva metadatos de ventana y pane en el pipeline integrado;
- usa la clave idempotente `merchant_id|window_start` para materializar revisiones mediante UPSERT.

Estas reglas corresponden al contrato del proyecto base: tiempo de evento, lateness de 120 s, filtro `CONFIRMED`, deduplicación por comercio y clave idempotente de salida.

## Archivos relevantes

```text
.
├── notebook.py
├── data/
│   └── payments.jsonl
├── tests/
│   ├── conftest.py
│   ├── test_assignment.py
│   └── test_temporal_stream.py
├── evidence/
├── .github/workflows/ci.yml
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── uv.lock
```

`data/payments.jsonl` se mantiene **sin modificaciones**.

## Decisiones de diseño

### 1. Tiempo de evento

`parse_utc()` convierte los timestamps ISO-8601 a `datetime` timezone-aware. El timestamp asignado a Beam es `event_time`, no `arrival_time`. Esto hace que un evento fuera de orden conserve el minuto en el que realmente ocurrió y evita que un replay cambie de ventana solo por ejecutarse más tarde.

### 2. Ventanas

`assign_fixed_window()` y los pipelines usan ventanas fijas de 60 segundos alineadas al epoch. Para un evento a `13:00:42Z`, la ventana es:

```text
[13:00:00Z, 13:01:00Z)
```

La decisión responde al objetivo de totalizar pagos por comercio y minuto. Una ventana deslizante multiplicaría estado y cómputo sin aportar valor a esta métrica.

### 3. Atraso y revisiones

El oráculo determinista `summarize_payments()` calcula:

```text
delay_seconds = arrival_time - event_time
```

Con la configuración por defecto, un evento con atraso superior a 120 s se audita como `too_late` y no modifica el total. Un evento aceptado que llega después del final de su ventana se marca como `revision=True`.

En el dataset base, `p-007` tiene 169 s de atraso: con 120 s queda fuera de tolerancia; con 180 s se acepta y produce una revisión.

### 4. Estado y deduplicación

`DeduplicatePayments` recibe elementos con clave `merchant_id`. `SetStateSpec` conserva los `event_id` ya observados **por clave y ventana**, por lo que dos comercios pueden usar el mismo `event_id` sin interferirse.

El timer `EXPIRY`, basado en `TimeDomain.WATERMARK`, se programa para:

```text
window.end + allowed_lateness
```

Cuando dispara, limpia `seen_ids`. Sin esta expiración, el estado crecería indefinidamente en un flujo no acotado.

### 5. Triggers y panes

`build_trigger_policy()` configura:

- **EARLY:** estimaciones cada 30 s de processing time;
- **ON_TIME:** cuando el watermark pasa el final de la ventana;
- **LATE:** corrección cuando aparece un evento tardío aceptado;
- **ACCUMULATING:** cada pane contiene el total acumulado conocido.

La prueba adicional `test_temporal_stream.py` utiliza `TestStream`: hace avanzar el watermark más allá del cierre de la ventana y luego introduce un evento cuyo `event_time` pertenece a esa ventana. El resultado acumulado esperado confirma que el evento tardío se acepta dentro del horizonte configurado.

### 6. Idempotencia del sink

La identidad lógica de una salida es:

```text
merchant_id|window_start
```

`simulate_sink_retries()` contrasta dos contratos:

- `POST` append-only: cada reintento crea otra fila;
- `UPSERT` idempotente: todos los reintentos de la misma salida convergen en una sola entidad materializada.

La auditoría conserva todos los intentos, aunque el estado visible del sink sea único.

## Resultado esperado con el dataset base

Con `window_seconds=60`, `allowed_lateness_seconds=120` y deduplicación activa:

- eventos de entrada: **9**;
- pagos `CONFIRMED` únicos aceptados: **5**;
- duplicados descartados: **1** (`p-002`);
- estados no confirmados descartados: **2** (`p-003`, `p-008`);
- eventos fuera de lateness: **1** (`p-007`);
- totales materializados: **4**.

Totales esperados:

| Comercio | Ventana UTC | Total |
|---|---|---:|
| `m-azul` | `[13:00, 13:01)` | 170000 |
| `m-verde` | `[13:00, 13:01)` | 80000 |
| `m-verde` | `[13:01, 13:02)` | 90000 |
| `m-azul` | `[13:02, 13:03)` | 200000 |

## Ejecutar con Docker

Requisito: Docker Desktop o Docker Engine con Compose.

```bash
docker compose up --build notebook
```

Abrir:

```text
http://localhost:2718
```

En otra terminal, ejecutar la suite completa:

```bash
docker compose exec notebook uv run pytest -q
docker compose exec notebook uv run ruff check notebook.py
docker compose exec notebook uv run marimo check --strict notebook.py
```

El editor usa `--no-token` únicamente para `localhost`; no debe exponerse directamente a una red pública.

## Ejecutar con uv

Requiere Python 3.12 y `uv`.

```bash
uv sync --frozen
uv run pytest -q
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
uv run marimo edit notebook.py
```

## Evidencia de ejecución

Antes de entregar, generar o actualizar la evidencia con:

```bash
uv run pytest -q | tee evidence/pytest.txt
uv run ruff check notebook.py | tee evidence/ruff.txt
uv run marimo check --strict notebook.py | tee evidence/marimo.txt
```

También puede hacerse dentro del contenedor reemplazando `uv run ...` por:

```bash
docker compose exec notebook uv run pytest -q
```

El repositorio incorpora GitHub Actions (`.github/workflows/ci.yml`), de modo que al publicarlo cada `push` vuelve a ejecutar pruebas, Ruff y Marimo. Para la entrega final conviene que la pestaña **Actions** muestre la ejecución en verde.

## Pruebas cubiertas

La suite provista verifica, entre otros casos:

- parsing timezone-aware;
- asignación de ventana por `event_time`;
- deduplicación sin modificar el total;
- aislamiento de estado entre comercios;
- evento fuera de orden en su ventana real;
- revisión tardía dentro de tolerancia;
- auditoría de un evento demasiado tardío;
- totales de Beam por ventana;
- `allowed_lateness=120` y panes acumulativos;
- convergencia de reintentos con UPSERT;
- contraste con sink append-only;
- limpieza de estado por timer.

Se agrega además una prueba con `TestStream` para evidenciar una revisión tardía aceptada.

## Trade-offs

La política busca equilibrio entre corrección, latencia y costo. Un `allowed_lateness` mayor aceptaría más eventos tardíos, pero retendría estado durante más tiempo. Un horizonte menor liberaría memoria antes, a costa de perder correcciones válidas. Los panes tempranos reducen la latencia percibida, pero generan más resultados provisionales y escrituras. La acumulación facilita el contrato de UPSERT porque cada nueva versión reemplaza el total anterior; a cambio, el runner conserva estado suficiente para reconstruir el acumulado.

La deduplicación estatal y la salida idempotente resuelven problemas diferentes: la primera evita contar dos veces un mismo evento lógico; la segunda evita materializar dos veces un mismo resultado lógico durante reintentos del sink.
