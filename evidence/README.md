# Evidencia

- `expected_results.json`: resultado determinista esperado del dataset base.
- `contract_check.txt`: verificación local de las funciones puras y del contrato idempotente.
- `pytest.txt`, `ruff.txt` y `marimo.txt`: se generan con `make evidence` en un entorno con Python 3.12 y las dependencias sincronizadas.

Para la entrega final, ejecutar:

```bash
make evidence
```

o los equivalentes dentro del contenedor Docker. La ejecución de GitHub Actions sirve además como evidencia pública de la suite completa.
