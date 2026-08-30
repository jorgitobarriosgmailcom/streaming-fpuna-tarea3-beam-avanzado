.PHONY: install edit run test check verify evidence docker-run

install:
	uv sync --frozen

edit:
	uv run marimo edit notebook.py

run:
	uv run marimo run notebook.py

test:
	uv run pytest -q

check:
	uv run ruff check notebook.py
	uv run marimo check --strict notebook.py

verify: test check

evidence:
	mkdir -p evidence
	uv run pytest -q | tee evidence/pytest.txt
	uv run ruff check notebook.py | tee evidence/ruff.txt
	uv run marimo check --strict notebook.py | tee evidence/marimo.txt

docker-run:
	docker compose up --build notebook
