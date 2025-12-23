.PHONY: install lint test backfill daily features qc dashboard

install:
	poetry install

lint:
	poetry run ruff check .

test:
	poetry run pytest

backfill:
	poetry run eicflows backfill --start $(START) --end $(END) --metric physical_flow

daily:
	poetry run eicflows daily --days $(DAYS)

features:
	poetry run eicflows features --start $(START) --end $(END)

qc:
	poetry run eicflows qc --start $(START) --end $(END)

dashboard:
	poetry run streamlit run dashboard/app.py
