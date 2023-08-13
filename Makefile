ifeq ($(OS), Windows_NT)
activate:
	pipenv shell

install: Pipfile
	pipenv install

clean:
	if exist "./build" rd /s /q build

generate:
	pipenv lock -r --dev > requirements.txt

notebook:
	jupyter notebook

format:
	black .

test:
	python -m pytest tests/

quick:
	python -m pytest tests/ -m "not slow"

current:
	python -m pytest tests/ -m current
endif
