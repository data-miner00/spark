ifeq ($(OS), Windows_NT)
activate:
	pipenv shell

install: Pipfile
	pipenv install

clean:
	if exist "./build" rd /s /q build

generate:
	pipenv lock -r --dev

notebook:
	jupyter notebook
endif
