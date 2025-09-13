all: lint mypy test

lint:
	@pylint -r y -j 0 meshbot.py src/

mypy:
	@mypy meshbot.py

test:
	@pytest --cov src/


