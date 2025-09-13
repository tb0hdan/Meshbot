all: lint test

lint:
	@pylint -r y -j 0 meshbot.py src/

test:
	@pytest --cov src/


