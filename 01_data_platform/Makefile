install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

lint:
	pylint --disable=R,C *.py

test:
	python -m pytest -vv --cov=clockwise test_*.py

format:
	black *.py clockwise/*.py

deploy:
	echo "deploy goes here"

all: install lint test format deploy
