install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

format:
	black census_averages/*.py

lint:
	pylint --disable=R,C census_averages/main.py

all: install format lint
