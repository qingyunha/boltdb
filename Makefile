test:
	python3 -m unittest tests/test_*.py

cover:
	coverage run -m unittest tests/test_*.py
	coverage report --omit 'tests/*.py'

lint:
	flake8 boltdb/ tests/

publish:
	python3 setup.py sdist bdist_wheel
	twine upload dist/*
	rm -rf boltdb.egg-info/ build/ dist/
