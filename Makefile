test:
	python3 -m unittest tests/test_*.py

publish:
	python3 setup.py sdist bdist_wheel
	twine upload dist/*
