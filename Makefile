clean:
	rm -fr .tox/ dist/ VERSION

prepare:
	pip3 install requirements/test_requirements.txt

build:
	python3 setup.py sdist

tests: checkstyle_tests unit_tests

checkstyle_tests:
	python3 -m pylint -j 0 -f parseable neptune tests

unit_tests:
	tox
