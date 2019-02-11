clean:
	rm -fr .tox/ dist/ VERSION

prepare:
	pip install -r requirements.txt -r test_requirements.txt

build:
	python setup.py git_version sdist

tests: checkstyle_tests unit_tests

checkstyle_tests:
	python -m pylint -j 0 -f parseable neptune tests

unit_tests:
	tox
