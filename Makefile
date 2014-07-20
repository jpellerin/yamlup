VPATH=$(PATH)
TEST_OPTS=

test: nose2
	nose2 $(TEST_OPTS)

deps:
	pip install -r requirements/base.txt

nose2:
	pip install -r requirements/test.txt

.PHONY: test deps
