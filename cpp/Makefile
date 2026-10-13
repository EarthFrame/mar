# C++ Reference Implementation Makefile
# Delegates to the root build orchestrator for unified dependency tracking.

.PHONY: all cpp test clean

all: cpp

cpp:
	@$(MAKE) -C .. cpp

test:
	@$(MAKE) -C .. test-cpp

clean:
	@$(MAKE) -C .. clean
