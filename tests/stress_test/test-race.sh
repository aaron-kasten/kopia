export GORACE="log_path=/tmp/race/report strip_path_prefix=/Users/aaron.alpar/devel/kopia"
make -j2 test UNIT_TEST_RACE_FLAGS=-race UNIT_TESTS_TIMEOUT=1200s
