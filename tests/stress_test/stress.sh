
mkdir -p "$HOME/stress"

pushd $HOME/devel/kopia/tests/stress_test

	rm -rf "$HOME/stress/*"

	# export KOPIA_DEBUG_PPROF='heap=rate=100:cpu:block=rate=100'
	export AWS_SECRET_ACCESS_KEY='***REMOVED***'
	export AWS_ACCESS_KEY_ID='***REMOVED***'

	go test -timeout 30m -v -bench '^\QBenchmarkBlockManager\E$' -run '^$' /Users/aaron.alpar/devel/kopia/tests/stress_test -args \
		-stress_test.rootdir=$HOME/stress \
		-stress_test.replacement=2 \
		-stress_test.createrepo=true \
		-stress_test.repoformat=s3 \
		-stress_test.repobucket=aaron-kopia-repo-loadtest \
		-stress_test.n=2 \
		-stress_test.n0=1000 \
		-stress_test.n1=100 \
		-stress_test.fsize0=4096 \
		-stress_test.seed=12931284 \
		-stress_test.label=Benchmark
popd
