mkdir -p "$HOME/stress"
rm -rf "$HOME/stress/*"

#export KOPIA_STRESS_REPO_S3_BUCKET="aaron-kopia-stress"

pushd $HOME/devel/kopia/tests/stress_test
	# export KOPIA_DEBUG_PPROF='heap=rate=100:cpu:block=rate=100'

	go test -timeout 90m -v -trace kopia.btrace -count 1 -bench '^\QBenchmarkBlockManager\E$' -run '^$' $HOME/devel/kopia/tests/stress_test -args \
		-stress_test.verbose=false \
		-stress_test.rootdir=$HOME/stress_2 \
		-stress_test.replacement=2 \
		-stress_test.createrepo=false \
		-stress_test.repoformat=filesystem \
		-stress_test.n=500 \
		-stress_test.n0=1000 \
		-stress_test.n1=100 \
		-stress_test.fsize0=131072 \
		-stress_test.seed=129115284 \
		-stress_test.label=Benchmark
popd

