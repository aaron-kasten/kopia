export KOPIA_STRESS_REPO_S3_BUCKET="aaron-kopia-stress"
export KOPIA_STRESS_REPO_CONFIG="$HOME/stress_s3/kopia_s3.confg"

pushd $HOME/devel/kopia/tests/stress_test
	go test -timeout 90m -v -bench '^\QBenchmarkBlockManager\E$' -run '^$' $HOME/devel/kopia/tests/stress_test -args \
		-stress_test.verbose=true \
		-stress_test.rootdir=$HOME/stress_s3 \
		-stress_test.configfile=$KOPIA_STRESS_REPO_CONFIG \
		-stress_test.replacement=3 \
		-stress_test.createrepo=true \
		-stress_test.repoformat=s3 \
		-stress_test.repobucket=$KOPIA_STRESS_REPO_S3_BUCKET \
		-stress_test.n=1 \
		-stress_test.n0=1000 \
		-stress_test.n1=100 \
		-stress_test.fsize0=131072 \
		-stress_test.seed=12931284 \
		-stress_test.label=Benchmark
popd

