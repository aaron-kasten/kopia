export KOPIA_STRESS_REPO_S3_BUCKET="aaron-kopia-stress"
export KOPIA_STRESS_REPO_CONFIG="$HOME/stress_s3/kopia_s3.confg"

pushd $HOME/devel/kopia/tests/stress_test
	kopia repository connect s3 --bucket=$KOPIA_STRESS_REPO_S3_BUCKET --config-file=$KOPIA_STRESS_REPO_CONFIG --password="password"
popd

