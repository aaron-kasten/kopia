kopia repository connect s3 \
	--log-level=debug \
	--bucket ${KOPIA_STRESS_REPO_S3_BUCKET} \
	--no-use-keychain \
	--password=${KOPIA_STRESS_REPO_PASSWORD} \
	--persist-credentials

