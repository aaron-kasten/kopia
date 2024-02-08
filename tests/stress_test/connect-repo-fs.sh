kopia repository connect filesystem \
	--log-level=debug \
	--config-file ${KOPIA_STRESS_REPO_CONFIG} \
	--path ${KOPIA_STRESS_REPO_FS_PATH} \
	--no-use-keychain \
	--password=${KOPIA_STRESS_REPO_PASSWORD} \
	--persist-credentials

