kopia repository create filesystem \
    --log-level debug \
    --config-file ${KOPIA_STRESS_REPO_CONFIG} \
    --persist-credentials
    --no-use-keychain \
    --password=${KOPIA_STRESS_REPO_PASSWORD} \
    --path ${KOPIA_STRESS_REPO_FS_PATH} \
