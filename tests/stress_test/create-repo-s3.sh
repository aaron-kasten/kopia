set -x
#
aws s3 mb s3://${KOPIA_STRESS_REPO_S3_BUCKET}/
#
kopia repository create s3 \
    --log-level debug \
    --config-file ${KOPIA_STRESS_REPO_S3_CONFIG} \
    --persist-credentials \
    --password ${KOPIA_STRESS_REPO_PASSWORD} \
    --bucket ${KOPIA_STRESS_REPO_S3_BUCKET}
#
kopia repository validate-provider \
	--config-file "${KOPIA_STRESS_REPO_S3_CONFIG}"
#
set +x
