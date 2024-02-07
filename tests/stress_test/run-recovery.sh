 7 # Positional arguments:
  8 #
  9 # 1. kopia_recovery_dir
 10 # 2. kopia_exe_dir
 11 # 3. test_duration
 12 # 4. test_timeout
 13 # 5. test_repo_path_prefix
 14
# Environment variables that modify the behavior of the recovery job execution
 16 #
 17 # - AWS_ACCESS_KEY_ID: To access the repo bucket
 18 # - AWS_SECRET_ACCESS_KEY: To access the repo bucket
 19 # - FIO_EXE: Path to the fio executable, if unset a Docker container will be
 20 #       used to run fio.
 21 # - HOST_FIO_DATA_PATH:
 22 # - LOCAL_FIO_DATA_PATH: Path to the local directory where snapshots should be
 23 #       restored to and fio data should be written to
 24 # - S3_BUCKET_NAME: Name of the S3 bucket for the repo

export FIO_EXE=/usr/local/bin/fio
./tools/recovery-job.sh
