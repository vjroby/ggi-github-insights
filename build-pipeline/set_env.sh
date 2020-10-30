export GCP_PROJECT_ID=$(gcloud config list --format 'value(core.project)')
export PROJECT_NUMBER=$(gcloud projects describe "${GCP_PROJECT_ID}" --format='get(projectNumber)')
export PROJECT_REGION='us-central1'

export ARTIFACTS_BUCKET="ggi_artifacts"
export DATA_INPUT_BUCKET="data_gharchive_org_20203010"
export DATAFLOW_STAGING_BUCKET_TEST="${GCP_PROJECT_ID}-dataflow-staging-${TEST}"

export COMPOSER_ZONE_ID='us-central1-a'
export COMPOSER_ENV_NAME='data-pipeline-composer'