#!/bin/bash
set -e

echo "<<< Running e2e client tests in $(pwd) >>>"

export WORKSPACE_NAME="administrator"
export ADMIN_USERNAME="administrator"
export ADMIN_NEPTUNE_API_TOKEN="TODO token"

# Set defaults
export SERVICE_ACCOUNT_NAME="unused"
export BUCKET_NAME='unused'
export NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE='TRUE'
export NEPTUNE_RETRIES_TIMEOUT=600

export USER_USERNAME=$ADMIN_USERNAME
export NEPTUNE_API_TOKEN=$ADMIN_NEPTUNE_API_TOKEN

echo "<<< Upgrade pip3 >>>"
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -U pip

echo "<<< Clone neptune-client repo with e2e tests >>>"
git clone -b onprem22-integration-test https://github.com/neptune-ai/neptune-client.git

echo "<<< Install pip requirements >>>"
python3 -m pip install --upgrade "./neptune-client[dev,e2e]"


echo "<<< Run tests >>>"
pytest ./neptune-client/tests/e2e \
  -m "not s3 and not integrations" \
  --junitxml test_results/junit-base.xml
