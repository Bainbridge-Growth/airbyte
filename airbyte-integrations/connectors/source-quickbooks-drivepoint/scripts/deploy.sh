#!/bin/bash
# Deploy script for source-quickbooks-drivepoint from remote docker to gcloud VM

set -e

VM_INSTANCE_NAME=${2:-patch}
if not [[ "$VM_INSTANCE_NAME" ]]; then
  echo "Must provide VM instance name as a first argument."
  exit 1
fi

VERSION=${1}
if not [[ "$VERSION" ]]; then
  echo "Must provide version to deploy."
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONNECTOR_DIR="$(dirname "$SCRIPT_DIR")"
MANIFEST="$CONNECTOR_DIR/source_quickbooks_drivepoint/manifest.yaml"
PYPROJECT="$CONNECTOR_DIR/pyproject.toml"
METADATA="$CONNECTOR_DIR/metadata.yaml"

if [ ! -f "$MANIFEST" ]; then
  echo "manifest.yaml not found in $CONNECTOR_DIR. Exiting."
  exit 1
fi

# 1. Get current version from manifest.yaml (robust: match 'version:' at start of any line, any indent)
CUR_VERSION=$(grep -E '^\s*version:' "$MANIFEST" | head -n1 | awk -F ': ' '{print $2}')
if [ -z "$CUR_VERSION" ]; then
  echo "Could not find current version in manifest.yaml. Exiting."
  exit 1
fi

DOCKER_IMAGE="us-central1-docker.pkg.dev/data-infrastructure-324613/airbyte-custom/airbyte/source-quickbooks-drivepoint:$VERSION"

# 2. SSH and update on gcloud
gcloud compute ssh $VM_INSTANCE_NAME --project=data-infrastructure-324613 --command "sudo su - -c 'docker pull $DOCKER_IMAGE && kind load docker-image $DOCKER_IMAGE -n airbyte-abctl'"

echo "Finished deploying version $VERSION to $VM_INSTANCE_NAME"
