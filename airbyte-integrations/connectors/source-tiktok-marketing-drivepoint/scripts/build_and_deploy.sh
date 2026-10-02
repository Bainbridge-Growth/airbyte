#!/bin/bash
# Deploy script for source-tiktok-marketing-drivepoint from local dev docker to gcloud VM
# 1. Get current version from metadata.yaml (dockerImageTag)
# 2. Bump version (major, minor, or patch, default patch)
# 3. Update metadata.yaml
# 4. Build, tag and push docker image
# 5. SSH to gcloud and update image
#
# Note: unlike source-quickbooks-drivepoint, this connector is manifest-only.
# manifest.yaml's own `version:` field is the Airbyte CDK schema version this
# manifest was built against, not the connector release version - it must NOT
# be bumped here. There is also no pyproject.toml at the connector root to
# update. metadata.yaml's dockerImageTag is the single source of truth.

set -e

# Parse arguments
BUMP_TYPE=${1:-patch}
if [[ "$BUMP_TYPE" != "major" && "$BUMP_TYPE" != "minor" && "$BUMP_TYPE" != "patch" ]]; then
  echo "Invalid bump type: $BUMP_TYPE. Must be 'major', 'minor', or 'patch'."
  exit 1
fi

GCP_PROJECT="data-infrastructure-324613"
VM_INSTANCE_NAME="airbyte-quickbooks"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONNECTOR_DIR="$(dirname "$SCRIPT_DIR")"
METADATA="$CONNECTOR_DIR/metadata.yaml"

if [ ! -f "$METADATA" ]; then
  echo "metadata.yaml not found in $CONNECTOR_DIR. Exiting."
  exit 1
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  echo "Docker is not running. Please start Docker and try again."
  exit 1
fi

# 0. Build new docker image locally
airbyte-ci connectors --name=source-tiktok-marketing-drivepoint build --architecture=linux/amd64

# 1. Get current version from metadata.yaml's dockerImageTag (robust: match at any indent)
CUR_VERSION=$(grep -E '^\s*dockerImageTag:' "$METADATA" | head -n1 | awk -F ': ' '{print $2}')
if [ -z "$CUR_VERSION" ]; then
  echo "Could not find dockerImageTag in metadata.yaml. Exiting."
  exit 1
fi

# 2. Bump version based on BUMP_TYPE
IFS='.' read -r MAJOR MINOR PATCH <<< "$CUR_VERSION"
case $BUMP_TYPE in
  major)
    NEW_VERSION="$((MAJOR+1)).0.0"
    ;;
  minor)
    NEW_VERSION="$MAJOR.$((MINOR+1)).0"
    ;;
  patch)
    NEW_VERSION="$MAJOR.$MINOR.$((PATCH+1))"
    ;;
esac

# 3. Update metadata.yaml (robust: preserve existing indentation)
sed -i '' -E "s/^([[:space:]]*dockerImageTag:).*/\1 $NEW_VERSION/" "$METADATA"

# 4. Docker tag and push
DOCKER_IMAGE="us-central1-docker.pkg.dev/$GCP_PROJECT/airbyte-custom/airbyte/source-tiktok-marketing-drivepoint:$NEW_VERSION"
docker tag airbyte/source-tiktok-marketing-drivepoint:dev $DOCKER_IMAGE
docker push $DOCKER_IMAGE

# 5. SSH and update on gcloud
gcloud compute ssh $VM_INSTANCE_NAME --project=$GCP_PROJECT --command "sudo su - -c 'docker pull $DOCKER_IMAGE && kind load docker-image $DOCKER_IMAGE -n airbyte-abctl'"

echo "Deploy to $VM_INSTANCE_NAME complete. Version: $NEW_VERSION"
