#!/bin/bash
# Deploy script for source-quickbooks-drivepoint from local dev docker to gcloud VM
# 1. Get current version from manifest.yaml
# 2. Bump version (major, minor, or patch, default patch)
# 3. Update manifest.yaml, pyproject.toml, metadata.yaml
# 4. Tag and push docker image
# 5. SSH to gcloud and update image

set -e

# Parse arguments
BUMP_TYPE=${1:-patch}
if [[ "$BUMP_TYPE" != "major" && "$BUMP_TYPE" != "minor" && "$BUMP_TYPE" != "patch" ]]; then
  echo "Invalid bump type: $BUMP_TYPE. Must be 'major', 'minor', or 'patch'."
  exit 1
fi

ENVIRONMENT=${2}
if [[ -z "$ENVIRONMENT" ]]; then
  echo "Must provide VM instance name as a second argument."
  exit 1
fi

VM_INSTANCE_NAME="airbyte_quickbooks"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONNECTOR_DIR="$(dirname "$SCRIPT_DIR")"
MANIFEST="$CONNECTOR_DIR/source_quickbooks_drivepoint/manifest.yaml"
PYPROJECT="$CONNECTOR_DIR/pyproject.toml"
METADATA="$CONNECTOR_DIR/metadata.yaml"

if [ ! -f "$MANIFEST" ]; then
  echo "manifest.yaml not found in $CONNECTOR_DIR. Exiting."
  exit 1
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  echo "Docker is not running. Please start Docker and try again."
  exit 1
fi

# 0. Build new docker image locally
airbyte-ci connectors --name=source-quickbooks-drivepoint build --architecture=linux/amd64
if [ $? -ne 0 ]; then
  echo "Build failed. Exiting."
  exit 1
fi

# 1. Get current version from manifest.yaml (robust: match 'version:' at start of any line, any indent)
CUR_VERSION=$(grep -E '^\s*version:' "$MANIFEST" | head -n1 | awk -F ': ' '{print $2}')
if [ -z "$CUR_VERSION" ]; then
  echo "Could not find version in manifest.yaml. Exiting."
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

if [[ "$ENVIRONMENT" == "staging" ]]; then
  VM_INSTANCE_NAME="airbyte-qbo-staging"
fi

# 3. Update manifest.yaml (robust: match 'version:' at start of any line, any indent)
sed -i '' "s/^version: .*/version: $NEW_VERSION/" "$MANIFEST"

# 4. Update pyproject.toml (robust: match 'version = "..."' at start of any line)
sed -i '' "s/^version = \".*\"/version = \"$NEW_VERSION\"/" "$PYPROJECT"

# 5. Update metadata.yaml
grep -q 'dockerImageTag:' "$METADATA" && \
  sed -i '' "s/^dockerImageTag: .*/dockerImageTag: $NEW_VERSION/" "$METADATA"

# 6. Docker tag and push
DOCKER_IMAGE="us-central1-docker.pkg.dev/data-infrastructure-324613/airbyte-custom/airbyte/source-quickbooks-drivepoint:$NEW_VERSION"
docker tag airbyte/source-quickbooks-drivepoint:dev $DOCKER_IMAGE
docker push $DOCKER_IMAGE

# 7. SSH and update on gcloud
gcloud compute ssh $VM_INSTANCE_NAME --project=data-infrastructure-324613 --command "sudo su - -c 'docker pull $DOCKER_IMAGE && kind load docker-image $DOCKER_IMAGE -n airbyte-abctl'"

echo "Deploy to $VM_INSTANCE_NAME complete. Version: $NEW_VERSION"
