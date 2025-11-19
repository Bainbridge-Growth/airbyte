#!/bin/bash
# Deploy script for source-quickbooks-drivepoint from local dev docker to gcloud VM
# 1. Get current version from manifest.yaml
# 2. Bump patch version
# 3. Update manifest.yaml, pyproject.toml, metadata.yaml
# 4. Tag and push docker image
# 5. SSH to gcloud and update image

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONNECTOR_DIR="$(dirname "$SCRIPT_DIR")"
MANIFEST="$CONNECTOR_DIR/source_quickbooks_drivepoint/manifest.yaml"
PYPROJECT="$CONNECTOR_DIR/pyproject.toml"
METADATA="$CONNECTOR_DIR/metadata.yaml"

if [ ! -f "$MANIFEST" ]; then
  echo "manifest.yaml not found in $CONNECTOR_DIR. Exiting."
  exit 1
fi

# 0. Build new docker image locally
airbyte-ci connectors --name=source-quickbooks-drivepoint build --architecture=linux/amd64

# 1. Get current version from manifest.yaml (robust: match 'version:' at start of any line, any indent)
CUR_VERSION=$(grep -E '^\s*version:' "$MANIFEST" | head -n1 | awk -F ': ' '{print $2}')
if [ -z "$CUR_VERSION" ]; then
  echo "Could not find version in manifest.yaml. Exiting."
  exit 1
fi

# 2. Bump patch version
IFS='.' read -r MAJOR MINOR PATCH <<< "$CUR_VERSION"
NEW_VERSION="$MAJOR.$MINOR.$((PATCH+1))"

# 3. Update manifest.yaml (robust: match 'version:' at start of any line, any indent)
sed -i '' -E "s/^([[:space:]]*version:).*/\1 $NEW_VERSION/" "$MANIFEST"

# 4. Update pyproject.toml (robust: match 'version = "..."' at start of any line)
sed -i '' -E "s/^version = \".*\"/version = \"$NEW_VERSION\"/" "$PYPROJECT"

# 5. Update metadata.yaml
grep -q 'dockerImageTag:' "$METADATA" && \
  sed -i '' -E "s/^([[:space:]]*dockerImageTag:).*/\1 $NEW_VERSION/" "$METADATA"

# 6. Docker tag and push
DOCKER_IMAGE="us-central1-docker.pkg.dev/data-infrastructure-324613/airbyte-custom/airbyte/source-quickbooks-drivepoint:$NEW_VERSION"
docker tag airbyte/source-quickbooks-drivepoint:dev $DOCKER_IMAGE
docker push $DOCKER_IMAGE

# 7. SSH and update on gcloud
gcloud compute ssh airbyte-quickbooks --project=data-infrastructure-324613 --command "sudo su - -c 'docker pull $DOCKER_IMAGE && kind load docker-image $DOCKER_IMAGE -n airbyte-abctl'"

echo "Deploy complete. Version: $NEW_VERSION"
