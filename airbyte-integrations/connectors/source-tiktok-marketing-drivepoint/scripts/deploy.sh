#!/bin/bash
# Deploy script for source-tiktok-marketing-drivepoint from remote docker to gcloud VM
# Use this to (re)deploy an already-built-and-pushed version, e.g. after a
# rollback or when re-provisioning the VM, without rebuilding the image.

set -e

VERSION=$1
if [[ -z "$VERSION" ]]; then
  echo "Must provide version to deploy as the first argument."
  exit 1
fi

GCP_PROJECT="data-infrastructure-324613"
VM_INSTANCE_NAME="airbyte-qbo-staging"

DOCKER_IMAGE="us-central1-docker.pkg.dev/$GCP_PROJECT/airbyte-custom/airbyte/source-tiktok-marketing-drivepoint:$VERSION"

# SSH and update on gcloud
gcloud compute ssh $VM_INSTANCE_NAME --project=$GCP_PROJECT --command "sudo su - -c 'docker pull $DOCKER_IMAGE && kind load docker-image $DOCKER_IMAGE -n airbyte-abctl'"

echo "Finished deploying version $VERSION to $VM_INSTANCE_NAME"
