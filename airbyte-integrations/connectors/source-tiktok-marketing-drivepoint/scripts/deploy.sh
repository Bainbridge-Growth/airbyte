#!/bin/bash
# Deploy script for source-tiktok-marketing-drivepoint from remote docker to gcloud VM
# Use this to (re)deploy an already-built-and-pushed version, e.g. after a
# rollback or when re-provisioning the VM, without rebuilding the image.

set -e

usage() {
  local name
  name="$(basename "$0")"
  cat <<EOF
Usage: $name <version> <staging|production>
       $name -h|--help

Deploys an existing source-tiktok-marketing-drivepoint image version from Artifact Registry
by pulling it on the target gcloud VM and loading it into the Airbyte kind cluster.
No new image is built and no version files are changed.

Arguments:
  version       Existing image version to deploy, e.g. 1.2.3
  environment   Target environment:
                  staging     -> VM airbyte-qbo-staging
                  production  -> VM airbyte-quickbooks

Options:
  -h, --help    Show this help and exit

Examples:
  $name 1.2.3 staging
  $name 1.2.3 production
EOF
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 0
fi

# Parse arguments
VERSION=${1}
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Invalid version: '$VERSION'. Must be an existing version number like 1.2.3."
  echo
  usage
  exit 1
fi

ENVIRONMENT=${2}
case "$ENVIRONMENT" in
  staging)
    VM_INSTANCE_NAME="airbyte-qbo-staging"
    ;;
  production)
    VM_INSTANCE_NAME="airbyte-quickbooks"
    ;;
  *)
    echo "Must provide target environment name as a second argument ('staging' or 'production')."
    echo
    usage
    exit 1
    ;;
esac

GCP_PROJECT="data-infrastructure-324613"

DOCKER_IMAGE="us-central1-docker.pkg.dev/$GCP_PROJECT/airbyte-custom/airbyte/source-tiktok-marketing-drivepoint:$VERSION"

# SSH and update on gcloud
gcloud compute ssh $VM_INSTANCE_NAME --project=$GCP_PROJECT --command "sudo su - -c 'docker pull $DOCKER_IMAGE && kind load docker-image $DOCKER_IMAGE -n airbyte-abctl'"

echo "Finished deploying version $VERSION to $VM_INSTANCE_NAME"
