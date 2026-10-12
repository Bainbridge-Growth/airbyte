#!/bin/bash
# Deploy script for source-quickbooks-drivepoint from remote docker to gcloud VM
# Deploys an already built and pushed image version (see build_and_deploy.sh to build a new one)

set -e

usage() {
  local name
  name="$(basename "$0")"
  cat <<EOF
Usage: $name <version> <staging|production>
       $name -h|--help

Deploys an existing source-quickbooks-drivepoint image version from Artifact Registry
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

DOCKER_IMAGE="us-central1-docker.pkg.dev/data-infrastructure-324613/airbyte-custom/airbyte/source-quickbooks-drivepoint:$VERSION"

# SSH and update on gcloud
gcloud compute ssh $VM_INSTANCE_NAME --project=data-infrastructure-324613 --command "sudo su - -c 'docker pull $DOCKER_IMAGE && kind load docker-image $DOCKER_IMAGE -n airbyte-abctl'"

echo "Finished deploying version $VERSION to $VM_INSTANCE_NAME"
