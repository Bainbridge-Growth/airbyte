import sys

from airbyte_cdk.entrypoint import launch
from source_quickbooks_drivepoint import SourceQuickbooksDrivepoint


def run():
    source = SourceQuickbooksDrivepoint()
    launch(source, sys.argv[1:])
