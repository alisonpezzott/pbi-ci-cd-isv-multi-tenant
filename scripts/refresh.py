"""
This script deploys the AdventureWorks semantic model to a specified environment using the Power BI Fabric CLI.
"""

import os
import argparse
from utils import *

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--spn-auth", action="store_true", default=False)
parser.add_argument("--environment", default="dev")
parser.add_argument("--config-file", default="./config.json")

args = parser.parse_args()

current_file = __file__
current_folder = os.path.dirname(current_file)
src_folder = os.path.join(current_folder, "..", "src")

# Deployment parameters:

spn_auth = args.spn_auth
environment = args.environment

config = read_pbip_jsonfile(args.config_file)
configEnv = config[args.environment]

workspace_name = configEnv["workspace"]

# Authentication

if spn_auth:
    fab_authenticate_spn()
        
workspace_id = run_fab_command(
        f"get /{workspace_name}.workspace -q id",
        capture_output=True
    )

# Deploy semantic models

semanticmodel_ids = {}

for folder_name in os.listdir(os.path.join(src_folder, "semanticmodels")):   

    platform_data = read_pbip_jsonfile(os.path.join(src_folder, "semanticmodels", folder_name, ".platform"))

    semanticmodel_name = platform_data["metadata"]["displayName"]
    
    semanticmodel_id = run_fab_command(
        f"get /{workspace_name}.workspace/{semanticmodel_name}.semanticmodel -q id",
        capture_output=True
    )

    print(f"Refreshing semantic model '{folder_name}': {workspace_id}/{semanticmodel_id}")
    print(f"api -A powerbi -X post groups/${workspace_id}/datasets/${semanticmodel_id}/refreshes")

    run_fab_command(f"api -A powerbi -X post groups/{workspace_id}/datasets/{semanticmodel_id}/refreshes")

# Log out to allow easy switching between SPN & Interactive for local run

if spn_auth:
    run_fab_command("auth logout")
