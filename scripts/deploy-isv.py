
import os
import argparse
import glob
from utils import *

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--config-file", default="./config-isv.json")
parser.add_argument("--tenant", default=None)

args = parser.parse_args()

current_file = __file__
current_folder = os.path.dirname(current_file)
src_folder = os.path.join(current_folder, "..", "src")

config = read_pbip_jsonfile(args.config_file)

semanticmodel_src = f"{src_folder}/semanticmodels/AdventureWorks"

reports_src = [ f"{src_folder}/reports/AdventureWorks", f"{src_folder}/reports/Stocks"]

config_environments = config.items()

tenant_arg = args.tenant

for key, value in config_environments:
    
    if (tenant_arg is not None and key.casefold() != tenant_arg.casefold()):
        continue

    print(f"Deploying to tenant: {key}")

    capacity_name = value.get("capacity", "none")
    workspace_name = value["workspace"]
    admin_upns = value.get("adminUPNs", "").split(",")
    semanticmodel_parameters = value.get("semanticModelsParameters", None)
    server = semanticmodel_parameters.get("SqlServerInstance", None)
    database = semanticmodel_parameters.get("SqlServerDatabase", None)


    # Get SPN details from the environment variable and authenticate with the SPN details

    spn_secret_envname = value["spnSecret"]

    spn_secret_raw = os.getenv(spn_secret_envname)

    if (spn_secret_raw is None):
        raise Exception(f"Environment variable '{spn_secret_envname}' not found.")

    spn_client_id, spn_client_secret, spn_tenant_id = spn_secret_raw.split('|', 2)    

    os.environ["FABRIC_CLIENT_ID"] = spn_client_id
    os.environ["FABRIC_CLIENT_SECRET"] = spn_client_secret
    os.environ["FABRIC_TENANT_ID"] = spn_tenant_id

    fab_authenticate_spn()

    # ensure workspace

    create_workspace(
        workspace_name=workspace_name, capacity_name=capacity_name, upns=admin_upns
    )

    # Deploy semantic model

    semanticmodel_id = deploy_item(
        "src/AdventureWorks.SemanticModel",
        workspace_name=workspace_name,
        find_and_replace={
            (
                r"expressions.tmdl",
                r'(expression\s+SqlServerInstance\s*=\s*)".*?"',
            ): rf'\1"{server}"',
            (
                r"expressions.tmdl",
                r'(expression\s+SqlServerDatabase\s*=\s*)".*?"',
            ): rf'\1"{database}"',
        },
    )

    # Deploy reports

    for report_path in glob.glob("src/*.Report"):

        deploy_item(
            report_path,
            workspace_name=workspace_name,
            find_and_replace={
                ("definition.pbir", r"\{[\s\S]*\}"): json.dumps(
                    {
                        "version": "4.0",
                        "datasetReference": {
                            "byConnection": {
                                "connectionString": None,
                                "pbiServiceModelId": None,
                                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                                "pbiModelDatabaseName": semanticmodel_id,
                                "name": "EntityDataSource",
                                "connectionType": "pbiServiceXmlaStyleLive",
                            }
                        },
                    }
                )
            },
        )

    run_fab_command("auth logout")

