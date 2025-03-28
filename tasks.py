import json
import os
from pathlib import Path

from robocorp import workitems, vault
from robocorp.tasks import get_output_dir, task
from RPA.Excel.Files import Files as Excel
from RPA.Robocorp.Process import Process
import requests


@task
def producer():
    """Split Excel rows into multiple output Work Items for the next step."""
    output = get_output_dir() or Path("output")
    excel = Excel()
    input_item = workitems.inputs.current
    files = input_item.get_files("*.xlsx", output)
    outputs_created = False
    for file in files:
        excel.open_workbook(file)
        rows = excel.read_worksheet_as_table(header=True)

        for row in rows:
            payload = {
                "Name": row["Name"],
                "Zip": row["Zip"],
                "Product": row["Item"],
            }
            workitems.outputs.create(payload)
            outputs_created = True
    if outputs_created:
        # Create a Reporter work item to guarantee that Reporter step is triggered even if all Consumer work items fail.
        workitems.outputs.create({"TYPE": "Reporter"})
    else:
        # If no outputs were created, mark the input item as failed
        input_item.fail(
            "APPLICATION", code="NO_OUTPUTS", message="No outputs were created"
        )


@task
def consumer():
    """Process all the produced input Work Items from the previous step."""
    for item in workitems.inputs:
        if "TYPE" in item.payload.keys() and item.payload["TYPE"] == "Reporter":
            workitems.outputs.create()
            item.done()
            continue
        try:
            name = item.payload["Name"]
            zipcode = item.payload["Zip"]
            product = item.payload["Product"]
            print(f"Processing order: {name}, {zipcode}, {product}")
            assert 1000 <= zipcode <= 9999, "Invalid ZIP code"
            item.payload["ProcessingStatus"] = "DONE"
            item.save()
            item.done()
        except AssertionError as err:
            item.payload["ProcessingStatus"] = "FAIL - INVALID_ORDER"
            item.save()
            item.fail("BUSINESS", code="INVALID_ORDER", message=str(err))
        except KeyError as err:
            item.payload["ProcessingStatus"] = "FAIL - MISSING_FIELD"
            item.save()
            item.fail("APPLICATION", code="MISSING_FIELD", message=str(err))


@task
def reporter():
    """Report on the results of the previous step."""
    # Mark all incoming work items from consumer as done (they were only used to trigger the reporter)
    for item in workitems.inputs:
        item.done()

    consumer_work_items = _filter_consumer_work_items()
    _output_report(consumer_work_items)


def _filter_consumer_work_items() -> list:
    """Filter the consumer work items from the current process run's all work items"""
    # Check if we're running in cloud environment
    is_cloud = os.getenv("RC_PROCESS_RUN_ID") is not None

    if is_cloud:
        return _get_cloud_consumer_work_items()
    else:
        return _get_local_consumer_work_items()


def _get_cloud_consumer_work_items() -> list:
    """Get consumer work items from cloud environment using Process API"""
    # Get the credentials to access the Robocorp Process API
    secrets = vault.get_secret("robocorp_process_api")
    # The RPA.Process library is used to interact with the Robocorp Process API
    process = Process()
    process.set_credentials(
        workspace_id=secrets["workspace_id"],
        process_id=secrets["process_id"],
        apikey=secrets["apikey"],
    )

    # Get the step run ids for the consumer step
    step_run_ids = _get_step_run_ids_by_step_name("Consumer")
    # Get the work items for the current process round
    run_work_items = process.list_process_run_work_items(os.getenv("RC_PROCESS_RUN_ID"))
    consumer_work_items = []
    # Iterate over the work items for the current process round
    for rwi in run_work_items:
        # If the work item is for the consumer step, add it to the list
        if "activityRunId" in rwi and rwi["activityRunId"] in step_run_ids:
            # Get the work item details
            work_item = process.get_work_item(rwi["id"], include_data=True)
            # Do not include the Reporter work item
            if (
                "TYPE" in work_item["payload"].keys()
                and work_item["payload"]["TYPE"] == "Reporter"
            ):
                continue
            consumer_work_items.append(work_item)
    return consumer_work_items


def _get_local_consumer_work_items() -> list:
    """Get consumer work items from local environment using file adapter"""

    # Get the input path from environment or use default
    input_path = os.getenv(
        "RC_WORKITEM_INPUT_PATH",
        os.getenv("COMPLETED_CONSUMER_WORKITEMS_JSON"),
    )

    # Read the work items from the input file
    with open(input_path, "r") as f:
        work_items = json.load(f)

    # Filter out the Reporter work item
    consumer_work_items = []
    for item in work_items:
        if "TYPE" not in item["payload"] or item["payload"]["TYPE"] != "Reporter":
            consumer_work_items.append(item)

    return consumer_work_items


def _get_step_run_ids_by_step_name(step_name: str):
    """Get the step run ids for the given step name"""
    secrets = vault.get_secret("robocorp_process_api")
    headers = {"Authorization": f"RC-WSKEY {secrets['apikey']}"}
    response = requests.get(
        f'https://cloud.robocorp.com/api/v1/workspaces/{secrets["workspace_id"]}/step-runs?process_run_id={os.getenv("RC_PROCESS_RUN_ID")}',
        headers=headers,
    )
    results = response.json()["data"]
    # TODO. handle if has_more is True, not included in this simple example
    step_run_ids = []
    for result in results:
        if result["step"]["name"] == step_name:
            step_run_ids.append(result["id"])
    return step_run_ids


def _output_report(work_items: list):
    """Output a report of the work items processing results"""
    print("\n\nRUN REPORT\n")
    for cwi in work_items:
        payload = cwi["payload"]
        exception = cwi.get("exception") if "exception" in cwi else None
        print(
            f"{payload['ProcessingStatus']} - Order: '{payload['Name']}' {payload['Zip']} '{payload['Product']}'"
        )
        if exception:
            print(f"\tException: {exception['code']}")
