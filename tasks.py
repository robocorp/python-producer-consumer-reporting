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


@task
def consumer():
    """Process all the produced input Work Items from the previous step."""
    for item in workitems.inputs:
        try:
            name = item.payload["Name"]
            zipcode = item.payload["Zip"]
            product = item.payload["Product"]
            print(f"Processing order: {name}, {zipcode}, {product}")
            assert 1000 <= zipcode <= 9999, "Invalid ZIP code"
            workitems.outputs.create()
            item.done()
        except AssertionError as err:
            item.fail("BUSINESS", code="INVALID_ORDER", message=str(err))
        except KeyError as err:
            item.fail("APPLICATION", code="MISSING_FIELD", message=str(err))


@task
def reporter():
    """Report on the results of the previous step."""
    # Mark all incoming work items from consumer as done (they were only used to trigger the reporter)
    for item in workitems.inputs:
        item.done()
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
    step_run_ids = _get_consumer_step_run_ids("Consumer")
    # Get the work items for the current process round
    run_work_items = process.list_process_run_work_items(os.getenv("RC_PROCESS_RUN_ID"))
    consumer_work_items = []
    # Iterate over the work items for the current process round
    for rwi in run_work_items:
        # If the work item is for the consumer step, add it to the list
        if "activityRunId" in rwi and rwi["activityRunId"] in step_run_ids:
            # Get the work item details
            work_item = process.get_work_item(rwi["id"], include_data=True)
            consumer_work_items.append(work_item)

    print("\n\nRUN REPORT\n")
    for cwi in consumer_work_items:
        state = "PASS" if cwi["state"] == "COMPLETED" else "FAIL"
        payload = cwi["payload"]
        exception = cwi["exception"] if "exception" in cwi else None
        print(
            f"{state} - Order: '{payload['Name']}' {payload['Zip']} '{payload['Product']}'"
        )
        if exception:
            print(f"\tException: {exception['code']}")


def _get_consumer_step_run_ids(step_name: str):
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
