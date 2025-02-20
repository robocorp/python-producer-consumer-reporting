import os
from pathlib import Path

from robocorp import workitems, vault
from robocorp.tasks import get_output_dir, task
from RPA.Excel.Files import Files as Excel
from RPA.Robocorp.Process import Process


@task
def producer():
    """Split Excel rows into multiple output Work Items for the next step."""
    output = get_output_dir() or Path("output")
    filename = "orders.xlsx"

    for item in workitems.inputs:
        path = item.get_file(filename, output / filename)

        excel = Excel()
        excel.open_workbook(path)
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
            workitems.outputs.create({"step_id": os.getenv("RC_ACTIVITY_ID")})
            item.done()
        except AssertionError as err:
            item.fail("BUSINESS", code="INVALID_ORDER", message=str(err))
        except KeyError as err:
            item.fail("APPLICATION", code="MISSING_FIELD", message=str(err))


@task
def reporter():
    """Report on the results of the previous step."""
    time_to_report = True
    input_work_item = workitems.inputs.current
    payload = input_work_item.payload
    step_id = payload["step_id"]
    run_work_items = _get_process_run_work_items()
    for index, input_work_item in enumerate(run_work_items):
        if (
            input_work_item["activityId"] != step_id
            or input_work_item["state"] == "COMPLETED"
        ):
            continue
        if input_work_item["state"] == "PENDING":
            time_to_report = False
            break
    input_work_item.payload["time_to_report"] = time_to_report
    input_work_item.save()
    if time_to_report:
        print("\nTIME TO REPORT\n")
    else:
        print("\nNOT TIME TO REPORT\n")


def _get_process_run_work_items():
    secrets = vault.get_secret("robocorp-process-api")
    process = Process()
    process.set_credentials(
        workspace_id=secrets["workspace_id"],
        process_id=secrets["process_id"],
        apikey=secrets["apikey"],
    )
    return process.list_process_run_work_items(os.getenv("RC_PROCESS_RUN_ID"))
