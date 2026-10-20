"""
Test script to run a single form through the full journalization flow.

Usage:
    python test_single_form.py [FORM_ID] [--force]

Runs against the database/GetOrganized environment set by ENV in config.py.
"""

import sys
import pyodbc
from case_manager import journalize_process as jp
from case_manager.helper_functions import fetch_cases_metadata
from config import ENV, HANDLE_FORMS
from process import main_process

FORM_ID = "f10e7cec-778d-429e-9675-71ebd8440f06"

args = [a for a in sys.argv[1:] if a != "--force"]
form_id = args[0] if args else FORM_ID
force = "--force" in sys.argv[1:]

try:
    # Get credentials using the same method as the service
    credentials = jp.get_credentials_and_constants(db_env=ENV)

    print(f"✓ Loaded credentials ({ENV})")

    # Get cases metadata using the same method as the service
    cases_metadata = fetch_cases_metadata(
        connection_string=credentials["DbConnectionString"]
    )
    if not cases_metadata:
        print("✗ Failed to load cases metadata")
        sys.exit(1)
    print(f"✓ Loaded cases metadata for {len(cases_metadata)} form types")

    # Query for the specific form (same columns as jp.get_forms_data, plus status)
    query = """
        SELECT
            j.form_id
            ,f.form_data
            ,CAST(f.form_submitted_date AS datetime) AS form_submitted_date
            ,f.form_type as os2formwebform_id
            ,COALESCE(j.attempt_count, 0) as attempt_count
            ,j.status
        FROM
            [RPA].[journalizing].[Journalizing] j
        JOIN
            [RPA].[journalizing].[Forms] f on f.form_id = j.form_id
        WHERE
            j.form_id = ?
    """

    with pyodbc.connect(credentials["DbConnectionString"]) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (form_id,))
            columns = [column[0] for column in cursor.description]
            row = cursor.fetchone()

    if not row:
        print(f"✗ Form {form_id} not found in database")
        sys.exit(1)

    form = dict(zip(columns, row))
    status = form.pop("status")
    form_type = form["os2formwebform_id"]

    if form_type not in cases_metadata:
        print(f"✗ Form type '{form_type}' not in active cases metadata")
        sys.exit(1)

    if form_type not in HANDLE_FORMS:
        print(f"! Form type '{form_type}' is not in HANDLE_FORMS - the service will not pick it up")

    if status in ("Successful", "InProgress") and not force:
        print(f"✗ Form status is '{status}' - rerunning may create duplicate cases/documents. Use --force to run anyway.")
        sys.exit(1)

    print(f"✓ Found form: {form_type} (status {status}, attempt #{form['attempt_count']})")
    print(f"  Form ID: {form_id}")
    print(f"  Submitted: {form['form_submitted_date']}")
    print(f"  Case type: {cases_metadata[form_type]['caseType']}")
    print("-" * 60)

    # Run the process
    main_process(form, credentials, cases_metadata, db_env=ENV)

    print("-" * 60)
    print("✓ Process finished - check the form status in [RPA].[journalizing].[Journalizing]")

except Exception as e:
    print(f"✗ Error during processing: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
