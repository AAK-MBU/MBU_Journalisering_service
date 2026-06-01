"""Test script to run a single form through the PPR befordring workflow."""

import sys
import pyodbc
from case_manager import journalize_process as jp
from case_manager.helper_functions import fetch_cases_metadata
from process import main_process

FORM_ID = "f10e7cec-778d-429e-9675-71ebd8440f06"

try:
    # Get credentials using the same method as the service
    credentials = jp.get_credentials_and_constants(db_env="PROD")

    print("✓ Loaded credentials")

    # Get cases metadata using the same method as the service
    cases_metadata = fetch_cases_metadata(
        connection_string=credentials["DbConnectionString"]
    )
    if not cases_metadata:
        print("✗ Failed to load cases metadata")
        sys.exit(1)
    print(f"✓ Loaded cases metadata for {len(cases_metadata)} form types")

    # Query for the specific form
    query = """
        SELECT
            j.form_id
            ,f.form_data
            ,CAST(f.form_submitted_date AS datetime) AS form_submitted_date
            ,f.form_type as os2formwebform_id
            ,COALESCE(j.attempt_count, 0) as attempt_count
        FROM
            [RPA].[journalizing].[Journalizing] j
        JOIN
            [RPA].[journalizing].[Forms] f on f.form_id = j.form_id
        WHERE
            j.form_id = ?
    """

    with pyodbc.connect(credentials["DbConnectionString"]) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (FORM_ID,))
            columns = [column[0] for column in cursor.description]
            row = cursor.fetchone()

    if not row:
        print(f"✗ Form {FORM_ID} not found in database")
        sys.exit(1)

    form = dict(zip(columns, row))
    form_type = form["os2formwebform_id"]

    if form_type not in cases_metadata:
        print(f"✗ Form type '{form_type}' not in cases metadata")
        sys.exit(1)

    print(f"✓ Found form: {form_type} (attempt #{form['attempt_count']})")
    print(f"  Form ID: {FORM_ID}")
    print(f"  Submitted: {form['form_submitted_date']}")
    print("-" * 60)

    # Run the process
    main_process(form, credentials, cases_metadata, db_env="PROD")

    print("-" * 60)
    print("✓ Process completed successfully")

except Exception as e:
    print(f"✗ Error during processing: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
