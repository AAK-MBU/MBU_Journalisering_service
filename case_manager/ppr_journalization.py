"""
This module handles the journalization process for case management.
It contains functionality to upload and journalize documents, and manage case data.
"""

import json
import re

from typing import Any, Dict, Optional, Tuple

from mbu_dev_shared_components.database.connection import RPAConnection
from mbu_dev_shared_components.getorganized.objects import CaseDataJson
from mbu_dev_shared_components.utils.db_stored_procedure_executor import (
    execute_stored_procedure,
)

from case_manager import journalize_process
from case_manager.case_handler import CaseHandler


class DatabaseError(Exception):
    """Custom exception for database related errors."""


class RequestError(Exception):
    """Custom exception for request related errors."""


class EnvVarNotFoundError(Exception):
    """Custom exception for environment variable not found."""

    def __init__(self, var_name, message="Environment variable not found"):
        self.var_name = var_name
        self.message = f"{message}: {var_name}"
        super().__init__(self.message)


def execute_sql_update(
    conn_string: str, procedure_name: str, params: Dict[str, tuple]
) -> None:
    """
    Execute a stored procedure to update data in the database.

    Args:
        conn_string (str): Connection string for the database.
        procedure_name (str): Name of the stored procedure to execute.
        params (Dict[str, tuple]):
            Parameters for the SQL procedure, in the form {param_name: (param_type, param_value)}.

    Raises:
        DatabaseError: If the SQL procedure execution fails.
    """
    sql_update_result = execute_stored_procedure(conn_string, procedure_name, params)
    if not sql_update_result["success"]:
        raise DatabaseError(f"SQL - {procedure_name} failed.")


def log_and_raise_error(
    log_db: str,
    error_message: str,
    context: str,
    exception: Exception,
    db_env: str,
) -> None:
    """
    Log an error and raise the specified exception.

    Args:
        orchestrator_connection (OrchestratorConnection): Connection object to log errors.
        error_message (str): The error message to log.
        exception (Exception): The exception to raise.

    Raises:
        exception: The passed-in exception is raised after logging the error.
    """
    with RPAConnection(db_env=db_env, commit=True) as rpa_conn:
        rpa_conn.log_event(
            log_db=log_db,
            level="ERROR",
            message=error_message,
            context=context,
        )
    raise exception


def handle_database_error(
    conn_string: str,
    procedure_name: str,
    process_status_params_failed: str,
    exception: Exception,
) -> None:
    """
    Handle database errors by executing the failure procedure and raising the exception.

    Args:
        conn_string (str): Connection string for the database.
        procedure_name (str): Name of the stored procedure to execute upon failure.
        process_status_params_failed (str): Parameters for the failure procedure.
        exception (Exception): The exception that occurred.

    Raises:
        exception: Re-raises the original exception.
    """
    execute_stored_procedure(conn_string, procedure_name, process_status_params_failed)
    raise exception


def create_case_folder(
    case_handler: CaseHandler,
    case_type: str,
    person_full_name: str,
    person_go_id: str,
    ssn: str,
    conn_string: str,
    update_response_data: str,
    update_process_status: str,
    process_status_params_failed: str,
    form_id: str,
) -> Optional[str]:
    """
    Create a new case folder if it doesn't exist.

    Returns:
        Optional[str]: The case folder ID if created successfully, otherwise None in case of an error.
    """
    try:
        case_folder_data = case_handler.create_ppr_case_folder_data(
            case_type, person_full_name, person_go_id, ssn
        )
        response = case_handler.create_case_folder(case_folder_data, "/_goapi/Cases")
        if not response.ok:
            raise RequestError("Request response failed during create case folder.")

        case_folder_id = response.json()["CaseID"]

        sql_data_params = {
            "StepName": ("str", "CaseFolder"),
            "JsonFragment": ("str", json.dumps({"CaseFolderId": case_folder_id})),
            "form_id": ("str", form_id),
        }
        execute_sql_update(conn_string, update_response_data, sql_data_params)

        return case_folder_id

    except (DatabaseError, RequestError) as e:
        handle_database_error(
            conn_string, update_process_status, process_status_params_failed, e
        )
        return None

    except Exception as e:
        handle_database_error(
            conn_string,
            update_process_status,
            process_status_params_failed,
            RuntimeError(
                f"An unexpected error occurred during case folder creation: {e}"
            ),
        )
        return None


def create_case_data(
    case_handler: CaseHandler,
    case_type: str,
    case_data: Dict[str, Any],
    case_title: str,
    ppr_case_id: str,
    received_date: str,
    case_profile_id,
    case_profile_name,
    person_full_name: str = None,
    person_go_id: str = None,
    person_ssn: str = None,
) -> Dict[str, Any]:
    """Create the data needed to create a new case."""

    return case_handler.create_ppr_case(
        case_type_prefix=case_type,
        case_category=case_data["caseCategory"],
        case_owner_id=case_data["caseOwnerId"],
        case_owner_name=case_data["caseOwnerName"],
        case_profile_id=case_profile_id,
        case_profile_name=case_profile_name,
        case_title=case_title,
        case_folder_id=ppr_case_id,
        # ows_Afdeling is intentionally omitted for PPR befordring sub-cases —
        # the reference working cases don't carry it; the parent folder holds
        # the department context.
        kle_number=case_data["kleNumber"],
        facet=case_data["facet"],
        start_date=received_date or case_data.get("startDate"),
        person_full_name=person_full_name,
        person_go_id=person_go_id,
        person_ssn=person_ssn,
        return_when_case_fully_created=True,
    )


def create_befordring_case(
    case_handler: CaseHandler,
    parsed_form_data: Dict[str, Any],
    os2form_webform_id: str,
    case_type: str,
    case_data: str,
    conn_string: str,
    update_response_data: str,
    update_process_status: str,
    process_status_params_failed: str,
    form_id: str,
    person_full_name: str = None,
    person_go_id: str = None,
    person_ssn: str = None,
    ppr_case_id: str = None,
    received_date: str = None,
) -> Optional[str]:
    """
    Create a new case and update the database.

    Returns:
        Optional[str]:  The case ID, case title, and relative URL if created successfully,
                        otherwise None in case of an error.
    """
    try:
        case_title = f"Kørsel til {person_full_name}"

        case_data["caseProfileId"], case_data["caseProfileName"] = (
            journalize_process.determine_case_profile(os2form_webform_id, case_data, parsed_form_data)
        )

        print("DEBUG create_befordring_case — input fields:")
        print(f"  case_type       : {case_type!r}")
        print(f"  ppr_case_id     : {ppr_case_id!r}")
        print(f"  person_full_name: {person_full_name!r}")
        print(f"  case_title      : {case_title!r}")
        print(f"  caseCategory    : {case_data.get('caseCategory')!r}")
        print(f"  caseOwnerId     : {case_data.get('caseOwnerId')!r}")
        print(f"  caseOwnerName   : {case_data.get('caseOwnerName')!r}")
        print(f"  caseProfileId   : {case_data.get('caseProfileId')!r}")
        print(f"  caseProfileName : {case_data.get('caseProfileName')!r}")
        print(f"  departmentId    : {case_data.get('departmentId')!r}")
        print(f"  departmentName  : {case_data.get('departmentName')!r}")
        print(f"  kleNumber       : {case_data.get('kleNumber')!r}")
        print(f"  facet           : {case_data.get('facet')!r}")
        print(f"  startDate       : {received_date or case_data.get('startDate')!r}")

        created_case_data = create_case_data(
            case_handler=case_handler,
            case_type=case_type,
            case_data=case_data,
            case_title=case_title,
            ppr_case_id=ppr_case_id,
            received_date=received_date,
            case_profile_id=case_data["caseProfileId"],
            case_profile_name=case_data["caseProfileName"],
            person_full_name=person_full_name,
            person_go_id=person_go_id,
            person_ssn=person_ssn,
        )

        print(f"DEBUG: MetadataXml being sent:\n  {created_case_data.get('MetadataXml', created_case_data)}")

        # Fetch metadata of the parent case — print every attribute
        import xml.etree.ElementTree as ET
        parent_meta_response = case_handler.get_case_metadata(f"/_goapi/Cases/Metadata/{ppr_case_id}")
        if parent_meta_response.ok:
            try:
                attrib = ET.fromstring(parent_meta_response.json().get("Metadata", "")).attrib
                print(f"DEBUG: PPR parent case {ppr_case_id!r} — ALL metadata attributes:")
                for key, value in sorted(attrib.items()):
                    print(f"  {key}: {value!r}")
            except Exception as meta_err:
                print(f"DEBUG: Could not parse parent metadata: {meta_err} — raw: {parent_meta_response.text[:500]}")
        else:
            print(f"DEBUG: get_case_metadata for {ppr_case_id!r} returned {parent_meta_response.status_code}")


        response = case_handler.create_case(created_case_data, "/_goapi/Cases")

        if not response.ok:
            print(f"ERROR: GO API returned {response.status_code}")
            print(f"  Response body   : {response.text}")
            print(f"  Response headers: {dict(response.headers)}")
            raise RequestError("Request response failed.")

        case_id = response.json()["CaseID"]

        case_rel_url = response.json()["CaseRelativeUrl"]

        sql_data_params = {
            "StepName": ("str", "Case"),
            "JsonFragment": ("str", json.dumps({"CaseId": case_id})),
            "form_id": ("str", form_id),
        }

        execute_sql_update(conn_string, update_response_data, sql_data_params)

        return case_id, case_title, case_rel_url

    except (DatabaseError, RequestError) as e:
        handle_database_error(
            conn_string, update_process_status, process_status_params_failed, e
        )
        print(f"An error occurred: {e}")
        raise e

    except Exception as e:
        handle_database_error(
            conn_string,
            update_process_status,
            process_status_params_failed,
            RuntimeError(f"An unexpected error occurred during case creation: {e}"),
        )
        print(f"An error occurred: {e}")
        raise e


def _search_for_sub_case(
    case_handler: CaseHandler,
    case_data_handler: CaseDataJson,
    case_type: str,
    person_full_name: str,
    person_go_id: str,
    ssn: str,
    case_title: str,
) -> Optional[str]:
    """Search for a specific sub-case by title."""

    properties_for_case_search = {"ows_Title": case_title}

    search_data = case_data_handler.generic_search_case_data_json(
        case_type_prefix=case_type,
        person_full_name=person_full_name,
        person_id=person_go_id,
        person_ssn=ssn,
        field_properties=properties_for_case_search
    )

    response = case_handler.search_for_case_folder(
        search_data, "/_goapi/cases/findbycaseproperties"
    )

    if not response.ok:
        raise RequestError("Request response failed during sub-case search.")

    cases_info = response.json().get("CasesInfo", [])
    print(f"DEBUG _search_for_sub_case — title={case_title!r}, hits: {[c.get('CaseID') for c in cases_info]}")
    if cases_info:
        return cases_info[0].get("CaseID")

    return None


def _search_for_ppr_case(
    case_handler: CaseHandler,
    case_data_handler: CaseDataJson,
    case_type: str,
    person_full_name: str,
    person_go_id: str,
    ssn: str,
) -> Optional[str]:
    """Search for any parent PPR case."""

    search_data = case_data_handler.generic_search_case_data_json(
        case_type_prefix=case_type,
        person_full_name=person_full_name,
        person_id=person_go_id,
        person_ssn=ssn,
    )

    response = case_handler.search_for_case_folder(
        search_data, "/_goapi/cases/findbycaseproperties"
    )

    if not response.ok:
        raise RequestError("Request response failed during PPR case search.")

    pattern = re.compile(r"^PPR-\d{4}-\d{6}$")
    cases_info = response.json().get("CasesInfo", [])

    print(f"DEBUG _search_for_ppr_case — all cases found for citizen:")
    for c in cases_info:
        print(f"  CaseID={c.get('CaseID')!r}  Title={c.get('Title', c.get('CaseTitle', '?'))!r}")

    for case in cases_info:
        case_id = case.get("CaseID")

        if pattern.fullmatch(case_id):
            return case_id

    return None


def check_for_befordring_case(
    case_handler: CaseHandler,
    case_data_handler: CaseDataJson,
    case_type: str,
    person_full_name: str,
    person_go_id: str,
    ssn: str,
    conn_string: str,
    update_process_status: str,
    process_status_params_failed: str,
) -> Optional[Dict[str, Optional[str]]]:
    """
    Check for existing PPR case and/or befordring sub-case.

    Returns:
        {
            "ppr_case_id": "PPR-XXXX-XXXXXX" or None,
            "befordring_case_id": "PPR-XXXX-XXXXXX-XXX" or None
        }
        Returns None if neither exists.
    """

    case_title = f"Kørsel til {person_full_name}"

    try:
        # First, try to find the specific sub-case
        befordring_case_id = _search_for_sub_case(
            case_handler, case_data_handler, case_type,
            person_full_name, person_go_id, ssn, case_title
        )

        # Then try to find parent PPR case
        ppr_case_id = _search_for_ppr_case(
            case_handler, case_data_handler, case_type,
            person_full_name, person_go_id, ssn
        )

        if befordring_case_id or ppr_case_id:
            return {
                "ppr_case_id": ppr_case_id,
                "befordring_case_id": befordring_case_id
            }

        return None

    except (DatabaseError, RequestError) as e:
        handle_database_error(
            conn_string, update_process_status, process_status_params_failed, e
        )
        return None

    except Exception as e:
        handle_database_error(
            conn_string,
            update_process_status,
            process_status_params_failed,
            RuntimeError(f"An unexpected error occurred during case folder check: {e}"),
        )
        return None
