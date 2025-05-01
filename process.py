"""This module contains the main process of the robot."""

import json

from mbu_dev_shared_components.utils.db_stored_procedure_executor import (
    execute_stored_procedure,
)
from mbu_dev_shared_components.getorganized.objects import CaseDataJson
from mbu_dev_shared_components.database.logging import log_event

from case_manager.case_handler import CaseHandler
from case_manager.document_handler import DocumentHandler
from case_manager import journalize_process as jp
from case_manager.helper_functions import notify_stakeholders

from config import LOG_DB, LOG_CONTEXT


def main_process(form, credentials, cases_metadata, db_env="PROD") -> None:
    """Do the primary process of the robot."""

    # Setup handlers
    case_handler = CaseHandler(
        credentials["go_api_endpoint"],
        credentials["go_api_username"],
        credentials["go_api_password"],
    )
    case_data_handler = CaseDataJson()
    document_handler = DocumentHandler(
        credentials["go_api_endpoint"],
        credentials["go_api_username"],
        credentials["go_api_password"],
    )

    # Extract form data and metadata
    os2formwebform_id = form["os2formwebform_id"]
    case_metadata = cases_metadata[os2formwebform_id]
    case_metadata["os2formwebform_id"] = os2formwebform_id  # Pass webformid into case_metadata to retrieve later
    process_name = case_metadata["description"]
    form_id = form["form_id"]
    form_submitted_date = form["form_submitted_date"]
    parsed_form_data = json.loads(form["form_data"])

    person_full_name = None
    case_folder_id = None

    # Get status params
    status_params_inprogress, status_params_success, status_params_failed, status_params_manual = (
        get_status_params(form_id)
    )

    try:
        ssn = extract_ssn(
            os2formwebform_id=os2formwebform_id,
            parsed_form_data=parsed_form_data,
        )

        if ssn is None: 
            if os2formwebform_id in ('indmeldelse_i_modtagelsesklasse'):
                execute_stored_procedure(
                    credentials["DbConnectionString"],
                    case_metadata["spUpdateProcessStatus"],
                    status_params_manual
                )
                return None
            if os2formwebform_id not in ('respekt_for_graenser', 'respekt_for_graenser_privat', 'indmeld_kraenkelser_af_boern'):
                raise ValueError("SSN is None")

    except ValueError as e:
        message = "Error extracting SSN"
        handle_error(
            message=message,
            case_metadata=case_metadata,
            error=e,
            process_name=process_name,
            credentials=credentials,
            form_id=form_id,
            db_env=db_env,
            form=form,
        )

    log_event(
        LOG_DB,
        "INFO",
        f"Beginning journalizing - {form_id = }, {form_submitted_date = }, {os2formwebform_id = }",
        context=f"{LOG_CONTEXT} ({process_name})",
        db_env=db_env,
    )

    execute_stored_procedure(
        credentials["DbConnectionString"],
        case_metadata["spUpdateProcessStatus"],
        status_params_inprogress,
    )

    if cases_metadata[os2formwebform_id]["caseType"] == "BOR":
        log_event(
            log_db=LOG_DB,
            level="INFO",
            message="Looking up the citizen.",
            context=f"{LOG_CONTEXT} ({process_name})",
            db_env=db_env,
        )
        try:
            person_full_name, person_go_id = jp.contact_lookup(
                case_handler=case_handler,
                ssn=ssn,
                conn_string=credentials["DbConnectionString"],
                update_response_data=case_metadata["spUpdateResponseData"],
                update_process_status=case_metadata["spUpdateProcessStatus"],
                process_status_params_failed=status_params_failed,
                form_id=form_id,
            )
        except Exception as e:
            message = "Error looking up the citizen"
            handle_error(
                message=message,
                case_metadata=case_metadata,
                error=e,
                process_name=process_name,
                credentials=credentials,
                form_id=form_id,
                db_env=db_env,
                form=form,
            )
            # continue
        log_event(
            LOG_DB,
            "INFO",
            "Checking for existing citizen folder.",
            context=f"{LOG_CONTEXT} ({process_name})",
            db_env=db_env,
        )
        try:
            case_folder_id = jp.check_case_folder(
                case_handler=case_handler,
                case_data_handler=case_data_handler,
                case_type=case_metadata["caseType"],
                person_full_name=person_full_name,
                person_go_id=person_go_id,
                ssn=ssn,
                conn_string=credentials["DbConnectionString"],
                update_response_data=case_metadata["spUpdateResponseData"],
                update_process_status=case_metadata["spUpdateProcessStatus"],
                process_status_params_failed=status_params_failed,
                form_id=form_id,
            )
        except Exception as e:
            message = "Error checking for existing citizen folder."
            handle_error(
                message=message,
                case_metadata=case_metadata,
                error=e,
                process_name=process_name,
                credentials=credentials,
                form_id=form_id,
                db_env=db_env,
                form=form,
            )
            # continue
        if not case_folder_id:
            log_event(
                LOG_DB,
                "INFO",
                "Creating citizen folder.",
                context=f"{LOG_CONTEXT} ({process_name})",
                db_env=db_env,
            )
            try:
                case_folder_id = jp.create_case_folder(
                    case_handler=case_handler,
                    case_type=case_metadata["caseType"],
                    person_full_name=person_full_name,
                    person_go_id=person_go_id,
                    ssn=ssn,
                    conn_string=credentials["DbConnectionString"],
                    update_response_data=case_metadata["spUpdateResponseData"],
                    update_process_status=case_metadata["spUpdateProcessStatus"],
                    process_status_params_failed=status_params_failed,
                    form_id=form_id,
                )
            except Exception as e:
                message = "Error creating citizen folder."
                handle_error(
                    message=message,
                    case_metadata=case_metadata,
                    error=e,
                    process_name=process_name,
                    credentials=credentials,
                    form_id=form_id,
                    db_env=db_env,
                    form=form,
                )
                # continue

    log_event(
        LOG_DB,
        "INFO",
        "Creating case.",
        context=f"{LOG_CONTEXT} ({process_name})",
        db_env=db_env,
    )
    case_data = json.loads(case_metadata["caseData"])
    try:
        case_id, case_title, case_rel_url = jp.create_case(
            case_handler=case_handler,
            parsed_form_data=parsed_form_data,
            os2form_webform_id=os2formwebform_id,
            case_type=case_metadata["caseType"],
            case_data=case_data,
            conn_string=credentials["DbConnectionString"],
            update_response_data=case_metadata["spUpdateResponseData"],
            update_process_status=case_metadata["spUpdateProcessStatus"],
            process_status_params_failed=status_params_failed,
            form_id=form_id,
            ssn=ssn,
            person_full_name=person_full_name,
            case_folder_id=case_folder_id,
        )
        log_event(
            LOG_DB,
            "INFO",
            f"Case created with id: {case_id}",
            context=f"{LOG_CONTEXT} ({process_name})",
            db_env=db_env,
        )
    except Exception as e:
        message = f"Error creating case: {e}"
        handle_error(
            message=message,
            case_metadata=case_metadata,
            error=e,
            process_name=process_name,
            credentials=credentials,
            form_id=form_id,
            db_env=db_env,
            form=form,
        )
        # continue

    log_event(LOG_DB, "INFO", "Journalizing file(s).", context=f"{LOG_CONTEXT} ({process_name})", db_env=db_env)
    try:
        jp.journalize_file(
            document_handler=document_handler,
            case_id=case_id,
            case_title=case_title,
            case_rel_url=case_rel_url,
            parsed_form_data=parsed_form_data,
            os2_api_key=credentials["os2_api_key"],
            conn_string=credentials["DbConnectionString"],
            process_status_params_failed=status_params_failed,
            form_id=form_id,
            case_metadata=case_metadata,
            log_db=LOG_DB,
            process_name=process_name,
            db_env=db_env,
        )
    except Exception as e:
        message = f"Error journalizing files. {e}"
        handle_error(
            message=message,
            case_metadata=case_metadata,
            error=e,
            process_name=process_name,
            credentials=credentials,
            form_id=form_id,
            db_env=db_env,
            form=form,
        )
        # continue

    execute_stored_procedure(
        credentials["DbConnectionString"],
        case_metadata["spUpdateProcessStatus"],
        status_params_success,
    )
    log_event(
        LOG_DB,
        "INFO",
        f"Ending journalizing - {form_id = }, {form_submitted_date = }, {os2formwebform_id = }",
        context=f"{LOG_CONTEXT} ({process_name})",
        db_env=db_env,
    )


def handle_error(
    message,
    case_metadata,
    error,
    process_name,
    credentials,
    form_id,
    db_env="PROD",
    form=None,
):
    """Function to log and notify on errors"""
    _, _, status_params_failed, _ = (
        get_status_params(form_id)
    )
    # Log
    log_event(
        LOG_DB,
        "ERROR",
        message,
        context=f"{LOG_CONTEXT} ({process_name})",
        db_env=db_env,
    )
    # Update status
    execute_stored_procedure(
        credentials["DbConnectionString"],
        case_metadata["spUpdateProcessStatus"],
        status_params_failed,
    )
    # Notify
    notify_stakeholders(
        case_metadata=case_metadata,
        case_id=None,
        case_title=None,
        case_rel_url=None,
        error_message=f"{message}: {error}",
        attachment_bytes=None,
        form=form,
        db_env=db_env,
    )
    raise Exception from error


def get_status_params(form_id: str):
    """
    Generates a set of status parameters for the process, based on the given form_id and JSON arguments.

    Args:
        form_id (str): The unique identifier for the current process.
        case_metadata (dict): A dictionary containing various process-related arguments, including table names.

    Returns:
        tuple: A tuple containing three dictionaries:
            - status_params_inprogress: Parameters indicating that the process is in progress.
            - status_params_success: Parameters indicating that the process completed successfully.
            - status_params_failed: Parameters indicating that the process has failed.
    """
    status_params_inprogress = {
        "Status": ("str", "InProgress"),
        "form_id": ("str", f"{form_id}"),
    }
    status_params_success = {
        "Status": ("str", "Successful"),
        "form_id": ("str", f"{form_id}"),
    }
    status_params_failed = {
        "Status": ("str", "Failed"),
        "form_id": ("str", f"{form_id}"),
    }
    status_params_manual = {
        "Status": ("str", "Manual"),
        "form_id": ("str", f"{form_id}"),
    }
    return status_params_inprogress, status_params_success, status_params_failed, status_params_manual


def extract_ssn(os2formwebform_id, parsed_form_data):
    """
    Extracts the Social Security Number (SSN) from the parsed form data based on the provided webform ID.

    Args:
        os2formwebform_id (str): A string containing the webform ID.
        parsed_form_data (dict): A dictionary containing the parsed form data, including potential SSN fields.

    Returns:
        str or None: The extracted SSN as a string with hyphens removed,
            or None if the SSN is not present in the form data.
    """
    match os2formwebform_id:
        case (
            "indmeldelse_i_modtagelsesklasse"
            | "ansoegning_om_koersel_af_skoleel"
            | "ansoegning_om_midlertidig_koerse"
        ):
            if "cpr_barnets_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["cpr_barnets_nummer"].replace("-", "")
            if "barnets_cpr_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["barnets_cpr_nummer"].replace("-", "")
            if "cpr_elevens_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["cpr_elevens_nummer"].replace("-", "")
            if "elevens_cpr_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["elevens_cpr_nummer"].replace("-", "")
            if "cpr_barnet" in parsed_form_data["data"]:
                return parsed_form_data["data"]["cpr_barnet"].replace("-", "")
            # TEST webform_id'er. Prod id i journalize_process.py
        case "tilmelding_til_modersmaalsunderv":
            if (
                parsed_form_data["data"]["elevens_cpr_nummer_mitid"] != ""
            ):  # Hvis cpr kommer fra MitID
                return parsed_form_data["data"]["elevens_cpr_nummer_mitid"].replace(
                    "-", ""
                )
            if (
                parsed_form_data["data"]["elevens_cpr_nummer"] != ""
            ):  # Hvis cpr er indtastet manuelt
                return parsed_form_data["data"]["elevens_cpr_nummer"].replace("-", "")
        case "anmeldelse_af_hjemmeundervisning":
            if (
                parsed_form_data["data"]["barnets_cpr_nummer_mitid"] != ""
            ):  # Hvis cpr kommer fra MitID
                return parsed_form_data["data"]["barnets_cpr_nummer_mitid"].replace(
                    "-", ""
                )
            if (
                parsed_form_data["data"]["cpr_barnets_nummer_"] != ""
            ):  # Hvis cpr er indtastet manuelt
                return parsed_form_data["data"]["cpr_barnets_nummer_"].replace("-", "")
        case "pasningstid":
            if (
                parsed_form_data["data"]["barnets_cpr_nummer"] != ""
            ):  # Hvis cpr kommer fra MitID
                return parsed_form_data["data"]["barnets_cpr_nummer"].replace("-", "")
            if (
                parsed_form_data["data"]["cpr_barnets_nummer_"] != ""
            ):  # Hvis cpr er indtastet manuelt
                return parsed_form_data["data"]["cpr_barnets_nummer_"].replace("-", "")
        case "skriv_dit_barn_paa_venteliste":
            if parsed_form_data['data']['barnets_cpr_nummer_mitid'] != '':  # Hvis cpr kommer fra MitID
                return parsed_form_data['data']['barnets_cpr_nummer_mitid'].replace('-', '')
            if parsed_form_data['data']['cpr_barnets_nummer_'] != '':
                return parsed_form_data['data']['cpr_barnets_nummer_'].replace('-', '')
        case _:
            return None
