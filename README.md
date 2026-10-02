# MBU_Journalisering_service

Journaliseringskomponent som Windows service.

A Windows service that picks up OS2Forms submissions from the RPA SQL database and
journalizes them into GetOrganized (GO): it looks up the citizen, finds or creates
the right case (folder), uploads the form's documents, optionally marks them as
case records / finalizes them, and notifies stakeholders by email.

## How it works

1. **Service loop** (`service.py`) – `JournalizeService` (`JournalizeToGetOrganized`)
   is a `pywin32` service. On start it spawns a heartbeat subprocess, then loops:
   - loads credentials/constants from the RPA database (`jp.get_credentials_and_constants`),
   - loads active form configurations from `[RPA].[journalizing].[Metadata]` (`fetch_cases_metadata`),
   - fetches pending forms for the webform types in `HANDLE_FORMS` (`jp.get_forms_data`),
   - runs `process.main_process` for each form in a `ThreadPoolExecutor` (`MAX_WORKERS`),
     or sleeps `FETCH_INTERVAL` seconds if there is nothing to do.
2. **Per-form process** (`process.py` → `main_process`):
   1. Sets the form status to `InProgress` (this increments `attempt_count`).
   2. Runs a GO API health check.
   3. Extracts the child's/student's CPR from the form data (`extract_ssn`, field names differ per webform).
   4. Finds or creates the case, depending on the `caseType` in the metadata:
      - **`BOR`**: contact lookup → find the citizen folder (Borgermappe) or create one → create a case in it.
        If a `BOR-YYYY-NNNNNN` folder exists but has the wrong caseCategory, an email alert is sent.
      - **`PPR`** (school transport/befordring, `case_manager/ppr_journalization.py`): contact lookup →
        find an existing *"Kørsel til …"* sub-case and/or the parent `PPR-YYYY-NNNNNN` case.
        A closed parent case is reopened; a missing sub-case, or both cases, are created.
      - **`indmeldelse_i_modtagelsesklasse`**: reuses a case with a *Kvitteringmodtagelsesklasse*
        receipt from the last 3 months and adds a numeric suffix to the filenames.
      - **Respekt for grænser** forms (`respekt_for_graenser`, `respekt_for_graenser_privat`,
        `indmeld_kraenkelser_af_boern`) need no CPR. The case title and case profile are based on the
        `omraade` field (Skole / Dagtilbud / Ungdomsskole / Klub).
   5. Downloads each attachment from OS2Forms, uploads it to the case, and marks it as a case record
      and/or finalizes it, depending on `documentData`.
   6. Sets the status to `Successful` and logs the result.
3. **Error handling / retries** – each step writes its result to the response JSON via the configured
   stored procedures (`spUpdateResponseData`, `spUpdateProcessStatus`). A failure before a case exists is
   retried by a later loop, up to `MAX_FORM_RETRIES` and after `FORM_RETRY_WAIT_*_MINUTES`. When the
   retries run out, or for the critical Respekt for grænser forms, an error email is sent (critical forms
   also go to the RPA team).

## Project layout

| Path | Purpose |
| --- | --- |
| `service.py` | Windows service entry point (install/start/stop via `win32serviceutil`). |
| `process.py` | `main_process` – the per-form journalization flow, error handling, CPR extraction. |
| `case_manager/journalize_process.py` | DB access (forms, credentials), health check, contact lookup, case/folder creation, file journalization. |
| `case_manager/ppr_journalization.py` | PPR/befordring-specific case search, creation and reopening. |
| `case_manager/case_handler.py` | `CaseHandler` – thin wrapper around the GO case/contact API (`mbu_dev_shared_components.getorganized`). |
| `case_manager/document_handler.py` | `DocumentHandler` – GO document upload, journalize, finalize, search. |
| `case_manager/helper_functions.py` | Attachment URL extraction, metadata fetch, stakeholder email notifications. |
| `test_single_form.py` | Manual script that runs one form (`FORM_ID`) through `main_process` against **PROD**. |
| `utils.py` | Legacy heartbeat helper (not used by the service). |

## Configuration

### `config.py` (not in git)

The code imports settings from a `config.py` in the project root. This file is git-ignored and must
be created locally. It must define:

| Name | Used for |
| --- | --- |
| `ENV` | DB environment passed to `RPAConnection` (e.g. `"PROD"`). |
| `HANDLE_FORMS` | List of OS2Forms webform IDs the service should process. |
| `FETCH_INTERVAL` | Seconds to sleep when there are no new forms. |
| `MAX_WORKERS` | Number of forms processed in parallel. |
| `MAX_FORM_RETRIES` | Max attempts before an error email is sent. |
| `FORM_RETRY_WAIT_HEALTH_CHECK_MINUTES` | Delay before retrying a form that failed the health check. |
| `FORM_RETRY_WAIT_CONTACT_LOOKUP_MINUTES` | Delay before retrying a form that failed after the health check. |
| `SERVICE_CHECK_INTERVAL` | Heartbeat interval. |
| `LOG_DB`, `LOG_CONTEXT` | Log table/database and context name used for logging. |
| `PATH_TO_PYTHONSERVICE` | Path to `pythonservice.exe` (used when not frozen). |
| `HEARTBEAT_INTERVAL` | Only used by `utils.py`. |

### Database

Credentials and constants are read at runtime through `RPAConnection`:

- constants: `go_api_endpoint`, `DbConnectionString`, `journalizing_tmp_path`, `Error Email`,
  `e-mail_noreply`, `smtp_server`, `smtp_port`, `rpa_team_email`
- credentials: `go_api`, `os2_api`

Per-form behaviour is configured in `[RPA].[journalizing].[Metadata]` (`isActive = 1`) with the columns
`os2formWebformId`, `description`, `caseType`, `spUpdateResponseData`, `spUpdateProcessStatus`,
`caseData` (JSON: case owner, profile, KLE, facet, `meta_case_title`, `emailRecipient`, …) and
`documentData` (JSON: `documentCategory`, `useCompletedDateFromFormAsDate`, `journalizeDocuments`,
`finalizeDocuments`). Forms are read from `[RPA].[journalizing].[Journalizing]` and
`[RPA].[journalizing].[Forms]`.

## Development setup

The project uses [uv](https://docs.astral.sh/uv/). Dependencies are declared in `pyproject.toml`
(`requirements.txt` is kept for the existing deployment).

```bash
uv venv .venv
uv sync
```

Notes:

- `pywin32` is only installed on Windows (see `override-dependencies` in `pyproject.toml`). That means
  `service.py` only runs on Windows, but the other modules can be installed and linted on Linux.
- `pyodbc` needs an ODBC driver manager and a SQL Server ODBC driver. On Linux that means `unixodbc`
  and Microsoft's `msodbcsql18`.
- Lint with the repo's `.pylintrc`.

## Running

On the Windows server, from an elevated prompt with the environment activated:

```powershell
python service.py install
python service.py start
python service.py stop
python service.py remove
python service.py debug   # run in the foreground for troubleshooting
```

To debug a single submission, set `FORM_ID` in `test_single_form.py` and run it. **It runs against the
PROD database and GetOrganized**, so it creates real cases and documents.
