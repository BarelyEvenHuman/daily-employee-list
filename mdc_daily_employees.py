import boto3
import json
import requests
import os
import pandas as pd
import phonenumbers as pn
import snowflake.connector
from copy import deepcopy
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError


NSL_API = os.environ["NSL_API"]
NSL_TOKEN_ENDPOINT = os.environ["NSL_TOKEN_ENDPOINT"]
SNOWFLAKE_SECRETS = os.environ["SNOWFLAKE_SECRETS"]


logger = Logger(service="mdc-daily-empl-dev-us-east-1")
logger.append_keys(id="")
logger.append_keys(employee_id="")


patient_template = {
    "id": "",
    "personal": {
        "first_name": "",
        "last_name": "",
        "dob": "",
        "ethnicity": "Prefer not to say",
        "race": "Prefer not to say",
        "gender": "U",
    },
    "contact": {"email": "", "phone": ""},
    "address": {
        "street_1": "N/A",
        "city": "N/A",
        "state": "FL",
        "postal_code": "N/A",
        "country": "US",
    },
    "employee_id": "",
    "source": "mdc_employee_roster",
}


patientId_template = {"type": "patient"}


def query_mdc(cursor):
    query1 = """create or replace temporary table tiger_dev.client_reporting.MDC_Current(
        employee_id integer,\
        first_name string,\
        last_name string,\
        dob date,\
        home_email string,\
        home_phone string,\
        work_email string,\
        work_phone string,\
        work_ext string,\
        cell_phone string,\
        work_cell string,\
        weekly_test_flag string,\
        roster_date date);"""
    cursor.execute(query1)

    query2 = """
        insert into tiger_dev.client_reporting.MDC_Current(
        select try_cast(value:"c1"::string AS integer) as employee_id,\
        value:"c2"::string as first_name,\
        value:"c3"::string as last_name,\
        try_to_date(value:"c4"::string,'MM/DD/YYYY') as dob,\
        value:"c5"::string as home_email,\
        value:"c6"::string as home_phone,\
        value:"c7"::string as work_email,\
        value:"c8"::string as work_phone,\
        value:"c9"::string as work_ext,\
        value:"c10"::string as cell_phone,\
        value:"c11"::string as work_cell,\
        value:"c11"::string as weekly_test_flag,\
        try_to_date(regexp_substr(metadata$filename,'[0-9]{2}\-[0-9]{2}\-[0-9]{4}'),'MM-DD-YYYY') as roster_date\
        from tiger_dev.client_reporting.mdc_employee_master\
        where roster_date = current_date()\
        order by roster_date desc);"""
    cursor.execute(query2)

    query3 = """create or replace temporary table tiger_dev.client_reporting.MDC_less_one(
        employee_id integer,\
        first_name string,\
        last_name string,\
        dob date,\
        home_email string,\
        home_phone string,\
        work_email string,\
        work_phone string,\
        work_ext string,\
        cell_phone string,\
        work_cell string,\
        weekly_test_flag string,\
        roster_date date);"""
    cursor.execute(query3)

    query4 = """
        insert into tiger_dev.client_reporting.MDC_less_one(
        select try_cast(value:"c1"::string AS integer) as employee_id,\
        value:"c2"::string as first_name,\
        value:"c3"::string as last_name,\
        try_to_date(value:"c4"::string,'MM/DD/YYYY') as dob,\
        value:"c5"::string as home_email,\
        value:"c6"::string as home_phone,\
        value:"c7"::string as work_email,\
        value:"c8"::string as work_phone,\
        value:"c9"::string as work_ext,\
        value:"c10"::string as cell_phone,\
        value:"c11"::string as work_cell,\
        value:"c11"::string as weekly_test_flag,\
        try_to_date(regexp_substr(metadata$filename,'[0-9]{2}\-[0-9]{2}\-[0-9]{4}'),'MM-DD-YYYY') as roster_date\
        from tiger_dev.client_reporting.mdc_employee_master\
        where roster_date = current_date() -1\
        order by roster_date desc);"""
    cursor.execute(query4)

    query5 = """
        (select employee_id, first_name,last_name,dob, home_email,home_phone,\
            work_email, work_phone, work_ext, cell_phone, work_cell, weekly_test_flag\
        from tiger_dev.client_reporting.MDC_Current\
    except\
    select employee_id, first_name,last_name,dob, home_email,home_phone,\
            work_email, work_phone, work_ext, cell_phone, work_cell, weekly_test_flag\
    from tiger_dev.client_reporting.mdc_less_one)\
    order by employee_id desc;"""
    cursor.execute(query5)
    df = cursor.fetch_pandas_all()
    df["DOB"] = pd.to_datetime(df["DOB"], errors="coerce")
    df["DOB"] = df["DOB"].dt.strftime("%Y-%m-%d")
    record_list = json.loads(df.to_json(orient="records"))
    return record_list


def snomi_connection(secrets):
    conn = snowflake.connector.connect(
        user=secrets["SNOMIUSER"],
        password=secrets["SNOMIPASS"],
        account=secrets["SNOMIACCOUNT"],
        database="TIGER_DEV",
        schema="client_reporting",
        warehouse="aws_svc_s_wh",
        role="MDC_SUPPORT",
    )
    return conn


def get_secrets(sm):
    """Retrieves secrets for Snowflake credentials."""
    return json.loads(sm.get_secret_value(SecretId=SNOWFLAKE_SECRETS)["SecretString"])


def employee_id(record_list):
    return record_list["EMPLOYEE_ID"]


def format_phone(item, phone_key):
    """Formats phone numbers to be +1xxxxxxxxxx"""
    try:
        phone = pn.format_number(
            pn.parse(item.get(phone_key), "US"), pn.PhoneNumberFormat.E164
        )
        return phone
    except:
        return item.get(phone_key)


def rename_keys(parsed, patient_template, bearer_token):
    patient_copy = deepcopy(patient_template)
    patient_copy["id"] = create_patient_id(patientId_template, bearer_token)
    patient_copy["employee_id"] = str(parsed["EMPLOYEE_ID"])
    patient_copy["personal"]["first_name"] = parsed["FIRST_NAME"]
    patient_copy["personal"]["last_name"] = parsed["LAST_NAME"]
    patient_copy["personal"]["dob"] = parsed["DOB"]
    patient_copy["contact"]["phone"] = parsed["HOME_PHONE"]
    if parsed["HOME_EMAIL"]:
        patient_copy["contact"]["email"] = parsed["HOME_EMAIL"]
    else:
        patient_copy["contact"]["email"] = parsed["WORK_EMAIL"]
    if parsed["HOME_PHONE"]:
        patient_copy["contact"]["phone"] = format_phone(parsed, "HOME_PHONE")
    else:
        patient_copy["contact"]["phone"] = format_phone(parsed, "WORK_PHONE")
    return patient_copy


def rename_keys_put(records, patients_to_put, patient_template):
    patient_copy = deepcopy(patient_template)
    patient_copy["id"] = patients_to_put
    patient_copy["employee_id"] = str(records["EMPLOYEE_ID"])
    patient_copy["personal"]["first_name"] = records["FIRST_NAME"]
    patient_copy["personal"]["last_name"] = records["LAST_NAME"]
    patient_copy["personal"]["dob"] = records["DOB"]
    patient_copy["contact"]["phone"] = records["HOME_PHONE"]
    if records["HOME_EMAIL"]:
        patient_copy["contact"]["email"] = records["HOME_EMAIL"]
    else:
        patient_copy["contact"]["email"] = records["WORK_EMAIL"]
    if records["HOME_PHONE"]:
        patient_copy["contact"]["phone"] = format_phone(records, "HOME_PHONE")
    else:
        patient_copy["contact"]["phone"] = format_phone(records, "WORK_PHONE")
    return patient_copy


def get_token():
    """Request for getting back bearer token to call NSL API."""
    logger.info("Retrieving Bearer Token...")
    try:
        response = requests.post(
            NSL_TOKEN_ENDPOINT,
            data=f"grant_type=client_credentials&scope={NSL_API}/general_scope",
            headers={
                "Authorization": os.environ["authorization"],
                "Content-Type": "application/x-www-form-urlencoded",
            },
            cookies={
                "XSRF-TOKEN": os.environ["xsrf_token"],
            },
            timeout=15,
        )
        return json.loads(response.text)
    except Exception as e:
        logger.error(f"Bearer Token Error: {str(e)}")


def check_employee_exists(employee_id, record_list, bearer_token):
    """Calls NSL API to check if employee exists and puts emp_id in a list. If does
    not exist, data will be inserted via tiger API."""
    exists = []
    logger.append_keys(employee_id=employee_id)
    try:
        response = requests.get(
            f"{NSL_API}/api/v1/patients/search?employee_id={employee_id}",
            data=json.dumps(patientId_template),
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token["access_token"],
            },
            timeout=15,
        )
        if response.text != "null":
            patient_id = response.text[8:20]
            exists.append({employee_id: patient_id})
            logger.info(f"Patient_ID found: {patient_id}")
            return exists[0]
        else:
            formatted = rename_keys(record_list, patient_template, bearer_token)
            insert_mdc_empl(formatted, bearer_token)
    except Exception as e:
        logger.info(f"Could not create patient ID. Error: {e}")


def insert_mdc_empl(formatted, bearer_token):
    """Calls NSL API to create patient from MDC employees, will retry 5 times."""
    logger.append_keys(employee_id=formatted["employee_id"])
    logger.append_keys(id=formatted["id"])
    for _ in range(5):
        try:
            response = requests.post(
                f"{NSL_API}/api/v1/patients",
                data=json.dumps(formatted),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": bearer_token["access_token"],
                },
            )
            if response.status_code == 202:
                logger.info(f"MDC Employee Successfully Inserted.")
                break
            elif response.status_code == 200 and response.text.find(
                "patient already exists"
            ):
                logger.info(f"Patient already exists. {response.text}")
                break
            elif response.status_code == 400 and response.text.find(
                "Invalid request body"
            ):
                logger.warning(
                    f"Patient's data missing required fields. Error:{response.text}"
                )
                break
            else:
                logger.warning(
                    f"Status: {response.status_code}, Error:{response.text}. Retry Count: {_}"
                )
        except Exception as e:
            logger.error(f"Error: {e}, EmployeeID: {formatted['employee_id']}")
    return response


def put_mdc_empl(final_records, bearer_token):
    """Calls NSL API to create patient from MDC employee data."""
    logger.append_keys(employee_id=final_records["employee_id"])
    logger.append_keys(id=final_records["id"])
    try:
        response = requests.put(
            f"{NSL_API}/api/v1/patients/{final_records['id']}",
            data=json.dumps(final_records),
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token["access_token"],
            },
        )
        if response.status_code == 202:
            logger.info(f"Patient Record updated. {response.text}")
            return response
        else:
            logger.warning(f"Status: {response.status_code}, Error:{response.text}.")
            return response
    except Exception as e:
        logger.error(f"Error: {e}, EmployeeID: {final_records['employee_id']}")


def create_patient_id(patientId_template, bearer_token):
    """Calls NSL API to create patient ID ."""
    for _ in range(5):
        try:
            response = requests.post(
                f"{NSL_API}/api/v1/ids",
                data=json.dumps(patientId_template),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": bearer_token["access_token"],
                },
                timeout=15,
            )
            if response.status_code in (200, 201):
                return response.text[7:-2]
            else:
                logger.warning(
                    f"Status: {response.status_code}, Error:{response.text}. Retry Count: {_}"
                )
        except Exception as e:
            logger.info(f"Could not create patient ID. Error: {e}")


def return_patients(record_list, employees_to_update):
    """Returns list of patient_ids and drops any None types if
    any were returned in the api call from check_employee_exists."""
    patient_ids = []
    for i, record in enumerate(record_list):
        try:
            ids = [x for x in employees_to_update[i]]
            if employees_to_update[i] is None:
                pass
            elif record["EMPLOYEE_ID"] == ids[0]:
                patient_ids.append(employees_to_update[i].get(record["EMPLOYEE_ID"]))
        except Exception:
            pass
    return patient_ids


def records_to_put(record_list, employees_to_update):
    """Returns list of records and drops the None types."""
    records = []
    for i, empl in enumerate(employees_to_update):
        if empl is None:
            pass
        else:
            records.append(record_list[i])
    return records


def lambda_handler(event, context):
    logger.info("MDC Employee list lambda triggered.")
    sm = boto3.client("secretsmanager")
    secrets = get_secrets(sm)
    conn = snomi_connection(secrets)
    cursor = conn.cursor()
    bearer_token = get_token()
    try:
        record_list = query_mdc(cursor)
        record_length = len(record_list)
        logger.info(f"Intial records to process: {record_length}")
        employee_ids = list(map(lambda x: employee_id(x), record_list))
        employees_to_update = list(
            map(
                lambda x, y: check_employee_exists(x, y, bearer_token),
                employee_ids,
                record_list,
            )
        )
        patients_to_put = return_patients(record_list, employees_to_update)
        records = records_to_put(record_list, employees_to_update)
        final_records = list(
            map(
                lambda x, y: rename_keys_put(x, y, patient_template),
                records,
                patients_to_put,
            )
        )
        put_final_records = list(
            map(lambda x: put_mdc_empl(x, bearer_token), final_records)
        )
    except ClientError as e:
        logger.error(e)
    logger.info("Lambda Completed.")
    return json.dumps(put_final_records, default=str)
