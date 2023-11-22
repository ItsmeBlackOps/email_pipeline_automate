import re
from datetime import datetime
import pytz
from dateutil import parser as date_parser
import json
import psycopg2
import pandas as pd
import warnings
import re
warnings.filterwarnings("ignore")


def extract_date_time(normalized_text):
    # Define a pattern to extract the time zone
    timezone_pattern = r"\((.*?)\)"
    timezone_match = re.search(timezone_pattern, normalized_text)
    timezone_str = timezone_match.group(1).strip() if timezone_match else None

    # Extract the date and time string (excluding time zone)
    date_time_pattern = r"Data and Time of Interview \(Mention time zone\):\s*(.*?)\s*Duration"
    date_time_match = re.search(date_time_pattern, normalized_text)
    date_time_str = date_time_match.group(1).strip() if date_time_match else None

    if date_time_str and timezone_str:
        # Combine date and time with timezone
        date_time_with_timezone = f"{date_time_str} ({timezone_str})"

        # Use dateutil.parser to parse the date and time string
        try:
            parsed_date_time = date_parser.parse(date_time_with_timezone, fuzzy=True)
            parsed_date_time = parsed_date_time.astimezone(pytz.UTC)
            date_time = parsed_date_time.isoformat() + 'Z'
        except ValueError:
            date_time = None
    else:
        date_time = None

    return date_time


def handler(pd: "pipedream"):
    # Reference data from previous steps
    # Replace multiple spaces and line breaks with a single space
    text = pd.steps["trim_whitespace_1"]["$return_value"]
    normalized_text = re.sub(r'\s+', ' ', text)
    data_store = pd.inputs['data_store']
    # Trim leading and trailing whitespace
    normalized_text = normalized_text.strip()
    print(normalized_text)
    # Define regular expressions for each field
    candidate_name_pattern = r"Candidate Name:\s*(.*?)\s*Birth date:"
    birth_date_pattern = r"Birth date:\s*(.*?)\s*Gender:"
    gender_pattern = r"Gender:\s*(.*?)\s*Education:"
    education_pattern = r"Education:\s*(.*?)\s*University:"
    university_pattern = r"University:\s*(.*?)\s*Total Experience in Years:"
    experience_pattern = r"Total Experience in Years:\s*(.*?)\s*State:"
    state_pattern = r"State:\s*(.*?)\s*Technology:"
    technology_pattern = r"Technology:\s*(.*?)\s*End Client:"
    end_client_pattern = r"End Client:\s*(.*?)\s*Interview Round"
    interview_round_pattern = r"Interview Round 1st 2nd 3rd or Final round\s*(.*?)\s*Job Title"
    job_title_pattern = r"Job Title in JD:\s*(.*?)\s*Email ID:"
    email_pattern = r"Email ID:\s*(.*?)\s*Personal Contact Number:"
    contact_number_pattern = r"Personal Contact Number:\s*(.*?)\s*Data and Time of Interview \(Mention time zone\):"
    date_time_pattern = r"Data and Time of Interview \(Mention time zone\):\s*(.*?)\s*Duration"
    duration_pattern = r"Duration (\d+\s*\w+)"
    # Extract information using the defined regular expressions
    candidate_name_match = re.search(candidate_name_pattern, normalized_text)
    candidate_name = candidate_name_match.group(1).strip() if candidate_name_match else None

    birth_date_match = re.search(birth_date_pattern, normalized_text)
    birth_date = birth_date_match.group(1).strip() if birth_date_match else None

    gender_match = re.search(gender_pattern, normalized_text)
    gender = gender_match.group(1).strip() if gender_match else None

    education_match = re.search(education_pattern, normalized_text)
    education = education_match.group(1).strip() if education_match else None

    university_match = re.search(university_pattern, normalized_text)
    university = university_match.group(1).strip() if university_match else None

    total_experience_match = re.search(experience_pattern, normalized_text)
    total_experience = total_experience_match.group(1).strip() if total_experience_match else None

    state_match = re.search(state_pattern, normalized_text)
    state = state_match.group(1).strip() if state_match else None

    technology_match = re.search(technology_pattern, normalized_text)
    technology = technology_match.group(1).strip() if technology_match else None

    end_client_match = re.search(end_client_pattern, normalized_text)
    end_client = end_client_match.group(1).strip() if end_client_match else None

    interview_round_match = re.search(interview_round_pattern, normalized_text)
    interview_round = interview_round_match.group(1).strip() if interview_round_match else None
    print(interview_round_pattern)
    job_title_match = re.search(job_title_pattern, normalized_text)
    job_title = job_title_match.group(1).strip() if job_title_match else None
    pattern = r'(\S+@\S+) \[(.*?)\]'

    email_match = re.search(email_pattern, normalized_text)
    email_id = email_match.group(1).strip() if email_match else None
    match = re.match(pattern, email_id)
    if match:
        email_address = match.group(1)

    contact_number_match = re.search(contact_number_pattern, normalized_text)
    contact_number = contact_number_match.group(1).strip() if contact_number_match else None

    duration_match = re.search(duration_pattern, normalized_text)
    duration = duration_match.group(1) if duration_match else None

    # Create a dictionary with the extracted data
    data_dict = {
        "candidate_name": candidate_name,
        "birth_date": birth_date,
        "gender": gender,
        "education": education,
        "university": university,
        "total_experience": total_experience,
        "state_name": state,
        "technology": technology,
        "end_client": end_client,
        "interview_round": interview_round,
        "job_title": job_title,
        "email_id": email_address,
        "contact_number": contact_number,
        "date_time_of_interview": extract_date_time(normalized_text),
        "duration": duration,
        "subject_line": pd.steps["trigger"]["event"]["email"]["subject"],
        "email_received_on": pd.steps["trigger"]["event"]["email"]["receivedDateTime"],
    }

    print(data_dict)
    for i in data_dict:
    # Store a value under a key
        data_store[i] = data_dict[i]
 
    # Connection parameters
    conn_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "vizvacons123",
        "host": "tmsdb.cnqltqgk9yzu.us-east-1.rds.amazonaws.com",  # Change this if your database is hosted elsewhere
        "port": "5432"  # Change this if your PostgreSQL uses a different port
    }
 
    # Establishing a connection to the PostgreSQL database
    conn = psycopg2.connect(**conn_params)
 
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
 
    # Generate the placeholders dynamically for the insertion query
    fields = ', '.join(data_dict.keys())
    placeholders = ', '.join(['%s'] * len(data_dict))
 
    # Construct the dynamic insertion query
    insert_query = f"INSERT INTO email_table ({fields}) VALUES ({placeholders})"
 
    # Extract the values from the data dictionary and convert them to a tuple
    values = tuple(data_dict.values())
 
    # Execute the dynamic insert query with parameterized values
    cursor.execute(insert_query, values)
 
    # Commit your changes in the database
    conn.commit()
 
    # Close the cursor and connection
    cursor.close()
    conn.close()
 







