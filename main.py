import os
import time
import pandas as pd
from datetime import datetime, date
from babel.numbers import format_decimal
from jobspy import scrape_jobs
from notion_client import Client
import logging

from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Notion configuration (replace with your credentials)
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

# Initialize Notion client
notion = Client(auth=NOTION_API_KEY)


def fetch_notion_databases():
    """fetch database"""
    databases = notion.search(query="")
    print(databases)
    return databases


def create_notion_database():
    """create database of notion for first time"""
    database_properties = {
        "ID": {"rich_text": {}},  # String type (rich text) for ID
        "Site": {"rich_text": {}},  # String type (rich text) for Site
        "Job Url": {"url": {}},  # Url type for Job Url
        "Direct Job Url": {"url": {}},  # Url type for Direct Job Url
        "Title": {"title": {}},  # Title type (required) for the Job title
        "Company": {"rich_text": {}},  # String type (rich text) for Company
        "Location": {"rich_text": {}},  # String type (rich text) for Location
        "Date Posted": {"date": {}},  # Date type for Date Posted
        "Job Type": {"rich_text": {}},  # String type (rich text) for Job Type
        "Salary Source": {"rich_text": {}},  # String type (rich text) for Salary Source
        "Salary Range": {"rich_text": {}},  # String type (rich text) for Salary Range
        "Is Remote": {"checkbox": {}},  # Checkbox type for Is Remote (boolean)
        "Job Level": {"rich_text": {}},  # String type (rich text) for Job Level
        "Job Function": {"rich_text": {}},  # String type (rich text) for Job Function
        "Listing Type": {"rich_text": {}},  # String type (rich text) for Listing Type
        "Emails": {"rich_text": {}},  # String type (rich text) for Emails
        "Description": {"rich_text": {}},  # String type (rich text) for Description
        "Company Industry": {
            "rich_text": {}
        },  # String type (rich text) for Company Industry
        "Company Url": {"url": {}},  # Url type for Company Url
        "Company Logo": {"url": {}},  # Url type for Company Logo
        "Direct Company Url": {"url": {}},  # Url type for Direct Company Url
        "Company Addresses": {
            "rich_text": {}
        },  # String type (rich text) for Company Addresses
        "Company Size": {"rich_text": {}},  # String type (rich text) for Company Size
        "Company Revenue": {
            "rich_text": {}
        },  # String type (rich text) for Company Revenue
        "Company Description": {
            "rich_text": {}
        },  # String type (rich text) for Company Description
        "Created Time": {"date": {}},
    }

    new_database = notion.databases.create(
        parent={"type": "page_id", "page_id": ""},
        title=[
            {"type": "text", "text": {"content": "Job Listings"}}
        ],  # The name of the database
        properties=database_properties,
    )

    print(new_database)


# create_notion_database()
# fetch_notion_databases()


def fetch_jobs(search_terms, location, results_wanted=20, hours_old=72):
    """Fetch jobs for multiple search terms and locations."""
    all_jobs = []
    for term in search_terms:
        try:
            logging.info("Scraping jobs for '%s' in '%s'...", term, location)
            jobs = scrape_jobs(
                site_name=[
                    "indeed",
                    "linkedin",
                    "zip_recruiter",
                    "glassdoor",
                    "google",
                ],
                search_term=term,
                location=location,
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed="india",  # For India
            )
            if not jobs.empty:
                logging.info("Found %d jobs for '%s'", len(jobs), term)
                all_jobs.append(jobs)
            else:
                logging.warning("No jobs found for '%s'", term)
        except Exception as e:
            logging.error("Error fetching jobs for '%s': %s", term, e)

    return pd.concat(all_jobs, ignore_index=True) if all_jobs else pd.DataFrame()


def prepare_properties(row):
    properties = {}

    # Helper function to create rich text content
    def create_rich_text(value):
        if value and value != "No Value":
            # if characters in value are grateer than 1000 then it will be truncated
            if len(value) > 1000:
                value = value[:1000]
            return {"rich_text": [{"text": {"content": str(value)}}]}
        return {"rich_text": []}

    # Helper function to create URL content
    def create_url(value):
        if value and value != "No Value" and value != "":
            return {"url": value}
        return {"url": None}  # Use None instead of empty string for URLs

    def format_currency(value):
        try:
            return format_decimal(value, locale='en_IN')
        except ValueError:
            return str(value)  # Fallback in case of formatting issues

    def create_date(value):
        if value and value != "No Value":
            # Check if the value is already a datetime.date object or datetime.datetime
            if isinstance(value, datetime):
                # If it's a datetime object, use isoformat directly
                return {"date": {"start": value.isoformat()}}
            elif isinstance(value, date):
                # If it's a date object, convert to string using isoformat
                return {"date": {"start": value.isoformat()}}
            elif isinstance(value, str):
                try:
                    # If it's a string, try to parse it in the format 'YYYY-MM-DD'
                    parsed_date = datetime.strptime(value, "%Y-%m-%d")
                    return {"date": {"start": parsed_date.isoformat()}}
                except ValueError:
                    logging.warning("Invalid date format: %s", value)
        
        # If none of the above conditions are met, return current date
        return {"date": {"start": datetime.now().isoformat()}}

    # Map the row data to Notion properties
    properties = {
        "ID": create_rich_text(row.get("id")),
        "Site": create_rich_text(row.get("site")),
        "Job Url": create_url(row.get("job_url")),
        "Direct Job Url": create_url(row.get("job_url_direct")),
        "Title": {"title": [{"text": {"content": str(row.get("title", "No Title"))}}]},
        "Company": create_rich_text(row.get("company")),
        "Location": create_rich_text(row.get("location")),
        "Date Posted": create_date(row.get("date_posted")),
        "Job Type": create_rich_text(row.get("job_type")),
        "Salary Source": create_rich_text(row.get("salary_source")),
        "Is Remote": {"checkbox": bool(row.get("is_remote", False))},
        "Job Level": create_rich_text(row.get("job_level")),
        "Job Function": create_rich_text(row.get("job_function")),
        "Listing Type": create_rich_text(row.get("listing_type")),
        "Emails": create_rich_text(row.get("emails")),
        "Description": create_rich_text(row.get("description")),
        "Company Industry": create_rich_text(row.get("company_industry")),
        "Company Url": create_url(row.get("company_url")),
        "Company Logo": create_url(row.get("company_logo")),
        "Direct Company Url": create_url(row.get("company_url_direct")),
        "Company Addresses": create_rich_text(row.get("company_addresses")),
        "Company Size": create_rich_text(row.get("company_num_employees")),
        "Company Revenue": create_rich_text(row.get("company_revenue")),
        "Company Description": create_rich_text(row.get("company_description")),
        "Created Time": {"date": {"start": datetime.now().isoformat()}},
    }

    # Handle salary range separately
    if all(key in row for key in ["currency", "min_amount", "max_amount", "interval"]):
        salary_range = f"{row['currency']} {format_currency(row['min_amount'])} - {format_currency(row['max_amount'])} ({row['interval']})"
        properties["Salary Range"] = create_rich_text(salary_range)
    else:
        properties["Salary Range"] = create_rich_text("No Salary Info")

    return properties


def check_job_exists(job_data):
    """
    Check if a job already exists in the database using unique identifiers
    """
    # Create a unique identifier based on multiple fields
    # unique_identifier = f"{job_data.get('title', '')}-{job_data.get('company', '')}-{job_data.get('job_url', '')}"

    # Query the database for matching entries
    query = notion.databases.query(
        database_id=NOTION_DATABASE_ID,
        filter={
            "and": [
                {"property": "Title", "title": {"equals": job_data.get("title", "")}},
                {
                    "property": "Company",
                    "rich_text": {"equals": job_data.get("company", "")},
                },
                {"property": "Job Url", "url": {"equals": job_data.get("job_url", "")}},
            ]
        },
    )

    return len(query["results"]) > 0


def append_to_notion(df, notion_client, database_id):
    """Append job listings to a Notion database."""
    added_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        time.sleep(5)  # Sleep for 1 second to avoid rate limiting
        try:
            # Convert row to dictionary for easier handling
            job_data = row.to_dict()

            # Check if job already exists
            if check_job_exists(job_data):
                logging.info(
                    "Skipping duplicate job: %s at %s", job_data['title'], job_data['company']
                )
                skipped_count += 1
                continue

            logging.info("Adding job: %s at %s...", job_data['title'], job_data['company'])
            properties = prepare_properties(row)
            notion_client.pages.create(
                parent={"database_id": database_id}, properties=properties
            )
            logging.info("Successfully added: %s", job_data['title'])
            added_count += 1

        except Exception as e:
            logging.error("Error processing job: %s", e)

    logging.info(
        "Operation completed. Added %d jobs, skipped %d duplicates.",
        added_count, skipped_count
    )


def sanitize_dataframe(df):
    """Sanitize the dataframe to ensure all fields are JSON-compliant."""
    # Replace NaN or None with empty strings
    df.fillna("", inplace=True)

    # Ensure all numeric fields are valid
    numeric_columns = ["min_amount", "max_amount"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Ensure boolean fields are valid
    boolean_columns = ["is_remote"]
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].astype(bool)

    return df


def main():
    """Main script to fetch jobs and append them to Notion."""
    # Define search terms and location
    search_terms = ["Software Engineer", "Backend Developer", "Backend Engineer", "SDE"]
    location = "India"

    # Fetch jobs
    jobs_df = fetch_jobs(search_terms, location)
    # jobs_df = pd.read_csv("jobs.csv")

    if not jobs_df.empty:
        # Save jobs to CSV for backup
        jobs_df = sanitize_dataframe(jobs_df)
        jobs_df.to_csv("jobs.csv", index=False)
        # logging.info("Jobs saved to jobs.csv")

        # Append jobs to Notion
        append_to_notion(jobs_df, notion, NOTION_DATABASE_ID)
    else:
        logging.warning("No jobs found to append to Notion.")


if __name__ == "__main__":
    main()
