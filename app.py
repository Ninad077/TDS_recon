import streamlit as st
import pandas as pd
import os
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials  # Correct import for service account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery
import io
from huggingface_hub import HfApi
import json

# Google Drive API Scopes
SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']


st.image("Fynd_logo_hd.jpeg", width=200)


html_title = """
<style>
    .fixed-title {
        font-size: 35px;
        color: #FFFAF0; /* Black color text */
        text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.4), 
                     4px 4px 6px rgba(0, 0, 0, 0.3); /* 3D shadow effect */
        margin: 0; /* Removes padding/margin */
        padding: 0; /* Ensures no extra padding */
    }
</style>
<h1 class="fixed-title">TDS Validation</h1>
"""
st.markdown(html_title, unsafe_allow_html=True)
st.write("")

# File upload UI
html_subject = """
    <html>
    <head>
    <style>
        .button {
            display: inline-block;
            padding: 10px 20px;
            border-radius: 12px;
            background: linear-gradient(to bottom, #f8f9fa, #e0e0e0);
            box-shadow: 
                0 6px 12px rgba(0, 0, 0, 0.3), 
                0 8px 16px rgba(0, 0, 0, 0.2), 
                inset 0 -2px 4px rgba(255, 255, 255, 0.6);
            text-align: center;
            position: relative;
            transform: translateY(4px);
            transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
            cursor: pointer;
            user-select: none;
        }
        .button:hover {
            box-shadow: 
                0 8px 16px rgba(0, 0, 0, 0.3), 
                0 12px 24px rgba(0, 0, 0, 0.2);
            transform: translateY(2px);
        }
        .button:active {
            box-shadow: 
                0 4px 8px rgba(0, 0, 0, 0.3), 
                0 6px 12px rgba(0, 0, 0, 0.2);
            transform: translateY(0);
        }
    </style>
    </head>
    <body>
        <div class="button">
            <h3 style="
                font-size: 20px;
                color: #ffffff;
                background-image: linear-gradient(to right, #000000, #808080);
                background-clip: text;
                -webkit-background-clip: text;
                text-fill-color: transparent;
                -webkit-text-fill-color: transparent;
                margin: 0;
                text-shadow: 0 2px 5px rgba(0, 0, 0, 0.4);
            ">Download the template file</h3>
        </div>
    </body>
    </html>
    """

st.markdown(html_subject, unsafe_allow_html=True)

st.write("")
st.write("")
template_data = pd.read_csv('template.csv')
template_df = pd.DataFrame(template_data)
st.write(template_df)

output = io.BytesIO()
with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    template_df.to_excel(writer, index=False, sheet_name='Template')
output.seek(0)

st.download_button(
    label="Download Template",
    data=output,
    file_name="template.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# Load service account credentials from Hugging Face secrets
def load_gcp_credentials():
    try:
        # Retrieve GCP credentials from the environment variable
        gcp_credentials_str = os.getenv('GCP_CREDENTIALS')
        if not gcp_credentials_str:
            raise ValueError("GCP_CREDENTIALS environment variable not defined")

        # Parse the secret (assuming it's a JSON string)
        gcp_credentials = json.loads(gcp_credentials_str)

        # Save to a temporary file (Google Cloud uses a JSON file for authentication)
        with open("gcp_credentials.json", "w") as f:
            json.dump(gcp_credentials, f)

        # Authenticate using Google Cloud SDK
        credentials_from_file = service_account.Credentials.from_service_account_file("gcp_credentials.json")

        # Return the credentials to be used later
        return credentials_from_file
    except Exception as e:
        print(f"Error retrieving or loading GCP credentials: {str(e)}")
        return None

# Upload to BQ
def upload_to_bigquery(df, table_id):
    try:
        # Load the GCP credentials from Hugging Face secret
        bigquery_creds = load_gcp_credentials()
        if not bigquery_creds:
            st.error("Unable to load GCP credentials.")
            return
        
        # Initialize BigQuery client with the loaded credentials
        client = bigquery.Client(credentials=bigquery_creds)

        # Convert the DataFrame to a list of dictionaries
        records = df.to_dict(orient='records')

        # Prepare the table schema if needed (optional)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # Use WRITE_TRUNCATE to overwrite, WRITE_APPEND to append
        )

        # Load the data to BigQuery
        load_job = client.load_table_from_json(records, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        st.success("Data submitted")

    except Exception as e:
        st.error(f"An error occurred while uploading to BigQuery: {e}")


#Upload to Gdrive
def upload_to_drive(file_path, folder_id):
    try:
        # Authenticate with Google Drive using Hugging Face secrets
        creds = authenticate_google_drive()
        if not creds:
            return

        # Build the Google Drive service
        service = build('drive', 'v3', credentials=creds)

        # Define the file metadata
        file_metadata = {'name': os.path.basename(file_path), 'parents': [folder_id]}
        # Determine MIME type based on file extension
        mime_type = 'application/vnd.ms-excel' if file_path.endswith('.xlsx') else 'text/csv'
        media = MediaFileUpload(file_path, mimetype=mime_type)

        # Upload the file to Google Drive
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        st.write("")
    except Exception as e:
        st.error(f"An error occurred: {e}")
        st.error("Ensure the folder ID is correct and the service account has permission to access the folder.")


# Authenticate Google Drive using credentials
def authenticate_google_drive():
    creds = load_gcp_credentials()
    if not creds:
        st.error("Unable to load GCP credentials for Google Drive authentication.")
        return None
    return creds

# Authenticate BigQuery using credentials
def authenticate_bigquery():
    creds = load_gcp_credentials()
    if not creds:
        st.error("Unable to load GCP credentials for BigQuery authentication.")
        return None
    return creds


# Retrieve the service account credentials from Hugging Face
gcp_credentials = load_gcp_credentials()


if gcp_credentials:
    # Authenticate using the credentials
    creds = authenticate_google_drive()  # Now authenticate without needing file path
    bigquery_creds = authenticate_bigquery()  # Same for BigQuery

    html_subject = """
    <html>
    <head>
    <style>
        .button {
            display: inline-block;
            padding: 10px 20px;
            border-radius: 12px;
            background: linear-gradient(to bottom, #f8f9fa, #e0e0e0);
            box-shadow: 
                0 6px 12px rgba(0, 0, 0, 0.3), 
                0 8px 16px rgba(0, 0, 0, 0.2), 
                inset 0 -2px 4px rgba(255, 255, 255, 0.6);
            text-align: center;
            position: relative;
            transform: translateY(4px);
            transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
            cursor: pointer;
            user-select: none;
        }
        .button:hover {
            box-shadow: 
                0 8px 16px rgba(0, 0, 0, 0.3), 
                0 12px 24px rgba(0, 0, 0, 0.2);
            transform: translateY(2px);
        }
        .button:active {
            box-shadow: 
                0 4px 8px rgba(0, 0, 0, 0.3), 
                0 6px 12px rgba(0, 0, 0, 0.2);
            transform: translateY(0);
        }
    </style>
    </head>
    <body>
        <div class="button">
            <h3 style="
                font-size: 20px;
                color: #ffffff;
                background-image: linear-gradient(to right, #000000, #808080);
                background-clip: text;
                -webkit-background-clip: text;
                text-fill-color: transparent;
                -webkit-text-fill-color: transparent;
                margin: 0;
                text-shadow: 0 2px 5px rgba(0, 0, 0, 0.4);
            ">Upload the TDS file</h3>
        </div>
    </body>
    </html>
    """

    st.markdown(html_subject, unsafe_allow_html=True)

    # Proceed with uploading files to Google Drive or BigQuery
    uploaded_file = st.file_uploader("", type=["csv", "xlsx"])

    if uploaded_file is not None:
        # Get the current directory (same folder as the script)
        current_directory = os.getcwd()
        
        # Save the uploaded file in the current directory
        file_path = os.path.join(current_directory, uploaded_file.name)
        
        # Write the uploaded file to the current directory
        with open(file_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())

        # Process the uploaded file
        if uploaded_file.type == 'application/vnd.ms-excel' or uploaded_file.type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        st.dataframe(df)  # Display the file contents

        button_styles = """
                    <style>
                    div.stButton > button {
                        color:  #000000; /* Text color */
                        font-size: 30px;
                        background-image: linear-gradient(to right, #ffffff, #ffffff); 
                        border: none;
                        padding: 10px 20px;
                        cursor: pointer;
                        border-radius: 15px;
                        display: inline-block;
                        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1), 0 8px 15px rgba(0, 0, 0, 0.1); /* Box shadow */
                        transition: all 0.3s ease; /* Smooth transition on hover */
                    }
                    div.stButton > button:hover {
                        background-color: #00ff00; /* Hover background color */
                        color: #ff0000; /* Hover text color */
                        box-shadow: 0 6px 10px rgba(0, 0, 0, 0.2), 0 12px 20px rgba(0, 0, 0, 0.2); /* Box shadow on hover */
                    }
                    </style>
                """
        st.markdown(button_styles, unsafe_allow_html=True)
        if st.button("Submit"):
            table_id = 'fynd-db.finance_dwh.tds_seller_deductions'
            upload_to_bigquery(df, table_id)
            # Specify your Google Drive folder ID here
            folder_id = '1HOQsH67YUi3LochstFHnpapsH06MW0Te'
            upload_to_drive(file_path, folder_id)
    
            # Now you don't need to save the credentials temporarily, as we are using them directly
            st.write("")
        else:
            st.write("")

else:
    st.write("")