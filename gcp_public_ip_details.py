from google.oauth2 import service_account
from googleapiclient import discovery
import pandas as pd
from googleapiclient import discovery
import os
import json
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
import openpyxl
from googleapiclient.errors import HttpError
import dateutil.parser as dparser
import teradatasql
import datetime
from datetime import datetime
import dateutil.parser as dparser
from dateutil.parser import parse
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time


current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class DBOperations(object):
    """
    Class to interact with database API
    """
    _db_con = None
    _db_session = None

    @staticmethod
    def connect():
        """
        Method to create the connection
        """
        if DBOperations._db_con == None:
            DBOperations._db_con = teradatasql.connect(
                host='biggulp.td.teradata.com',
                user='aws_billing_admin',
                password='aws_billing_admin',
                logmech='td2')
        if DBOperations._db_session == None:
            DBOperations._db_session = DBOperations._db_con.cursor()
        return DBOperations._db_session

    @staticmethod
    def close():
        """
        Method to close the connection
        """
        if DBOperations._db_session:
            DBOperations._db_session.close()
        if DBOperations._db_con:
            DBOperations._db_con.close()

    @staticmethod
    def insert_data(json_data):
        try:
            print("Inserting data into DB started")
            db_con = DBOperations.connect()
            fields = list(json_data.keys())
            values = list(json_data.values())
            fields = ",".join(item for item in fields)
            values = ",".join("\'" + str(item) + "\'" if item is not None else 'NULL' for item in values) 
            print(json_data)
           
            insert_query = f"""
        UPDATE aws_billing.gcp_publicip_details
            SET last_Seen = '{current_timestamp}', 
                pip_status = '{json_data["pip_status"]}', 
                users = '{json_data["users"]}',
                usage_purpose = '{json_data["usage_purpose"]}'
            WHERE pip_status = '{json_data["pip_status"]}'
                and org_id = '{json_data["org_id"]}'
                and project_id = '{json_data["project_id"]}'
                and pip_name = '{json_data["pip_name"]}'
                AND region = '{json_data["region"]}'
                AND creationTimestamp = '{json_data["creationTimestamp"]}'
                and Deletion_Status = 'No'
            """
            db_con.execute(insert_query)

            # If no rows were affected, insert the record
            if db_con.rowcount == 0:
                insert_query = f"""
                INSERT INTO aws_billing.gcp_publicip_details({fields}, last_seen) 
                VALUES ({values}, '{current_timestamp}')
                """
                db_con.execute(insert_query)
        except Exception as err:
            print("Failed to update details in DB.")
            print(str(err))
            raise


def send_mail():
    # Define the HTML document
    html = '''
    <html>
    <body>
    <h1>Daily Resource GCP PublicIPs details report</h1>
    <p>Hello, welcome to your report!</p>
            Thanks,<br>
            Cloud Governance Team
    </body>
    </html>
        '''

    def attach_file_to_email(email_message, filename):
        # Open the attachment file for reading in binary mode, and make it a MIMEApplication class
        with open(filename, "rb") as f:
            file_attachment = MIMEApplication(f.read())
        # Add header/name to the attachments
        file_attachment.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )
        # Attach the file to the message
        email_message.attach(file_attachment)

    # Set up the email addresses and password. Please replace below with your email address and password
    email_from = 'adam.khan@teradata.com'
    email_to = "adam.khan@teradata.com"
    # Generate today's date to be included in the email Subject
    date_str = pd.Timestamp.today().strftime('%Y-%m-%d')
    # Create a MIMEMultipart class, and set up the From, To, Subject fields
    email_message = MIMEMultipart()
    email_message['From'] = email_from
    email_message['To'] = email_to
    email_message['Subject'] = f'GCP Resource PublicIPs Report email - {date_str}'
    # Attach the html doc defined earlier, as a MIMEText html content type to the MIME message
    email_message.attach(MIMEText(html, "html"))
    # Attach more (documents)
    ##############################################################
    # attach_file_to_email(email_message, 'chart.png')
    # attach_file_to_email(email_message, 'sample.txt')
    attach_file_to_email(email_message, 'public_ips_details.xlsx')
    ##############################################################
    # Convert it as a string
    email_string = email_message.as_string()
    try:
        smtpObj = smtplib.SMTP('rbmailer.td.teradata.com', 25)
        smtpObj.sendmail(email_from, email_to, email_string)
        print("Successfully sent email")
    except Exception:
        print("Error: unable to send email")



org_ids = ['441211675334','958178520169','398162026661','289917272062']
key_file_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
skip_projects = ['946321170780', '666087878088', '76166919157', '1024176972982', '884086977557', '28453924960', 'gcp-user-automation']


def list_folders(service, parent):
    folders = []
    request = service.folders().list(parent=parent)
    while request is not None:
        response = request.execute()
        folders.extend(response.get('folders', []))
        request = service.folders().list_next(previous_request=request, previous_response=response)
    return folders

def list_all_projects(service_v1, service_v2, parent):
    projects = []
    folders = list_folders(service_v2, parent)
    for folder in folders:
        projects.extend(list_all_projects(service_v1, service_v2, folder['name']))
    request = service_v1.projects().list(filter=f'parent.id:{parent.split("/")[-1]}')
    while request is not None:
        response = request.execute()
        projects.extend(response.get('projects', []))
        request = service_v1.projects().list_next(previous_request=request, previous_response=response)
    return projects

def get_all_external_ips(org_ids, key_file_path, skip_projects):
    credentials = service_account.Credentials.from_service_account_file(key_file_path)
    service_v1 = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)
    service_v2 = discovery.build('cloudresourcemanager', 'v2', credentials=credentials)
    compute_service = discovery.build('compute', 'v1', credentials=credentials)
    
    external_ips = []
    
    for org_id in org_ids:
        print(f"Checking org: {org_id}")
        print("++++++++++")

        projects = list_all_projects(service_v1, service_v2, f'organizations/{org_id}')
        for project in projects:
            project_id = project['projectId']
            print(f"Checking project: {project_id}")
            print("++++++++++")
            if project_id in skip_projects:
                continue
            
            try:
                regions = compute_service.regions().list(project=project_id).execute().get('items', [])
            except HttpError as e:
                if e.resp.status == 403:
                    print(f"Skipping project {project_id} due to API not enabled or insufficient permissions.")
                    continue
                elif e.resp.status == 404:
                    print(f"Project {project_id} not found, skipping.")
                    continue
                else:
                    raise

            for region in regions:
                region_name = region['name']
                try:
                    request = compute_service.addresses().list(project=project_id, region=region_name)
                    while request is not None:
                        response = request.execute()
                        if 'items' in response:
                            for address in response['items']:
                                format_date = '%Y-%m-%d %H:%M:%S'
                                creationTimestamp = parse(address['creationTimestamp']).strftime(format_date)
                                users = [user.split('/')[-1] for user in address['users']] if 'users' in address else None
                                external_ips.append({
                                    'org_id': org_id,
                                    'project_id': project_id,
                                    'pip_name': address['name'],
                                    'pip_address': address['address'],
                                    'pip_status': address['status'],
                                    'addressType': address['addressType'],
                                    'usage_purpose': address.get('purpose', None),
                                    'region': address['region'].split('/')[-1],
                                    'users': ', '.join(users) if users else None,
                                    'creationTimestamp': creationTimestamp,
                                    'Deletion_Status': 'No',
                                })
                        request = compute_service.addresses().list_next(previous_request=request, previous_response=response)
                except HttpError as e:
                    if e.resp.status == 403:
                        print(f"Skipping addresses listing in project {project_id}, region {region_name} due to insufficient permissions.")
                        continue
                    else:
                        raise
    
    return external_ips

# Usage
external_ips = get_all_external_ips(org_ids, key_file_path, skip_projects)

df = pd.DataFrame.from_records(external_ips)
df.to_excel('public_ips_details.xlsx', index=False)

for item in external_ips:
    try:
        DBOperations.insert_data(item)
    except Exception as err:
        print("Failed to insert data into DB.")
        print(str(err))
        raise

send_mail()
