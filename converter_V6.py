import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import smtplib
from email.mime.text import MIMEText
import traceback
import json

# Load configuration from JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

log_folder = config['log_folder']
folder_table_mapping = config['folder_table_mapping']
server_name = config['server_name']
database_name = config['database_name']
sql_username = config['sql_username']
sql_password = config['sql_password']
smtp_server = config['smtp_server']
sender_email = config['sender_email']
recipient_emails = config['recipient_emails']

# Set up logging configuration
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_filename = os.path.join(log_folder, f"conversion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to send completion email
# Function to send completion email to multiple recipients
def send_email(subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)  # Join multiple recipients with a comma and space

    server = smtplib.SMTP(smtp_server)
    server.sendmail(msg['From'], recipient_emails, msg.as_string())
    server.quit()


# Function to read CSV files from multiple folders and create tables based on folder paths
def convert_folders_to_defined_tables():
    try:
        # Create SQL Server connection string
        conn_str = f"mssql+pyodbc://{sql_username}:{sql_password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"

        # Create SQLAlchemy engine
        engine = create_engine(conn_str)

        total_rows_processed = 0
        total_rows_read = 0

        for folder, table_name in folder_table_mapping.items():
            start_time = datetime.now()

            # Initialize an empty DataFrame to store combined data for this folder
            combined_df = pd.DataFrame()

            # Get a list of CSV files in the folder
            csv_files = [file for file in os.listdir(folder) if file.endswith('.csv')]

            for file in csv_files:
                try:
                    # Read each CSV file into a pandas DataFrame
                    df = pd.read_csv(os.path.join(folder, file), sep='|')  # Change the delimiter if needed

                    # Add columns for file name and time of conversion
                    df['FileName'] = file
                    df['ConversionTime'] = datetime.now()

                    # Concatenate current DataFrame with the combined DataFrame
                    combined_df = pd.concat([combined_df, df])

                    # Log the number of rows processed for each file
                    rows_processed = len(df)
                    total_rows_processed += rows_processed
                    logging.info(f"Processed {rows_processed} rows from {file} in folder {folder}")

                except Exception as e:
                    # Log any errors encountered during file processing
                    logging.error(f"Error processing {file} in folder {folder}: {str(e)}")
                    logging.error(traceback.format_exc())

            # Write the combined DataFrame to SQL Server with defined table name
            combined_df.to_sql(table_name, engine, if_exists='replace', index=False)

            end_time = datetime.now()
            execution_time = end_time - start_time

            # Log successful completion for each folder
            logging.info(f"Successfully converted data from {folder} to table {table_name}. "
                         f"Total time taken: {execution_time}")

        engine.dispose()

        # Log total number of rows processed across all files and folders
        logging.info(f"Total rows processed: {total_rows_processed}")

        logging.info("All conversions completed successfully")

        # Send email on successful completion
        success_message = (
            "The conversion process has completed successfully.\n"
            f"Total number of rows read from files: {total_rows_read}\n"
            f"Total number of rows processed: {total_rows_processed}\n"
            f"Total time taken: {execution_time}\n"
            f"This email was automatically generated using the successfactors file Converter version 6.0.\n"
        )
        send_email("Conversion Process Completed", success_message)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error(traceback.format_exc())

        # Send error email
        error_message = (
            f"An error occurred during the conversion process: {str(e)}\n"
            "Details:\n"
            f"Total number of rows read from files before error occurred: {total_rows_read}\n"
            f"Total number of rows processed before error occurred: {total_rows_processed}\n"
            f"Total time taken before error occurred: {execution_time}\n"
            f"This email was automatically generated using the successfactors file Converter version 6.0.\n"
        )
        send_email("Conversion Process Error", error_message)

# Call the function to convert folders to specified tables in SQL and log the process
convert_folders_to_defined_tables()
