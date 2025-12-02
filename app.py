#!/usr/bin/env python3
"""
Blinkit HOT Automation - Gmail to Drive → Drive to Sheet with Source File Tracking
Designed to run on GitHub Actions every 3 hours
"""

import os
import json
import base64
import tempfile
import time
import logging
import pandas as pd
import zipfile
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from io import StringIO
import threading
import queue
import re
import io
import warnings

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import zipfile

from lxml import etree

warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('blinkit_hot_scheduler.log'),
        logging.StreamHandler()
    ]
)

class BlinkitHOTScheduler:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        self.logs: List[Dict] = []
        
        # Hardcoded configs (same as your Streamlit app)
        self.gmail_config = {
            'sender': 'purchaseorder@handsontrades.com',
            'search_term': 'GRN and reconciliation ',
            'days_back': 7,
            'max_results': 1000,
            'gdrive_folder_id': '1pZnVxyPRJWaoYldxvWyXLFxQHbdckZfP'
        }
        
        self.excel_config = {
            'excel_folder_id': '1KM0UGCN4_Z3XLD7nZTMpyM_bKVcsBCOZ',
            'spreadsheet_id': '10wyfALowemBcEFiZP9Tyy08npl_44FpHonO3rKARmRY',
            'sheet_name': 'test',
            'header_row': 0,
            'days_back': 7,
            'max_results': 1000,
            'source_file_column': 'source_file_name'  # New column name for source tracking
        }
        
        # Summary sheet configuration
        self.summary_config = {
            'spreadsheet_id': '10wyfALowemBcEFiZP9Tyy08npl_44FpHonO3rKARmRY',
            'sheet_name': 'hot_workflow_log'
        }
    
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {"timestamp": timestamp, "level": level.upper(), "message": message}
        self.logs.append(log_entry)
        logging.info(f"[{level}] {message}")
    
    def authenticate(self):
        """Authenticate using local credentials file"""
        try:
            self.log("Authenticating with Google APIs...", "INFO")
            
            # Load credentials from token file if exists
            creds = None
            token_file = 'token.json'
            
            if os.path.exists(token_file):
                creds = Credentials.from_authorized_user_file(token_file, 
                    list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes)))
            
            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', 
                        list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    )
                    creds = flow.run_local_server(port=0)
                
                # Save the credentials for the next run
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            self.log("Authentication successful!", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            # Build search query
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            # Add date filter
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            self.log(f"Searching Gmail with query: {query}", "INFO")
            
            # Execute search
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.log(f"Gmail search returned {len(messages)} messages", "INFO")
            
            return messages
            
        except Exception as e:
            self.log(f"Email search failed: {str(e)}", "ERROR")
            return []
    
    def process_gmail_workflow(self) -> Dict[str, Any]:
        """Process Gmail attachment download workflow"""
        workflow_start = datetime.now()
        emails_checked = 0
        attachments_saved = 0
        
        try:
            self.log("Starting Gmail workflow...", "INFO")
            
            # Search for emails
            emails = self.search_emails(
                sender=self.gmail_config['sender'],
                search_term=self.gmail_config['search_term'],
                days_back=self.gmail_config['days_back'],
                max_results=self.gmail_config['max_results']
            )
            
            emails_checked = len(emails)
            
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                return {
                    'success': True, 
                    'processed': 0,
                    'emails_checked': 0,
                    'attachments_saved': 0,
                    'start_time': workflow_start,
                    'end_time': datetime.now()
                }
            
            self.log(f"Found {len(emails)} emails matching criteria", "INFO")
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments"
            base_folder_id = self._create_drive_folder(base_folder_name, self.gmail_config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                self.log("Failed to create base folder in Google Drive", "ERROR")
                return {
                    'success': False, 
                    'processed': 0,
                    'emails_checked': emails_checked,
                    'attachments_saved': 0,
                    'start_time': workflow_start,
                    'end_time': datetime.now()
                }
            
            processed_count = 0
            total_attachments = 0
            
            for i, email in enumerate(emails):
                try:
                    # Get email details first
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    self.log(f"Processing email: {subject} from {sender}", "INFO")
                    
                    # Get full message with payload
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        self.log(f"No payload found for email: {subject}", "WARNING")
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], sender, self.gmail_config, base_folder_id
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        self.log(f"Found {attachment_count} attachments in: {subject}", "SUCCESS")
                    else:
                        self.log(f"No matching attachments in: {subject}", "INFO")
                    
                except Exception as e:
                    self.log(f"Failed to process email {email.get('id', 'unknown')}: {str(e)}", "ERROR")
            
            attachments_saved = total_attachments
            self.log(f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails", "INFO")
            
            return {
                'success': True, 
                'processed': total_attachments,
                'emails_checked': emails_checked,
                'attachments_saved': attachments_saved,
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
            
        except Exception as e:
            self.log(f"Gmail workflow failed: {str(e)}", "ERROR")
            return {
                'success': False, 
                'processed': 0,
                'emails_checked': emails_checked,
                'attachments_saved': attachments_saved,
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
    
    def process_excel_workflow(self) -> Dict[str, Any]:
        """Process Excel GRN workflow with source file tracking"""
        workflow_start = datetime.now()
        files_processed = 0
        new_files_count = 0
        
        try:
            self.log("Starting Excel GRN workflow with source file tracking...", "INFO")
            
            # Step 1: Get total Excel files with 'GRN' in name from Drive folder
            all_excel_files = self._get_excel_files_with_grn(
                self.excel_config['excel_folder_id'], 
                self.excel_config['days_back'], 
                self.excel_config['max_results']
            )
            
            if not all_excel_files:
                self.log("No Excel files with 'GRN' found in the specified folder", "WARNING")
                return {
                    'success': True, 
                    'processed': 0,
                    'files_processed': 0,
                    'new_files_count': 0,
                    'total_files_found': 0,
                    'start_time': workflow_start,
                    'end_time': datetime.now()
                }
            
            self.log(f"Found {len(all_excel_files)} Excel files containing 'GRN' in total", "INFO")
            
            # Step 2: Get existing source files from Google Sheet
            existing_source_files = self._get_existing_source_files(
                self.excel_config['spreadsheet_id'], 
                self.excel_config['sheet_name'],
                self.excel_config['source_file_column']
            )
            
            self.log(f"Found {len(existing_source_files)} existing source files in the sheet", "INFO")
            
            # Step 3: Filter out files that are already in the sheet
            new_excel_files = []
            for file in all_excel_files:
                if file['name'] not in existing_source_files:
                    new_excel_files.append(file)
                else:
                    self.log(f"Skipping already processed file: {file['name']}", "INFO")
            
            new_files_count = len(new_excel_files)
            self.log(f"Found {new_files_count} new files to process (not in sheet yet)", "INFO")
            
            if not new_excel_files:
                self.log("All files already processed in previous runs", "INFO")
                return {
                    'success': True, 
                    'processed': 0,
                    'files_processed': 0,
                    'new_files_count': 0,
                    'total_files_found': len(all_excel_files),
                    'start_time': workflow_start,
                    'end_time': datetime.now()
                }
            
            # Step 4: Process new files
            processed_count = 0
            sheet_has_data = self._check_sheet_has_data(
                self.excel_config['spreadsheet_id'], 
                self.excel_config['sheet_name']
            )
            
            is_first_file = True
            
            for i, file in enumerate(new_excel_files):
                try:
                    # Read Excel file
                    df = self._read_excel_file(file['id'], file['name'], self.excel_config['header_row'])
                    
                    if df.empty:
                        self.log(f"No data extracted from: {file['name']}", "WARNING")
                        continue
                    
                    # Add source file column to DataFrame
                    df[self.excel_config['source_file_column']] = file['name']
                    
                    self.log(f"Data shape: {df.shape} - Columns: {list(df.columns)[:3]}{'...' if len(df.columns) > 3 else ''}", "INFO")
                    
                    # Append to Google Sheet
                    self._append_to_sheet_with_source(
                        self.excel_config['spreadsheet_id'], 
                        self.excel_config['sheet_name'], 
                        df, 
                        self.excel_config['source_file_column'],
                        is_first_file and not sheet_has_data  # Only include headers if first file AND sheet is empty
                    )
                    
                    self.log(f"Appended data from: {file['name']}", "SUCCESS")
                    processed_count += 1
                    is_first_file = False
                    
                except Exception as e:
                    self.log(f"Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}", "ERROR")
            
            files_processed = processed_count
            
            self.log(f"Excel workflow completed! Processed {processed_count} new files out of {len(all_excel_files)} total files", "INFO")
            
            return {
                'success': True, 
                'processed': processed_count,
                'files_processed': files_processed,
                'new_files_count': new_files_count,
                'total_files_found': len(all_excel_files),
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
            
        except Exception as e:
            self.log(f"Excel workflow failed: {str(e)}", "ERROR")
            return {
                'success': False, 
                'processed': 0,
                'files_processed': files_processed,
                'new_files_count': new_files_count,
                'total_files_found': 0,
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
    
    def run_complete_workflow(self):
        """Run complete workflow: Gmail → Excel with Source File Tracking → Log Summary"""
        try:
            self.log("=== Starting Complete Blinkit HOT Workflow with Source File Tracking ===", "INFO")
            
            overall_start = datetime.now()
            
            # Step 1: Run Gmail workflow
            gmail_result = self.process_gmail_workflow()
            
            # Step 2: Run Excel workflow with source file tracking
            excel_result = self.process_excel_workflow()
            
            overall_end = datetime.now()
            
            # Step 3: Log summary to sheet
            summary_data = {
                'workflow_start': overall_start,
                'workflow_end': overall_end,
                'emails_checked': gmail_result.get('emails_checked', 0),
                'attachments_saved': gmail_result.get('attachments_saved', 0),
                'total_files_found': excel_result.get('total_files_found', 0),
                'new_files_processed': excel_result.get('new_files_count', 0),
                'files_processed': excel_result.get('files_processed', 0),
                'gmail_success': gmail_result.get('success', False),
                'excel_success': excel_result.get('success', False),
                'overall_success': gmail_result.get('success', False) and excel_result.get('success', False)
            }
            
            self._log_summary_to_sheet(summary_data)
            
            # Log final summary
            duration = (overall_end - overall_start).total_seconds() / 60
            self.log(f"=== Complete Workflow Finished ===", "INFO")
            self.log(f"Duration: {duration:.2f} minutes", "INFO")
            self.log(f"Emails checked: {summary_data['emails_checked']}", "INFO")
            self.log(f"Attachments saved: {summary_data['attachments_saved']}", "INFO")
            self.log(f"Total Excel files found: {summary_data['total_files_found']}", "INFO")
            self.log(f"New files processed: {summary_data['new_files_processed']}", "INFO")
            self.log(f"Files processed: {summary_data['files_processed']}", "INFO")
            self.log(f"Overall success: {summary_data['overall_success']}", "INFO")
            
            # Return success status
            return summary_data['overall_success']
            
        except Exception as e:
            self.log(f"Complete workflow failed: {str(e)}", "ERROR")
            return False
    
    # ... [Keep all the helper methods unchanged from your original appv2.py]
    # _get_existing_source_files, _check_sheet_has_data, _append_to_sheet_with_source,
    # _log_summary_to_sheet, _get_email_details, _create_drive_folder, _sanitize_filename,
    # _file_exists_in_folder, _extract_attachments_from_email, _get_excel_files_with_grn,
    # _read_excel_file, _try_raw_xml_extraction, _clean_dataframe
    # ... [All these methods remain exactly as in your original file]


def main():
    """Main function to run the complete workflow once"""
    print("=" * 80)
    print("BLINKIT HOT AUTOMATION WORKFLOW")
    print("=" * 80)
    
    automation = BlinkitHOTScheduler()
    
    # Authenticate
    print("\nAuthenticating...")
    if not automation.authenticate():
        print("ERROR: Authentication failed. Please check credentials.")
        return 1
    
    print("Authentication successful!")
    
    # Run the complete workflow
    print("\nRunning complete workflow...")
    try:
        success = automation.run_complete_workflow()
        
        print("\n" + "=" * 80)
        if success:
            print("✓ WORKFLOW COMPLETED SUCCESSFULLY")
        else:
            print("✗ WORKFLOW COMPLETED WITH ERRORS")
        print("=" * 80)
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"\n✗ WORKFLOW FAILED: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
