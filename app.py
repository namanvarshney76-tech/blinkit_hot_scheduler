#!/usr/bin/env python3
"""
Scheduled Blinkit HOT Automation Workflows with Source File Tracking
Optimized for GitHub Actions - Runs every 3 hours via GitHub scheduler
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
from lxml import etree

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import zipfile

warnings.filterwarnings("ignore")

# Configure logging for GitHub Actions
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
        self.gmail_scopes = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.send'
        ]
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
            'sheet_name': 'hotgrn',
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
        
        # Email configuration
        self.email_config = {
            'recipient': 'keyur@thebakersdozen.in',
            'subject_prefix': 'Blinkit HOT Automation Summary'
        }
    
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {"timestamp": timestamp, "level": level.upper(), "message": message}
        self.logs.append(log_entry)
        
        # Different formatting for different levels
        if level.upper() == "ERROR":
            logging.error(message)
        elif level.upper() == "WARNING":
            logging.warning(message)
        elif level.upper() == "SUCCESS":
            logging.info(f"✅ {message}")
        else:
            logging.info(message)
    
    def authenticate(self):
        """Authenticate using pre-existing token file with proper refresh handling"""
        try:
            self.log("Authenticating with Google APIs for GitHub Actions...", "INFO")
            
            # Load credentials from token file if exists
            creds = None
            token_file = 'token.json'
            creds_file = 'credentials.json'
            
            if os.path.exists(token_file):
                self.log(f"Found token file at {token_file}", "INFO")
                try:
                    # Read token file to check its content
                    with open(token_file, 'r') as f:
                        token_content = f.read()
                        self.log(f"Token file size: {len(token_content)} bytes", "INFO")
                    
                    creds = Credentials.from_authorized_user_file(token_file, 
                        list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes)))
                    
                    # Check if token is expired
                    if creds.expired:
                        self.log(f"Token expired at {creds.expiry}", "WARNING")
                        if creds.refresh_token:
                            self.log("Token has refresh token, attempting to refresh...", "INFO")
                            try:
                                creds.refresh(Request())
                                self.log("Token refreshed successfully!", "SUCCESS")
                                
                                # Save refreshed token
                                with open(token_file, 'w') as token:
                                    token.write(creds.to_json())
                                self.log("Refreshed token saved to file", "INFO")
                            except Exception as refresh_error:
                                self.log(f"Failed to refresh token: {str(refresh_error)}", "ERROR")
                                
                                # Try to get new token using credentials
                                if os.path.exists(creds_file):
                                    self.log("Attempting to get new token using credentials...", "INFO")
                                    try:
                                        flow = InstalledAppFlow.from_client_secrets_file(
                                            creds_file,
                                            list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                                        )
                                        # For non-interactive environment, we need to handle this differently
                                        # Since we can't do local server in GitHub Actions
                                        self.log("Cannot perform interactive auth in GitHub Actions", "ERROR")
                                        return False
                                    except Exception as flow_error:
                                        self.log(f"Failed to create auth flow: {str(flow_error)}", "ERROR")
                                        return False
                                else:
                                    self.log("Credentials file not found", "ERROR")
                                    return False
                        else:
                            self.log("Token expired and no refresh token available", "ERROR")
                            return False
                    else:
                        self.log(f"Token is valid until {creds.expiry}", "INFO")
                except Exception as e:
                    self.log(f"Failed to load token: {str(e)}", "ERROR")
                    return False
            else:
                self.log("Token file not found", "ERROR")
                return False
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            # Test authentication by making a simple API call
            try:
                profile = self.gmail_service.users().getProfile(userId='me').execute()
                self.log(f"Authenticated as: {profile.get('emailAddress', 'Unknown')}", "SUCCESS")
            except Exception as api_error:
                self.log(f"Authentication test failed: {str(api_error)}", "ERROR")
                return False
            
            self.log("Authentication successful!", "SUCCESS")
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
        """Process Gmail attachment download workflow with detailed tracking"""
        workflow_start = datetime.now()
        
        # Initialize counters
        gmail_summary = {
            'emails_checked': 0,
            'attachments_found': 0,
            'attachments_skipped': 0,
            'attachments_uploaded': 0,
            'attachments_failed': 0,
            'details': []
        }
        
        try:
            self.log("Starting Gmail workflow...", "INFO")
            
            # Search for emails
            emails = self.search_emails(
                sender=self.gmail_config['sender'],
                search_term=self.gmail_config['search_term'],
                days_back=self.gmail_config['days_back'],
                max_results=self.gmail_config['max_results']
            )
            
            gmail_summary['emails_checked'] = len(emails)
            
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                return {
                    'success': True, 
                    'processed': 0,
                    'gmail_summary': gmail_summary,
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
                    'gmail_summary': gmail_summary,
                    'start_time': workflow_start,
                    'end_time': datetime.now()
                }
            
            processed_emails = 0
            
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
                    
                    # Extract attachments with detailed tracking
                    attachment_stats = self._extract_attachments_from_email_detailed(
                        email['id'], message['payload'], sender, self.gmail_config, base_folder_id
                    )
                    
                    # Update summary
                    gmail_summary['attachments_found'] += attachment_stats['total']
                    gmail_summary['attachments_skipped'] += attachment_stats['skipped']
                    gmail_summary['attachments_uploaded'] += attachment_stats['uploaded']
                    gmail_summary['attachments_failed'] += attachment_stats['failed']
                    
                    # Add to details
                    gmail_summary['details'].append({
                        'email_subject': subject,
                        'sender': sender,
                        'attachments_found': attachment_stats['total'],
                        'attachments_uploaded': attachment_stats['uploaded'],
                        'attachments_skipped': attachment_stats['skipped'],
                        'attachments_failed': attachment_stats['failed']
                    })
                    
                    if attachment_stats['total'] > 0:
                        processed_emails += 1
                        self.log(f"Found {attachment_stats['total']} attachments in: {subject} (Uploaded: {attachment_stats['uploaded']}, Skipped: {attachment_stats['skipped']}, Failed: {attachment_stats['failed']})", "SUCCESS")
                    else:
                        self.log(f"No matching attachments in: {subject}", "INFO")
                    
                except Exception as e:
                    gmail_summary['attachments_failed'] += 1  # Count this email as failed
                    self.log(f"Failed to process email {email.get('id', 'unknown')}: {str(e)}", "ERROR")
            
            self.log(f"Gmail workflow completed! Processed {gmail_summary['attachments_uploaded']} attachments from {processed_emails} emails", "INFO")
            self.log(f"Summary: Found: {gmail_summary['attachments_found']}, Uploaded: {gmail_summary['attachments_uploaded']}, Skipped: {gmail_summary['attachments_skipped']}, Failed: {gmail_summary['attachments_failed']}", "INFO")
            
            return {
                'success': True, 
                'processed': gmail_summary['attachments_uploaded'],
                'gmail_summary': gmail_summary,
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
            
        except Exception as e:
            self.log(f"Gmail workflow failed: {str(e)}", "ERROR")
            return {
                'success': False, 
                'processed': 0,
                'gmail_summary': gmail_summary,
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
    
    def process_excel_workflow(self) -> Dict[str, Any]:
        """Process Excel GRN workflow with source file tracking and duplicate removal"""
        workflow_start = datetime.now()
        
        # Initialize counters
        excel_summary = {
            'files_found': 0,
            'files_skipped': 0,
            'files_processed': 0,
            'files_failed': 0,
            'duplicates_removed': 0,
            'details': []
        }
        
        try:
            self.log("Starting Excel GRN workflow with source file tracking...", "INFO")
            
            # Step 1: Get total Excel files with 'GRN' in name from Drive folder
            all_excel_files = self._get_excel_files_with_grn(
                self.excel_config['excel_folder_id'], 
                self.excel_config['days_back'], 
                self.excel_config['max_results']
            )
            
            excel_summary['files_found'] = len(all_excel_files)
            
            if not all_excel_files:
                self.log("No Excel files with 'GRN' found in the specified folder", "WARNING")
                return {
                    'success': True, 
                    'processed': 0,
                    'excel_summary': excel_summary,
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
                    excel_summary['files_skipped'] += 1
                    self.log(f"Skipping already processed file: {file['name']}", "INFO")
            
            self.log(f"Found {len(new_excel_files)} new files to process (not in sheet yet)", "INFO")
            
            if not new_excel_files:
                self.log("All files already processed in previous runs", "INFO")
                return {
                    'success': True, 
                    'processed': 0,
                    'excel_summary': excel_summary,
                    'start_time': workflow_start,
                    'end_time': datetime.now()
                }
            
            # Step 4: Process new files
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
                        excel_summary['files_failed'] += 1
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
                    
                    excel_summary['files_processed'] += 1
                    excel_summary['details'].append({
                        'file_name': file['name'],
                        'status': 'processed',
                        'rows_added': len(df)
                    })
                    
                    self.log(f"Appended data from: {file['name']}", "SUCCESS")
                    is_first_file = False
                    
                except Exception as e:
                    excel_summary['files_failed'] += 1
                    self.log(f"Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}", "ERROR")
            
            # Step 5: Remove duplicates from the entire sheet
            if excel_summary['files_processed'] > 0:
                duplicates_removed = self._remove_duplicates_from_sheet(
                    self.excel_config['spreadsheet_id'],
                    self.excel_config['sheet_name'],
                    self.excel_config['source_file_column']
                )
                excel_summary['duplicates_removed'] = duplicates_removed
            
            self.log(f"Excel workflow completed! Processed {excel_summary['files_processed']} new files out of {len(all_excel_files)} total files", "INFO")
            self.log(f"Summary: Found: {excel_summary['files_found']}, Processed: {excel_summary['files_processed']}, Skipped: {excel_summary['files_skipped']}, Failed: {excel_summary['files_failed']}, Duplicates Removed: {excel_summary['duplicates_removed']}", "INFO")
            
            return {
                'success': True, 
                'processed': excel_summary['files_processed'],
                'excel_summary': excel_summary,
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
            
        except Exception as e:
            self.log(f"Excel workflow failed: {str(e)}", "ERROR")
            return {
                'success': False, 
                'processed': 0,
                'excel_summary': excel_summary,
                'start_time': workflow_start,
                'end_time': datetime.now()
            }
    
    def _remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str, source_file_column: str) -> int:
        """Remove duplicates from the entire Google Sheet"""
        try:
            self.log(f"Removing duplicates from sheet: {sheet_name}", "INFO")
            
            # Get all data from the sheet
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:Z"
            ).execute()
            
            values = result.get('values', [])
            
            if not values or len(values) <= 1:
                self.log("No data found in sheet to remove duplicates", "INFO")
                return 0
            
            # Convert to DataFrame for duplicate removal
            headers = values[0]
            data = values[1:]
            
            # Find source file column index
            try:
                source_col_index = headers.index(source_file_column)
            except ValueError:
                self.log(f"Source file column '{source_file_column}' not found in sheet", "WARNING")
                source_col_index = -1
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=headers)
            
            # Store original count
            original_count = len(df)
            
            # Remove duplicates (keeping the first occurrence)
            df = df.drop_duplicates()
            
            # Count duplicates removed
            duplicates_removed = original_count - len(df)
            
            if duplicates_removed > 0:
                self.log(f"Removing {duplicates_removed} duplicate rows from sheet", "INFO")
                
                # Prepare data for update
                all_values = [headers] + df.fillna('').astype(str).values.tolist()
                
                # Clear the entire sheet
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:Z"
                ).execute()
                
                # Update with deduplicated data
                body = {'values': all_values}
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
                
                self.log(f"Successfully removed {duplicates_removed} duplicate rows", "SUCCESS")
            else:
                self.log("No duplicates found in sheet", "INFO")
            
            return duplicates_removed
            
        except Exception as e:
            self.log(f"Failed to remove duplicates from sheet: {str(e)}", "ERROR")
            return 0
    
    def _extract_attachments_from_email_detailed(self, message_id: str, payload: Dict, sender: str, config: dict, base_folder_id: str) -> Dict[str, int]:
        """Extract attachments from email with detailed tracking"""
        stats = {'total': 0, 'uploaded': 0, 'skipped': 0, 'failed': 0}
        
        if "parts" in payload:
            for part in payload["parts"]:
                part_stats = self._extract_attachments_from_email_detailed(
                    message_id, part, sender, config, base_folder_id
                )
                for key in stats:
                    stats[key] += part_stats[key]
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            # Filter for Excel files only
            if not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                return stats
            
            stats['total'] += 1
            
            try:
                # Get attachment data
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Create nested folder structure
                sender_email = sender
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()
                sender_folder_name = self._sanitize_filename(sender_email)
                search_term = config.get('search_term', 'all-attachments')
                search_folder_name = search_term if search_term else "all-attachments"
                file_type_folder = "Excel_Files"
                
                # Create folders
                sender_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)
                search_folder_id = self._create_drive_folder(search_folder_name, sender_folder_id)
                type_folder_id = self._create_drive_folder(file_type_folder, search_folder_id)
                
                # Clean filename and make it unique
                clean_filename = self._sanitize_filename(filename)
                final_filename = f"{message_id}_{clean_filename}"
                
                # Check if file already exists
                if not self._file_exists_in_folder(final_filename, type_folder_id):
                    # Upload to Drive
                    file_metadata = {
                        'name': final_filename,
                        'parents': [type_folder_id]
                    }
                    
                    media = MediaIoBaseUpload(
                        io.BytesIO(file_data),
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                    
                    self.drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()
                    
                    stats['uploaded'] += 1
                else:
                    stats['skipped'] += 1
                    
            except Exception as e:
                stats['failed'] += 1
                self.log(f"Failed to process attachment {filename}: {str(e)}", "ERROR")
        
        return stats
    
    def _send_summary_email(self, summary_data: Dict):
        """Send summary email with workflow results - Fixed version"""
        try:
            self.log("Preparing to send summary email...", "INFO")
            
            # Get user's email address
            profile = self.gmail_service.users().getProfile(userId='me').execute()
            user_email = profile['emailAddress']
            self.log(f"Sending email from: {user_email}", "INFO")
            
            # Prepare email content
            subject = f"{self.email_config['subject_prefix']} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Build email body with proper formatting
            body_lines = [
                "Blinkit HOT Automation Workflow Summary",
                "=" * 40,
                f"Workflow Start: {summary_data['workflow_start'].strftime('%Y-%m-%d %H:%M:%S')}",
                f"Workflow End: {summary_data['workflow_end'].strftime('%Y-%m-%d %H:%M:%S')}",
                f"Days Back Parameter: {self.gmail_config['days_back']} days",
                "",
                "MAIL TO DRIVE WORKFLOW:",
                "-" * 20,
                f"Number of emails checked: {summary_data['emails_checked']}",
                f"Number of attachments found: {summary_data['attachments_found']}",
                f"Number of attachments skipped (already exist): {summary_data['attachments_skipped']}",
                f"Number of attachments uploaded: {summary_data['attachments_uploaded']}",
                f"Number of attachments failed to upload: {summary_data['attachments_failed']}",
                f"Gmail Workflow Status: {'SUCCESS' if summary_data['gmail_success'] else 'FAILED'}",
                "",
                "DRIVE TO SHEET WORKFLOW:",
                "-" * 20,
                f"Number of files found (within {self.excel_config['days_back']} days): {summary_data['total_files_found']}",
                f"Number of files skipped (already in sheet): {summary_data['files_skipped']}",
                f"Number of files processed: {summary_data['files_processed']}",
                f"Number of files failed to process: {summary_data['files_failed']}",
                f"Number of duplicate rows removed: {summary_data['duplicates_removed']}",
                f"Excel Workflow Status: {'SUCCESS' if summary_data['excel_success'] else 'FAILED'}",
                "",
                "OVERALL STATUS:",
                "-" * 15,
                f"Overall Workflow Status: {'SUCCESS' if summary_data['overall_success'] else 'FAILED'}",
                f"Total Duration: {summary_data['duration_minutes']:.2f} minutes",
                "",
                "This is an automated email from Blinkit HOT Automation Scheduler."
            ]
            
            body = "\n".join(body_lines)
            
            # Create email message with proper formatting
            message = f"Subject: {subject}\n"
            message += f"To: {self.email_config['recipient']}\n"
            message += f"From: {user_email}\n"
            message += f"Cc: {user_email}\n"
            message += "Content-Type: text/plain; charset=\"UTF-8\"\n\n"
            message += body
            
            # Encode message
            raw_message = base64.urlsafe_b64encode(message.encode('utf-8')).decode('utf-8')
            
            # Send email
            send_result = self.gmail_service.users().messages().send(
                userId='me',
                body={'raw': raw_message}
            ).execute()
            
            self.log(f"Summary email sent to {self.email_config['recipient']} and CC'd to {user_email}", "SUCCESS")
            self.log(f"Email message ID: {send_result.get('id', 'Unknown')}", "INFO")
            
        except Exception as e:
            self.log(f"Failed to send summary email: {str(e)}", "ERROR")
            # Don't raise the exception, just log it
    
    def run_complete_workflow(self):
        """Run complete workflow: Gmail → Excel with Source File Tracking → Log Summary → Send Email"""
        self.log("=" * 70, "INFO")
        self.log("Starting Complete Blinkit HOT Workflow with Source File Tracking", "INFO")
        self.log("=" * 70, "INFO")
        self.log(f"Current working directory: {os.getcwd()}", "INFO")
        self.log(f"Files in directory: {os.listdir('.')}", "INFO")
        
        # Authenticate first
        if not self.authenticate():
            self.log("Authentication failed! Cannot run workflow.", "ERROR")
            return False
        
        overall_start = datetime.now()
        
        # Step 1: Run Gmail workflow
        self.log("--- Step 1: Gmail to Drive Workflow ---", "INFO")
        gmail_result = self.process_gmail_workflow()
        
        # Step 2: Run Excel workflow with source file tracking
        self.log("--- Step 2: Drive to Sheet Workflow ---", "INFO")
        excel_result = self.process_excel_workflow()
        
        overall_end = datetime.now()
        duration = (overall_end - overall_start).total_seconds() / 60
        
        # Step 3: Prepare summary data
        summary_data = {
            'workflow_start': overall_start,
            'workflow_end': overall_end,
            'duration_minutes': duration,
            'emails_checked': gmail_result.get('gmail_summary', {}).get('emails_checked', 0),
            'attachments_found': gmail_result.get('gmail_summary', {}).get('attachments_found', 0),
            'attachments_skipped': gmail_result.get('gmail_summary', {}).get('attachments_skipped', 0),
            'attachments_uploaded': gmail_result.get('gmail_summary', {}).get('attachments_uploaded', 0),
            'attachments_failed': gmail_result.get('gmail_summary', {}).get('attachments_failed', 0),
            'total_files_found': excel_result.get('excel_summary', {}).get('files_found', 0),
            'files_skipped': excel_result.get('excel_summary', {}).get('files_skipped', 0),
            'files_processed': excel_result.get('excel_summary', {}).get('files_processed', 0),
            'files_failed': excel_result.get('excel_summary', {}).get('files_failed', 0),
            'duplicates_removed': excel_result.get('excel_summary', {}).get('duplicates_removed', 0),
            'gmail_success': gmail_result.get('success', False),
            'excel_success': excel_result.get('success', False),
            'overall_success': gmail_result.get('success', False) and excel_result.get('success', False)
        }
        
        # Step 4: Log summary to sheet
        self.log("--- Step 3: Logging Summary to Google Sheet ---", "INFO")
        self._log_summary_to_sheet(summary_data)
        
        # Step 5: Send summary email
        self.log("--- Step 4: Sending Summary Email ---", "INFO")
        self._send_summary_email(summary_data)
        
        # Log final summary to console
        self.log("=" * 70, "INFO")
        self.log("Complete Workflow Finished", "SUCCESS")
        self.log(f"Duration: {duration:.2f} minutes", "INFO")
        self.log(f"Emails checked: {summary_data['emails_checked']}", "INFO")
        self.log(f"Attachments uploaded: {summary_data['attachments_uploaded']}", "INFO")
        self.log(f"Total Excel files found: {summary_data['total_files_found']}", "INFO")
        self.log(f"New files processed: {summary_data['files_processed']}", "INFO")
        self.log(f"Duplicates removed: {summary_data['duplicates_removed']}", "INFO")
        self.log(f"Overall success: {summary_data['overall_success']}", "SUCCESS")
        self.log("=" * 70, "INFO")
        
        return summary_data['overall_success']
    
    # Helper methods
    def _append_to_sheet_with_source(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, 
                                    source_file_column: str, include_headers: bool):
        """Append DataFrame to Google Sheet with source file column"""
        try:
            # Make sure source file column is the last column
            columns = [col for col in df.columns if col != source_file_column] + [source_file_column]
            df = df[columns]
            
            # Convert DataFrame to values
            if include_headers:
                # Include headers
                values = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()
            else:
                # Skip headers
                values = df.fillna('').astype(str).values.tolist()
            
            if not values:
                self.log("No data to append", "WARNING")
                return
            
            # Prepare the request body
            body = {
                'values': values
            }
            
            # Append data to the sheet
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A",
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            self.log(f"Appended {len(values)} rows to Google Sheet with source file tracking", "INFO")
            
        except Exception as e:
            self.log(f"Failed to append to Google Sheet: {str(e)}", "ERROR")

    def _log_summary_to_sheet(self, summary_data: Dict):
        """Log workflow summary to Google Sheet"""
        try:
            # Prepare summary row
            summary_row = [
                summary_data['workflow_start'].strftime("%Y-%m-%d %H:%M:%S"),
                summary_data['workflow_end'].strftime("%Y-%m-%d %H:%M:%S"),
                summary_data['duration_minutes'],
                summary_data['emails_checked'],
                summary_data['attachments_found'],
                summary_data['attachments_skipped'],
                summary_data['attachments_uploaded'],
                summary_data['attachments_failed'],
                summary_data['total_files_found'],
                summary_data['files_skipped'],
                summary_data['files_processed'],
                summary_data['files_failed'],
                summary_data['duplicates_removed'],
                "SUCCESS" if summary_data['gmail_success'] else "FAILED",
                "SUCCESS" if summary_data['excel_success'] else "FAILED",
                "SUCCESS" if summary_data['overall_success'] else "FAILED"
            ]
            
            # Check if summary sheet exists and has headers
            try:
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=self.summary_config['spreadsheet_id'],
                    range=f"{self.summary_config['sheet_name']}!A:A"
                ).execute()
                
                values = result.get('values', [])
                
                # If no headers exist, add them
                if not values:
                    headers = [
                        "Workflow Start", "Workflow End", "Duration (min)", "Emails Checked", 
                        "Attachments Found", "Attachments Skipped", "Attachments Uploaded",
                        "Attachments Failed", "Total Files Found", "Files Skipped",
                        "Files Processed", "Files Failed", "Duplicates Removed",
                        "Gmail Status", "Excel Status", "Overall Status"
                    ]
                    body = {'values': [headers, summary_row]}
                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=self.summary_config['spreadsheet_id'],
                        range=f"{self.summary_config['sheet_name']}!A1",
                        valueInputOption='USER_ENTERED',
                        body=body
                    ).execute()
                else:
                    # Append new summary row
                    body = {'values': [summary_row]}
                    self.sheets_service.spreadsheets().values().append(
                        spreadsheetId=self.summary_config['spreadsheet_id'],
                        range=f"{self.summary_config['sheet_name']}!A:A",
                        valueInputOption='USER_ENTERED',
                        insertDataOption='INSERT_ROWS',
                        body=body
                    ).execute()
                
                self.log("Workflow summary logged to Google Sheet", "INFO")
                
            except HttpError as e:
                # Sheet might not exist, create it
                if "Unable to parse range" in str(e):
                    self.log("Creating summary sheet...", "INFO")
                    # Create the sheet by writing headers and data
                    headers = [
                        "Workflow Start", "Workflow End", "Duration (min)", "Emails Checked", 
                        "Attachments Found", "Attachments Skipped", "Attachments Uploaded",
                        "Attachments Failed", "Total Files Found", "Files Skipped",
                        "Files Processed", "Files Failed", "Duplicates Removed",
                        "Gmail Status", "Excel Status", "Overall Status"
                    ]
                    body = {'values': [headers, summary_row]}
                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=self.summary_config['spreadsheet_id'],
                        range=f"{self.summary_config['sheet_name']}!A1",
                        valueInputOption='USER_ENTERED',
                        body=body
                    ).execute()
                    self.log("Created summary sheet and logged workflow data", "INFO")
                else:
                    raise e
                    
        except Exception as e:
            self.log(f"Failed to log summary to sheet: {str(e)}", "ERROR")

    def _get_existing_source_files(self, spreadsheet_id: str, sheet_name: str, source_file_column: str) -> List[str]:
        """Get list of existing source files from Google Sheet"""
        try:
            # First, check if the sheet exists and has data
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:Z"
            ).execute()
            
            values = result.get('values', [])
            
            if not values or len(values) <= 1:
                return []
            
            # Find the column index for source file
            headers = values[0]
            try:
                source_col_index = headers.index(source_file_column)
            except ValueError:
                # Source file column doesn't exist yet
                return []
            
            # Extract all source files from the column (skip header)
            source_files = []
            for row in values[1:]:
                if len(row) > source_col_index and row[source_col_index]:
                    source_files.append(row[source_col_index])
            
            return list(set(source_files))  # Return unique source files
            
        except HttpError as e:
            # Sheet might not exist
            if "Unable to parse range" in str(e):
                return []
            else:
                self.log(f"Failed to get existing source files: {str(e)}", "ERROR")
                return []
        except Exception as e:
            self.log(f"Failed to get existing source files: {str(e)}", "ERROR")
            return []
    
    def _check_sheet_has_data(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Check if the sheet already has data (more than just headers)"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A"
            ).execute()
            
            values = result.get('values', [])
            # Consider sheet has data if there are more than 1 row (header + at least one data row)
            return len(values) > 1
            
        except Exception as e:
            self.log(f"Failed to check if sheet has data: {str(e)}", "WARNING")
            return False

    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            self.log(f"Failed to get email details for {message_id}: {str(e)}", "ERROR")
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive"""
        try:
            # Check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            self.log(f"Failed to create folder {folder_name}: {str(e)}", "ERROR")
            return ""
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean up filenames to be safe for all operating systems"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _file_exists_in_folder(self, filename: str, folder_id: str) -> bool:
        """Check if file already exists in folder"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            return len(files) > 0
        except Exception as e:
            self.log(f"Failed to check file existence: {str(e)}", "ERROR")
            return False
    
    def _get_excel_files_with_grn(self, folder_id: str, days_back: int, max_results: int) -> List[Dict]:
        """Get Excel files containing 'GRN' in name from Drive folder"""
        try:
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00')
            query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel') and name contains 'GRN' and trashed=false and modifiedTime > '{start_date}'"
            results = self.drive_service.files().list(
                q=query,
                pageSize=max_results,
                fields="files(id, name, mimeType)",
                orderBy="modifiedTime desc"
            ).execute()
            
            files = results.get('files', [])
            return files
            
        except Exception as e:
            self.log(f"Failed to get Excel files: {str(e)}", "ERROR")
            return []
    
    def _read_excel_file(self, file_id: str, filename: str, header_row: int) -> pd.DataFrame:
        """Read Excel file from Drive with robust parsing"""
        try:
            # Download file content
            request = self.drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_stream.seek(0)
            
            # Attempt to read with pandas
            try:
                if header_row == -1:
                    df = pd.read_excel(file_stream, header=None)
                else:
                    df = pd.read_excel(file_stream, header=header_row)
                return self._clean_dataframe(df)
            except Exception as e:
                self.log(f"Standard read failed: {str(e)[:50]}...", "WARNING")
            
            # Fallback: raw XML extraction for corrupted files
            df = self._try_raw_xml_extraction(file_stream, filename, header_row)
            if not df.empty:
                return self._clean_dataframe(df)
            
            return pd.DataFrame()
            
        except Exception as e:
            self.log(f"Failed to read {filename}: {str(e)}", "ERROR")
            return pd.DataFrame()
    
    def _try_raw_xml_extraction(self, file_stream: io.BytesIO, filename: str, header_row: int) -> pd.DataFrame:
        """Extract data from Excel XML for corrupted files"""
        try:
            file_stream.seek(0)
            with zipfile.ZipFile(file_stream) as zip_ref:
                # Find worksheet
                worksheet_files = [f for f in zip_ref.namelist() if f.startswith('xl/worksheets/sheet')]
                if not worksheet_files:
                    return pd.DataFrame()
                
                xml_content = zip_ref.read(worksheet_files[0]).decode('utf-8')
                tree = etree.fromstring(xml_content.encode('utf-8'))
                
                # Extract cells
                ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
                rows = tree.xpath('//ns:row', namespaces=ns)
                
                data = []
                for row in rows:
                    row_data = []
                    cells = row.xpath('ns:c', namespaces=ns)
                    for cell in cells:
                        value = cell.xpath('ns:v/text()', namespaces=ns)
                        row_data.append(value[0] if value else '')
                    if row_data:
                        data.append(row_data)
                
                if not data:
                    return pd.DataFrame()
                
                if header_row >= 0 and len(data) > header_row:
                    headers = data[header_row]
                    df = pd.DataFrame(data[header_row+1:], columns=headers)
                else:
                    df = pd.DataFrame(data)
                
                return df
                
        except Exception as e:
            self.log(f"Raw XML extraction failed: {str(e)[:50]}...", "WARNING")
            return pd.DataFrame()
    
    def _clean_dataframe(self, df):
        """Clean DataFrame by removing rows with blank B column, duplicates, and single quotes"""
        if df.empty:
            return df
        
        self.log(f"Original DataFrame shape: {df.shape}", "INFO")
        
        # Remove single quotes from all string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)
        
        # Remove rows where second column (B column) is blank/empty
        if len(df.columns) >= 2:
            second_col = df.columns[1]
            mask = ~(
                df[second_col].isna() | 
                (df[second_col].astype(str).str.strip() == "") |
                (df[second_col].astype(str).str.strip() == "nan")
            )
            df = df[mask]
            self.log(f"After removing blank B column rows: {df.shape}", "INFO")
        
        # Remove duplicate rows
        original_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = original_count - len(df)
        
        if duplicates_removed > 0:
            self.log(f"Removed {duplicates_removed} duplicate rows", "INFO")
        
        self.log(f"Final cleaned DataFrame shape: {df.shape}", "INFO")
        return df


def run_once():
    """Run the workflow once (for GitHub Actions)"""
    automation = BlinkitHOTScheduler()
    return automation.run_complete_workflow()


if __name__ == "__main__":
    # For GitHub Actions - always run once
    success = run_once()
    exit_code = 0 if success else 1
    exit(exit_code)
