#!/usr/bin/env python3
"""
Blinkit HOT Automation - Gmail to Drive → Drive to Sheet with Source File Tracking
Runs on GitHub Actions every 3 hours
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
import re
import io
import warnings

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from lxml import etree

warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('blinkit_scheduler.log'),
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
        
        # Configuration
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
            'source_file_column': 'source_file_name'
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
            
            creds = None
            token_file = 'token.json'
            
            if os.path.exists(token_file):
                creds = Credentials.from_authorized_user_file(token_file, 
                    list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes)))
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', 
                        list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    )
                    creds = flow.run_local_server(port=0)
                
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
            
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
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            self.log(f"Searching Gmail with query: {query}", "INFO")
            
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
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    self.log(f"Processing email: {subject} from {sender}", "INFO")
                    
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        self.log(f"No payload found for email: {subject}", "WARNING")
                        continue
                    
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], sender, self.gmail_config, base_folder_id
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        self.log(f"Found {attachment_count} attachments in: {subject}", "SUCCESS")
                    
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
            
            existing_source_files = self._get_existing_source_files(
                self.excel_config['spreadsheet_id'], 
                self.excel_config['sheet_name'],
                self.excel_config['source_file_column']
            )
            
            self.log(f"Found {len(existing_source_files)} existing source files in the sheet", "INFO")
            
            new_excel_files = []
            for file in all_excel_files:
                if file['name'] not in existing_source_files:
                    new_excel_files.append(file)
                else:
                    self.log(f"Skipping already processed file: {file['name']}", "INFO")
            
            new_files_count = len(new_excel_files)
            self.log(f"Found {new_files_count} new files to process", "INFO")
            
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
            
            processed_count = 0
            sheet_has_data = self._check_sheet_has_data(
                self.excel_config['spreadsheet_id'], 
                self.excel_config['sheet_name']
            )
            
            is_first_file = True
            
            for i, file in enumerate(new_excel_files):
                try:
                    df = self._read_excel_file(file['id'], file['name'], self.excel_config['header_row'])
                    
                    if df.empty:
                        self.log(f"No data extracted from: {file['name']}", "WARNING")
                        continue
                    
                    df[self.excel_config['source_file_column']] = file['name']
                    
                    self.log(f"Data shape: {df.shape} - Columns: {list(df.columns)[:3]}{'...' if len(df.columns) > 3 else ''}", "INFO")
                    
                    self._append_to_sheet_with_source(
                        self.excel_config['spreadsheet_id'], 
                        self.excel_config['sheet_name'], 
                        df, 
                        self.excel_config['source_file_column'],
                        is_first_file and not sheet_has_data
                    )
                    
                    self.log(f"Appended data from: {file['name']}", "SUCCESS")
                    processed_count += 1
                    is_first_file = False
                    
                except Exception as e:
                    self.log(f"Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}", "ERROR")
            
            files_processed = processed_count
            
            self.log(f"Excel workflow completed! Processed {processed_count} new files", "INFO")
            
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
    
    def run_workflow(self):
        """Run complete workflow: Gmail → Excel with Source File Tracking → Log Summary"""
        try:
            self.log("=== Starting Blinkit HOT Workflow ===", "INFO")
            
            overall_start = datetime.now()
            
            gmail_result = self.process_gmail_workflow()
            excel_result = self.process_excel_workflow()
            
            overall_end = datetime.now()
            
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
            
            duration = (overall_end - overall_start).total_seconds() / 60
            self.log(f"=== Workflow Finished ===", "INFO")
            self.log(f"Duration: {duration:.2f} minutes", "INFO")
            self.log(f"Emails checked: {summary_data['emails_checked']}", "INFO")
            self.log(f"Attachments saved: {summary_data['attachments_saved']}", "INFO")
            self.log(f"Total Excel files found: {summary_data['total_files_found']}", "INFO")
            self.log(f"New files processed: {summary_data['new_files_processed']}", "INFO")
            self.log(f"Files processed: {summary_data['files_processed']}", "INFO")
            
            return summary_data['overall_success']
            
        except Exception as e:
            self.log(f"Complete workflow failed: {str(e)}", "ERROR")
            return False
    
    def _get_existing_source_files(self, spreadsheet_id: str, sheet_name: str, source_file_column: str) -> List[str]:
        """Get list of existing source files from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:Z"
            ).execute()
            
            values = result.get('values', [])
            
            if not values or len(values) <= 1:
                return []
            
            headers = values[0]
            try:
                source_col_index = headers.index(source_file_column)
            except ValueError:
                return []
            
            source_files = []
            for row in values[1:]:
                if len(row) > source_col_index and row[source_col_index]:
                    source_files.append(row[source_col_index])
            
            return list(set(source_files))
            
        except HttpError as e:
            if "Unable to parse range" in str(e):
                return []
            else:
                self.log(f"Failed to get existing source files: {str(e)}", "ERROR")
                return []
        except Exception as e:
            self.log(f"Failed to get existing source files: {str(e)}", "ERROR")
            return []
    
    def _check_sheet_has_data(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Check if the sheet already has data"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A"
            ).execute()
            
            values = result.get('values', [])
            return len(values) > 1
            
        except Exception as e:
            self.log(f"Failed to check if sheet has data: {str(e)}", "WARNING")
            return False
    
    def _append_to_sheet_with_source(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, 
                                    source_file_column: str, include_headers: bool):
        """Append DataFrame to Google Sheet with source file column"""
        try:
            columns = [col for col in df.columns if col != source_file_column] + [source_file_column]
            df = df[columns]
            
            if include_headers:
                values = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()
            else:
                values = df.fillna('').astype(str).values.tolist()
            
            if not values:
                self.log("No data to append", "WARNING")
                return
            
            body = {'values': values}
            
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A",
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            self.log(f"Appended {len(values)} rows to Google Sheet", "INFO")
            
        except Exception as e:
            self.log(f"Failed to append to Google Sheet: {str(e)}", "ERROR")

    def _log_summary_to_sheet(self, summary_data: Dict):
        """Log workflow summary to Google Sheet"""
        try:
            summary_row = [
                summary_data['workflow_start'].strftime("%Y-%m-%d %H:%M:%S"),
                summary_data['workflow_end'].strftime("%Y-%m-%d %H:%M:%S"),
                summary_data['emails_checked'],
                summary_data['attachments_saved'],
                summary_data['total_files_found'],
                summary_data['new_files_processed'],
                summary_data['files_processed'],
                "SUCCESS" if summary_data['gmail_success'] else "FAILED",
                "SUCCESS" if summary_data['excel_success'] else "FAILED",
                "SUCCESS" if summary_data['overall_success'] else "FAILED"
            ]
            
            try:
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=self.summary_config['spreadsheet_id'],
                    range=f"{self.summary_config['sheet_name']}!A:A"
                ).execute()
                
                values = result.get('values', [])
                
                if not values:
                    headers = [
                        "Workflow Start", "Workflow End", "Emails Checked", 
                        "Attachments Saved", "Total Files Found", "New Files Processed",
                        "Files Processed", "Gmail Status", "Excel Status", "Overall Status"
                    ]
                    body = {'values': [headers, summary_row]}
                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=self.summary_config['spreadsheet_id'],
                        range=f"{self.summary_config['sheet_name']}!A1",
                        valueInputOption='USER_ENTERED',
                        body=body
                    ).execute()
                else:
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
                if "Unable to parse range" in str(e):
                    headers = [
                        "Workflow Start", "Workflow End", "Emails Checked", 
                        "Attachments Saved", "Total Files Found", "New Files Processed",
                        "Files Processed", "Gmail Status", "Excel Status", "Overall Status"
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
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
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
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender: str, config: dict, base_folder_id: str) -> int:
        """Extract attachments from email with proper folder structure"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, sender, config, base_folder_id
                )
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            if not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                return 0
            
            try:
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                sender_email = sender
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()
                sender_folder_name = self._sanitize_filename(sender_email)
                search_term = config.get('search_term', 'all-attachments')
                search_folder_name = search_term if search_term else "all-attachments"
                file_type_folder = "Excel_Files"
                
                sender_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)
                search_folder_id = self._create_drive_folder(search_folder_name, sender_folder_id)
                type_folder_id = self._create_drive_folder(file_type_folder, search_folder_id)
                
                clean_filename = self._sanitize_filename(filename)
                final_filename = f"{message_id}_{clean_filename}"
                
                if not self._file_exists_in_folder(final_filename, type_folder_id):
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
                    
                    processed_count += 1
                    
            except Exception as e:
                self.log(f"Failed to process attachment {filename}: {str(e)}", "ERROR")
        
        return processed_count
    
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
            request = self.drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_stream.seek(0)
            
            try:
                if header_row == -1:
                    df = pd.read_excel(file_stream, header=None)
                else:
                    df = pd.read_excel(file_stream, header=header_row)
                return self._clean_dataframe(df)
            except Exception as e:
                self.log(f"Standard read failed: {str(e)[:50]}...", "WARNING")
            
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
                worksheet_files = [f for f in zip_ref.namelist() if f.startswith('xl/worksheets/sheet')]
                if not worksheet_files:
                    return pd.DataFrame()
                
                xml_content = zip_ref.read(worksheet_files[0]).decode('utf-8')
                tree = etree.fromstring(xml_content.encode('utf-8'))
                
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
        
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)
        
        if len(df.columns) >= 2:
            second_col = df.columns[1]
            mask = ~(
                df[second_col].isna() | 
                (df[second_col].astype(str).str.strip() == "") |
                (df[second_col].astype(str).str.strip() == "nan")
            )
            df = df[mask]
            self.log(f"After removing blank B column rows: {df.shape}", "INFO")
        
        original_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = original_count - len(df)
        
        if duplicates_removed > 0:
            self.log(f"Removed {duplicates_removed} duplicate rows", "INFO")
        
        self.log(f"Final cleaned DataFrame shape: {df.shape}", "INFO")
        return df


def main():
    """Main function to run the complete workflow once"""
    print("=" * 80)
    print("BLINKIT HOT AUTOMATION WORKFLOW")
    print("=" * 80)
    
    automation = BlinkitHOTScheduler()
    
    print("\nAuthenticating...")
    if not automation.authenticate():
        print("ERROR: Authentication failed. Please check credentials.")
        return 1
    
    print("Authentication successful!")
    
    print("\nRunning complete workflow...")
    try:
        success = automation.run_workflow()
        
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
