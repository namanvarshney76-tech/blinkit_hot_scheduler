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
        
        # Statistics for summary
        self.stats = {
            'gmail': {
                'emails_found': 0,
                'emails_processed': 0,
                'emails_skipped': 0,
                'attachments_found': 0,
                'attachments_uploaded': 0,
                'attachments_skipped': 0,
                'attachments_filtered': 0,
                'start_time': None,
                'end_time': None
            },
            'excel': {
                'files_found': 0,
                'files_processed': 0,
                'files_skipped': 0,
                'files_filtered': 0,
                'rows_added': 0,
                'start_time': None,
                'end_time': None
            },
            'overall': {
                'start_time': None,
                'end_time': None
            }
        }
        
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
        
        # Format log message without timestamp for console (timestamp will be added by logging)
        log_message = message
        if message.startswith("[") and "] " in message:
            # Extract just the message part after the bracket
            log_message = message.split("] ", 1)[1] if "] " in message else message
        
        if level.upper() == "ERROR":
            logging.error(log_message)
        elif level.upper() == "WARNING":
            logging.warning(log_message)
        elif level.upper() == "SUCCESS":
            logging.info(f"✓ {log_message}")
        else:
            logging.info(log_message)
    
    def print_summary(self, workflow_type: str):
        """Print detailed summary for a workflow"""
        print("\n" + "=" * 80)
        print(f"{workflow_type.upper()} WORKFLOW SUMMARY")
        print("=" * 80)
        
        if workflow_type == 'gmail':
            stats = self.stats['gmail']
            duration = (stats['end_time'] - stats['start_time']).total_seconds() if stats['end_time'] and stats['start_time'] else 0
            
            print(f"📧 Emails Found: {stats['emails_found']}")
            print(f"   • Processed: {stats['emails_processed']}")
            print(f"   • Skipped: {stats['emails_skipped']}")
            print(f"")
            print(f"📎 Attachments Found: {stats['attachments_found']}")
            print(f"   • Uploaded to Drive: {stats['attachments_uploaded']}")
            print(f"   • Skipped (already exist): {stats['attachments_skipped']}")
            print(f"   • Filtered (non-Excel): {stats['attachments_filtered']}")
            print(f"")
            print(f"⏱️  Duration: {duration:.2f} seconds")
            
        elif workflow_type == 'excel':
            stats = self.stats['excel']
            duration = (stats['end_time'] - stats['start_time']).total_seconds() if stats['end_time'] and stats['start_time'] else 0
            
            print(f"📁 Excel Files Found: {stats['files_found']}")
            print(f"   • Processed: {stats['files_processed']}")
            print(f"   • Skipped (already in sheet): {stats['files_skipped']}")
            print(f"   • Filtered (other types): {stats['files_filtered']}")
            print(f"")
            print(f"📊 Data Rows Added: {stats['rows_added']}")
            print(f"")
            print(f"⏱️  Duration: {duration:.2f} seconds")
        
        print("=" * 80)
    
    def print_overall_summary(self):
        """Print overall summary of complete workflow"""
        print("\n" + "=" * 80)
        print("OVERALL WORKFLOW SUMMARY")
        print("=" * 80)
        
        gmail_stats = self.stats['gmail']
        excel_stats = self.stats['excel']
        overall_duration = (self.stats['overall']['end_time'] - self.stats['overall']['start_time']).total_seconds()
        
        print(f"📊 GMAIL TO DRIVE:")
        print(f"   • Emails Processed: {gmail_stats['emails_processed']}/{gmail_stats['emails_found']}")
        print(f"   • Attachments Uploaded: {gmail_stats['attachments_uploaded']}")
        
        print(f"")
        print(f"📊 DRIVE TO SHEET:")
        print(f"   • Files Processed: {excel_stats['files_processed']}/{excel_stats['files_found']}")
        print(f"   • Rows Added: {excel_stats['rows_added']}")
        
        print(f"")
        print(f"⏱️  Total Duration: {overall_duration:.2f} seconds")
        
        success_rate = (gmail_stats['emails_processed'] + excel_stats['files_processed']) / max(1, gmail_stats['emails_found'] + excel_stats['files_found'])
        
        if success_rate > 0.8:
            status = "✅ SUCCESS"
        elif success_rate > 0.5:
            status = "⚠️  PARTIAL SUCCESS"
        else:
            status = "❌ FAILED"
        
        print(f"")
        print(f"📈 Success Rate: {success_rate:.1%}")
        print(f"📋 Status: {status}")
        print("=" * 80)
    
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
            
            self.log("Authentication successful!", "SUCCESS")
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
            self.log(f"[GMAIL] Searching with query: {query}", "INFO")
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.log(f"[GMAIL] Found {len(messages)} emails", "INFO")
            
            return messages
            
        except Exception as e:
            self.log(f"[GMAIL] Email search failed: {str(e)}", "ERROR")
            return []
    
    def process_gmail_workflow(self) -> Dict[str, Any]:
        """Process Gmail attachment download workflow"""
        self.stats['gmail']['start_time'] = datetime.now()
        
        try:
            self.log("=" * 80, "INFO")
            self.log("Starting Gmail to Drive workflow...", "INFO")
            self.log("=" * 80, "INFO")
            
            # Search for emails
            emails = self.search_emails(
                sender=self.gmail_config['sender'],
                search_term=self.gmail_config['search_term'],
                days_back=self.gmail_config['days_back'],
                max_results=self.gmail_config['max_results']
            )
            
            self.stats['gmail']['emails_found'] = len(emails)
            self.log(f"📧 Found {len(emails)} emails matching criteria", "INFO")
            
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                self.stats['gmail']['end_time'] = datetime.now()
                return {
                    'success': True, 
                    'processed': 0,
                    'emails_checked': 0,
                    'attachments_saved': 0,
                    'start_time': self.stats['gmail']['start_time'],
                    'end_time': self.stats['gmail']['end_time']
                }
            
            base_folder_name = "Gmail_Attachments"
            base_folder_id = self._create_drive_folder(base_folder_name, self.gmail_config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                self.log("Failed to create base folder in Google Drive", "ERROR")
                self.stats['gmail']['end_time'] = datetime.now()
                return {
                    'success': False, 
                    'processed': 0,
                    'emails_checked': self.stats['gmail']['emails_found'],
                    'attachments_saved': 0,
                    'start_time': self.stats['gmail']['start_time'],
                    'end_time': self.stats['gmail']['end_time']
                }
            
            processed_emails = 0
            skipped_emails = 0
            
            for i, email in enumerate(emails, 1):
                try:
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    self.log(f"📨 Processing email {i}/{len(emails)}: {subject}", "INFO")
                    
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        self.log(f"No payload found for email: {subject}", "WARNING")
                        skipped_emails += 1
                        continue
                    
                    # Extract attachments with detailed stats
                    attachment_stats = self._extract_attachments_from_email(
                        email['id'], message['payload'], sender, self.gmail_config, base_folder_id
                    )
                    
                    uploaded = attachment_stats.get('uploaded', 0)
                    skipped = attachment_stats.get('skipped', 0)
                    filtered = attachment_stats.get('filtered', 0)
                    
                    self.stats['gmail']['attachments_uploaded'] += uploaded
                    self.stats['gmail']['attachments_skipped'] += skipped
                    self.stats['gmail']['attachments_filtered'] += filtered
                    self.stats['gmail']['attachments_found'] += (uploaded + skipped + filtered)
                    
                    if uploaded > 0:
                        processed_emails += 1
                        self.log(f"✓ Found {uploaded} attachments in: {subject}", "SUCCESS")
                        if skipped > 0:
                            self.log(f"  (Skipped {skipped} existing attachments)", "INFO")
                        if filtered > 0:
                            self.log(f"  (Filtered {filtered} non-Excel files)", "INFO")
                    else:
                        skipped_emails += 1
                        if skipped > 0:
                            self.log(f"⚠️ All attachments already exist for: {subject}", "WARNING")
                        elif filtered > 0:
                            self.log(f"⚠️ No Excel attachments found for: {subject}", "WARNING")
                        else:
                            self.log(f"⚠️ No attachments found for: {subject}", "WARNING")
                    
                except Exception as e:
                    self.log(f"❌ Failed to process email {email.get('id', 'unknown')}: {str(e)}", "ERROR")
                    skipped_emails += 1
            
            self.stats['gmail']['emails_processed'] = processed_emails
            self.stats['gmail']['emails_skipped'] = skipped_emails
            self.stats['gmail']['end_time'] = datetime.now()
            
            # Print detailed summary
            self.print_summary('gmail')
            
            return {
                'success': True, 
                'processed': self.stats['gmail']['attachments_uploaded'],
                'emails_checked': self.stats['gmail']['emails_found'],
                'attachments_saved': self.stats['gmail']['attachments_uploaded'],
                'start_time': self.stats['gmail']['start_time'],
                'end_time': self.stats['gmail']['end_time']
            }
            
        except Exception as e:
            self.log(f"❌ Gmail workflow failed: {str(e)}", "ERROR")
            self.stats['gmail']['end_time'] = datetime.now()
            self.print_summary('gmail')
            return {
                'success': False, 
                'processed': 0,
                'emails_checked': self.stats['gmail']['emails_found'],
                'attachments_saved': self.stats['gmail']['attachments_uploaded'],
                'start_time': self.stats['gmail']['start_time'],
                'end_time': self.stats['gmail']['end_time']
            }
    
    def process_excel_workflow(self) -> Dict[str, Any]:
        """Process Excel GRN workflow with source file tracking"""
        self.stats['excel']['start_time'] = datetime.now()
        
        try:
            self.log("=" * 80, "INFO")
            self.log("Starting Drive to Sheet workflow...", "INFO")
            self.log("=" * 80, "INFO")
            
            # Step 1: Get total Excel files with 'GRN' in name from Drive folder
            all_files = self._get_files_in_folder(
                self.excel_config['excel_folder_id'], 
                self.excel_config['days_back'], 
                self.excel_config['max_results']
            )
            
            # Filter for Excel files with 'GRN' in name
            excel_files = []
            other_files = []
            
            for file in all_files:
                if (file['mimeType'] in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                                         'application/vnd.ms-excel'] and 
                    'GRN' in file['name'].upper()):
                    excel_files.append(file)
                else:
                    other_files.append(file)
            
            self.stats['excel']['files_found'] = len(excel_files)
            self.stats['excel']['files_filtered'] = len(other_files)
            
            self.log(f"📁 Found {len(excel_files)} Excel files with 'GRN' in name", "INFO")
            if other_files:
                self.log(f"   (Filtered {len(other_files)} other files)", "INFO")
            
            if not excel_files:
                self.log("No Excel files with 'GRN' found", "WARNING")
                self.stats['excel']['end_time'] = datetime.now()
                self.print_summary('excel')
                return {
                    'success': True, 
                    'processed': 0,
                    'files_processed': 0,
                    'new_files_count': 0,
                    'total_files_found': 0,
                    'start_time': self.stats['excel']['start_time'],
                    'end_time': self.stats['excel']['end_time']
                }
            
            # Step 2: Get existing source files from Google Sheet
            existing_source_files = self._get_existing_source_files(
                self.excel_config['spreadsheet_id'], 
                self.excel_config['sheet_name'],
                self.excel_config['source_file_column']
            )
            
            self.log(f"📋 Found {len(existing_source_files)} existing files in sheet", "INFO")
            
            # Step 3: Filter out files that are already in the sheet
            new_excel_files = []
            skipped_excel_files = []
            
            for file in excel_files:
                if file['name'] not in existing_source_files:
                    new_excel_files.append(file)
                else:
                    skipped_excel_files.append(file)
            
            self.stats['excel']['files_skipped'] = len(skipped_excel_files)
            
            self.log(f"📊 Files to process: {len(new_excel_files)} new, {len(skipped_excel_files)} already in sheet", "INFO")
            
            if skipped_excel_files:
                self.log(f"📋 Skipped files:", "INFO")
                for file in skipped_excel_files[:5]:  # Show first 5
                    self.log(f"   • {file['name']}", "INFO")
                if len(skipped_excel_files) > 5:
                    self.log(f"   ... and {len(skipped_excel_files) - 5} more", "INFO")
            
            if not new_excel_files:
                self.log("All files already processed in previous runs", "INFO")
                self.stats['excel']['end_time'] = datetime.now()
                self.print_summary('excel')
                return {
                    'success': True, 
                    'processed': 0,
                    'files_processed': 0,
                    'new_files_count': 0,
                    'total_files_found': len(excel_files),
                    'start_time': self.stats['excel']['start_time'],
                    'end_time': self.stats['excel']['end_time']
                }
            
            # Step 4: Process new files
            processed_count = 0
            sheet_has_data = self._check_sheet_has_data(
                self.excel_config['spreadsheet_id'], 
                self.excel_config['sheet_name']
            )
            
            is_first_file = True
            
            for i, file in enumerate(new_excel_files, 1):
                try:
                    self.log(f"🔄 Processing file {i}/{len(new_excel_files)}: {file['name']}", "INFO")
                    
                    # Read Excel file
                    df = self._read_excel_file(file['id'], file['name'], self.excel_config['header_row'])
                    
                    if df.empty:
                        self.log(f"⚠️ No data extracted from: {file['name']}", "WARNING")
                        continue
                    
                    # Add source file column to DataFrame
                    df[self.excel_config['source_file_column']] = file['name']
                    
                    self.log(f"📈 Data shape: {df.shape} rows × {df.shape[1]} columns", "INFO")
                    
                    # Append to Google Sheet
                    rows_added = self._append_to_sheet_with_source(
                        self.excel_config['spreadsheet_id'], 
                        self.excel_config['sheet_name'], 
                        df, 
                        self.excel_config['source_file_column'],
                        is_first_file and not sheet_has_data
                    )
                    
                    if rows_added > 0:
                        self.log(f"✅ Appended {rows_added} rows from: {file['name']}", "SUCCESS")
                        self.stats['excel']['rows_added'] += rows_added
                        processed_count += 1
                        is_first_file = False
                    else:
                        self.log(f"⚠️ No rows added from: {file['name']}", "WARNING")
                    
                except Exception as e:
                    self.log(f"❌ Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}", "ERROR")
            
            self.stats['excel']['files_processed'] = processed_count
            self.stats['excel']['end_time'] = datetime.now()
            
            # Print detailed summary
            self.print_summary('excel')
            
            return {
                'success': True, 
                'processed': processed_count,
                'files_processed': processed_count,
                'new_files_count': len(new_excel_files),
                'total_files_found': len(excel_files),
                'start_time': self.stats['excel']['start_time'],
                'end_time': self.stats['excel']['end_time']
            }
            
        except Exception as e:
            self.log(f"❌ Excel workflow failed: {str(e)}", "ERROR")
            self.stats['excel']['end_time'] = datetime.now()
            self.print_summary('excel')
            return {
                'success': False, 
                'processed': 0,
                'files_processed': self.stats['excel']['files_processed'],
                'new_files_count': 0,
                'total_files_found': self.stats['excel']['files_found'],
                'start_time': self.stats['excel']['start_time'],
                'end_time': self.stats['excel']['end_time']
            }
    
    def run_workflow(self):
        """Run complete workflow: Gmail → Excel with Source File Tracking → Log Summary"""
        try:
            self.stats['overall']['start_time'] = datetime.now()
            
            self.log("\n" + "=" * 80, "INFO")
            self.log("🚀 STARTING COMPLETE BLINKIT HOT WORKFLOW", "INFO")
            self.log("=" * 80, "INFO")
            
            # Step 1: Run Gmail workflow
            gmail_result = self.process_gmail_workflow()
            
            # Small delay between workflows
            time.sleep(2)
            
            # Step 2: Run Excel workflow with source file tracking
            excel_result = self.process_excel_workflow()
            
            self.stats['overall']['end_time'] = datetime.now()
            
            # Step 3: Log summary to sheet
            summary_data = {
                'workflow_start': self.stats['overall']['start_time'],
                'workflow_end': self.stats['overall']['end_time'],
                'emails_checked': self.stats['gmail']['emails_found'],
                'attachments_uploaded': self.stats['gmail']['attachments_uploaded'],
                'attachments_skipped': self.stats['gmail']['attachments_skipped'],
                'attachments_filtered': self.stats['gmail']['attachments_filtered'],
                'total_files_found': self.stats['excel']['files_found'],
                'files_processed': self.stats['excel']['files_processed'],
                'files_skipped': self.stats['excel']['files_skipped'],
                'files_filtered': self.stats['excel']['files_filtered'],
                'rows_added': self.stats['excel']['rows_added'],
                'gmail_success': gmail_result.get('success', False),
                'excel_success': excel_result.get('success', False),
                'overall_success': gmail_result.get('success', False) and excel_result.get('success', False)
            }
            
            self._log_summary_to_sheet(summary_data)
            
            # Print overall summary
            self.print_overall_summary()
            
            return summary_data['overall_success']
            
        except Exception as e:
            self.log(f"❌ Complete workflow failed: {str(e)}", "ERROR")
            self.stats['overall']['end_time'] = datetime.now()
            self.print_overall_summary()
            return False
    
    def _get_files_in_folder(self, folder_id: str, days_back: int, max_results: int) -> List[Dict]:
        """Get all files from Drive folder"""
        try:
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00')
            query = f"'{folder_id}' in parents and trashed=false and modifiedTime > '{start_date}'"
            results = self.drive_service.files().list(
                q=query,
                pageSize=max_results,
                fields="files(id, name, mimeType)",
                orderBy="modifiedTime desc"
            ).execute()
            
            files = results.get('files', [])
            return files
            
        except Exception as e:
            self.log(f"Failed to get files from folder: {str(e)}", "ERROR")
            return []
    
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
                                    source_file_column: str, include_headers: bool) -> int:
        """Append DataFrame to Google Sheet with source file column, returns number of rows added"""
        try:
            columns = [col for col in df.columns if col != source_file_column] + [source_file_column]
            df = df[columns]
            
            if include_headers:
                values = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()
            else:
                values = df.fillna('').astype(str).values.tolist()
            
            if not values:
                self.log("No data to append", "WARNING")
                return 0
            
            body = {'values': values}
            
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A",
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            rows_added = len(values) if not include_headers else len(values) - 1
            return rows_added
            
        except Exception as e:
            self.log(f"Failed to append to Google Sheet: {str(e)}", "ERROR")
            return 0

    def _log_summary_to_sheet(self, summary_data: Dict):
        """Log workflow summary to Google Sheet"""
        try:
            summary_row = [
                summary_data['workflow_start'].strftime("%Y-%m-%d %H:%M:%S"),
                summary_data['workflow_end'].strftime("%Y-%m-%d %H:%M:%S"),
                summary_data['emails_checked'],
                summary_data['attachments_uploaded'],
                summary_data['attachments_skipped'],
                summary_data['attachments_filtered'],
                summary_data['total_files_found'],
                summary_data['files_processed'],
                summary_data['files_skipped'],
                summary_data['files_filtered'],
                summary_data['rows_added'],
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
                        "Attachments Uploaded", "Attachments Skipped", "Attachments Filtered",
                        "Total Files Found", "Files Processed", "Files Skipped", "Files Filtered",
                        "Rows Added", "Gmail Status", "Excel Status", "Overall Status"
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
                
                self.log("Workflow summary logged to Google Sheet", "SUCCESS")
                
            except HttpError as e:
                if "Unable to parse range" in str(e):
                    headers = [
                        "Workflow Start", "Workflow End", "Emails Checked", 
                        "Attachments Uploaded", "Attachments Skipped", "Attachments Filtered",
                        "Total Files Found", "Files Processed", "Files Skipped", "Files Filtered",
                        "Rows Added", "Gmail Status", "Excel Status", "Overall Status"
                    ]
                    body = {'values': [headers, summary_row]}
                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=self.summary_config['spreadsheet_id'],
                        range=f"{self.summary_config['sheet_name']}!A1",
                        valueInputOption='USER_ENTERED',
                        body=body
                    ).execute()
                    self.log("Created summary sheet and logged workflow data", "SUCCESS")
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
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender: str, config: dict, base_folder_id: str) -> Dict[str, int]:
        """Extract attachments from email with proper folder structure, returns statistics"""
        stats = {'uploaded': 0, 'skipped': 0, 'filtered': 0}
        
        if "parts" in payload:
            for part in payload["parts"]:
                part_stats = self._extract_attachments_from_email(
                    message_id, part, sender, config, base_folder_id
                )
                stats['uploaded'] += part_stats['uploaded']
                stats['skipped'] += part_stats['skipped']
                stats['filtered'] += part_stats['filtered']
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            # Filter for Excel files only
            if not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                stats['filtered'] += 1
                return stats
            
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
                    
                    stats['uploaded'] += 1
                else:
                    stats['skipped'] += 1
                    
            except Exception as e:
                self.log(f"Failed to process attachment {filename}: {str(e)}", "ERROR")
        
        return stats
    
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
                    df = pd.read_excel(file_stream, header=None, engine='openpyxl')
                else:
                    df = pd.read_excel(file_stream, header=header_row, engine='openpyxl')
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
        
        original_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = original_count - len(df)
        
        if duplicates_removed > 0:
            self.log(f"Removed {duplicates_removed} duplicate rows", "INFO")
        
        return df


def main():
    """Main function to run the complete workflow once"""
    print("=" * 80)
    print("🚀 BLINKIT HOT AUTOMATION WORKFLOW")
    print("=" * 80)
    
    automation = BlinkitHOTScheduler()
    
    print("\n🔐 Authenticating...")
    if not automation.authenticate():
        print("❌ ERROR: Authentication failed. Please check credentials.")
        return 1
    
    print("✅ Authentication successful!")
    
    print("\n🔄 Running complete workflow...")
    try:
        success = automation.run_workflow()
        
        if success:
            print("\n" + "=" * 80)
            print("✅ WORKFLOW COMPLETED SUCCESSFULLY")
            print("=" * 80)
        else:
            print("\n" + "=" * 80)
            print("⚠️  WORKFLOW COMPLETED WITH ERRORS")
            print("=" * 80)
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"\n❌ WORKFLOW FAILED: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
