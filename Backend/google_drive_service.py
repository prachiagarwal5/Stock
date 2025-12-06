"""
Google Drive Service Module
Handles authentication, folder management, and file uploads to Google Drive
"""

import os
import json
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery
from io import BytesIO


class GoogleDriveService:
    """Service for interacting with Google Drive API"""
    
    SCOPES = ['https://www.googleapis.com/auth/drive']
    TOKEN_FILE = 'token.pickle'
    CREDENTIALS_FILE = 'credentials.json'
    
    def __init__(self, credentials_path=None):
        """
        Initialize Google Drive Service
        
        Args:
            credentials_path: Path to credentials.json file
        """
        self.service = None
        self.credentials = None
        self.credentials_path = credentials_path or self.CREDENTIALS_FILE
        self.automation_folder_id = None
    
    def authenticate(self):
        """
        Authenticate with Google Drive API
        Uses OAuth 2.0 flow for user authentication
        """
        try:
            # Check if token.pickle exists (saved credentials)
            if os.path.exists(self.TOKEN_FILE):
                with open(self.TOKEN_FILE, 'rb') as token:
                    self.credentials = pickle.load(token)
            
            # If credentials are expired, refresh them
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(Request())
            
            # If no valid credentials, use OAuth flow
            if not self.credentials or not self.credentials.valid:
                if not os.path.exists(self.credentials_path):
                    raise FileNotFoundError(
                        f"❌ {self.credentials_path} not found. "
                        "Please download it from Google Cloud Console."
                    )
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.SCOPES
                )
                self.credentials = flow.run_local_server(port=0)
                
                # Save credentials for next run
                with open(self.TOKEN_FILE, 'wb') as token:
                    pickle.dump(self.credentials, token)
            
            # Build the Drive service
            self.service = discovery.build('drive', 'v3', credentials=self.credentials)
            print("✅ Google Drive authenticated successfully")
            return True
        
        except FileNotFoundError as e:
            print(f"❌ Authentication error: {e}")
            return False
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            return False
    
    def get_or_create_automation_folder(self):
        """
        Get or create 'Automation' folder in Google Drive
        
        Returns:
            Folder ID if successful, None otherwise
        """
        try:
            if not self.service:
                if not self.authenticate():
                    return None
            
            # Search for existing 'Automation' folder in root directory
            query = "name='Automation' and mimeType='application/vnd.google-apps.folder' and trashed=false and 'root' in parents"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=1
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                # Folder exists
                self.automation_folder_id = files[0]['id']
                print(f"✅ Found existing 'Automation' folder: {self.automation_folder_id}")
                return self.automation_folder_id
            
            # Create new 'Automation' folder
            file_metadata = {
                'name': 'Automation',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            
            self.automation_folder_id = folder.get('id')
            print(f"✅ Created new 'Automation' folder: {self.automation_folder_id}")
            return self.automation_folder_id
        
        except Exception as e:
            print(f"❌ Error creating/finding Automation folder: {e}")
            return None
    
    def upload_file(self, file_path, file_name=None, folder_id=None):
        """
        Upload file to Google Drive
        
        Args:
            file_path: Path to file to upload
            file_name: Name for file in Google Drive (default: original name)
            folder_id: Folder ID to upload to (default: Automation folder)
        
        Returns:
            File ID if successful, None otherwise
        """
        try:
            if not self.service:
                if not self.authenticate():
                    return None
            
            # Use Automation folder if no folder specified
            if not folder_id:
                folder_id = self.get_or_create_automation_folder()
                if not folder_id:
                    return None
            
            # Use original filename if not specified
            if not file_name:
                file_name = os.path.basename(file_path)
            
            # Check if file exists in folder, delete if it does (to avoid duplicates)
            self._delete_existing_file(file_name, folder_id)
            
            # Prepare file metadata
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            
            # Upload file
            from googleapiclient.http import MediaFileUpload
            
            media = MediaFileUpload(file_path, resumable=True)
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            
            file_id = file.get('id')
            web_link = file.get('webViewLink')
            
            print(f"✅ File uploaded successfully: {file_name} ({file_id})")
            print(f"   View: {web_link}")
            
            return {
                'file_id': file_id,
                'file_name': file_name,
                'web_link': web_link
            }
        
        except Exception as e:
            print(f"❌ Error uploading file: {e}")
            return None
    
    def upload_file_from_bytes(self, file_bytes, file_name, folder_id=None):
        """
        Upload file from bytes/BytesIO to Google Drive
        
        Args:
            file_bytes: File content as bytes or BytesIO
            file_name: Name for file in Google Drive
            folder_id: Folder ID to upload to (default: Automation folder)
        
        Returns:
            File ID if successful, None otherwise
        """
        try:
            if not self.service:
                if not self.authenticate():
                    return None
            
            # Use Automation folder if no folder specified
            if not folder_id:
                folder_id = self.get_or_create_automation_folder()
                if not folder_id:
                    return None
            
            # Check if file exists in folder, delete if it does
            self._delete_existing_file(file_name, folder_id)
            
            # Prepare file metadata
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            
            # Convert bytes to BytesIO if needed
            if isinstance(file_bytes, bytes):
                file_bytes = BytesIO(file_bytes)
            
            # Upload file
            from googleapiclient.http import MediaIoBaseUpload
            
            media = MediaIoBaseUpload(
                file_bytes,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                resumable=True
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            
            file_id = file.get('id')
            web_link = file.get('webViewLink')
            
            print(f"✅ File uploaded successfully: {file_name} ({file_id})")
            print(f"   View: {web_link}")
            
            return {
                'file_id': file_id,
                'file_name': file_name,
                'web_link': web_link
            }
        
        except Exception as e:
            print(f"❌ Error uploading file: {e}")
            return None
    
    def _delete_existing_file(self, file_name, folder_id):
        """
        Delete file with same name in folder (to avoid duplicates)
        
        Args:
            file_name: Name of file to delete
            folder_id: Folder ID to search in
        """
        try:
            query = f"name='{file_name}' and trashed=false and '{folder_id}' in parents"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id)',
                pageSize=10
            ).execute()
            
            files = results.get('files', [])
            
            for file in files:
                self.service.files().delete(fileId=file['id']).execute()
                print(f"⚠️ Deleted existing file: {file_name}")
        
        except Exception as e:
            print(f"⚠️ Warning while checking for existing file: {e}")
    
    def list_files_in_automation_folder(self):
        """
        List all files in Automation folder
        
        Returns:
            List of files with id, name, and link
        """
        try:
            if not self.service:
                if not self.authenticate():
                    return []
            
            folder_id = self.get_or_create_automation_folder()
            if not folder_id:
                return []
            
            query = f"trashed=false and '{folder_id}' in parents"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, createdTime, webViewLink, size)',
                pageSize=50,
                orderBy='createdTime desc'
            ).execute()
            
            files = results.get('files', [])
            print(f"✅ Found {len(files)} files in Automation folder")
            
            return files
        
        except Exception as e:
            print(f"❌ Error listing files: {e}")
            return []
    
    def get_file_link(self, file_id):
        """
        Get shareable link for file
        
        Args:
            file_id: ID of file
        
        Returns:
            Shareable link
        """
        try:
            if not self.service:
                return None
            
            file = self.service.files().get(
                fileId=file_id,
                fields='webViewLink'
            ).execute()
            
            return file.get('webViewLink')
        
        except Exception as e:
            print(f"❌ Error getting file link: {e}")
            return None
    
    def is_authenticated(self):
        """Check if service is authenticated"""
        return self.service is not None and self.credentials is not None
