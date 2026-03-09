"""
Outlook Email Attachments to OneDrive Sync
Automatically saves email attachments to OneDrive folders
"""

import os
import json
import msal
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('email_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MicrosoftGraphClient:
    """Client for Microsoft Graph API operations"""
    
    GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
    SCOPES = ["https://graph.microsoft.com/.default"]
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.access_token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        
        # Initialize MSAL confidential client
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}"
        )
    
    def get_access_token(self) -> str:
        """Get or refresh access token"""
        # Check if token is still valid
        if self.access_token and self.token_expires:
            if datetime.now() < self.token_expires - timedelta(minutes=5):
                return self.access_token
        
        # Acquire new token
        result = self.app.acquire_token_for_client(scopes=self.SCOPES)
        
        if "access_token" in result:
            self.access_token = result["access_token"]
            # Token typically expires in 1 hour
            self.token_expires = datetime.now() + timedelta(seconds=result.get("expires_in", 3600))
            logger.info("Successfully acquired access token")
            return self.access_token
        else:
            error = result.get("error_description", "Unknown error")
            logger.error(f"Failed to acquire token: {error}")
            raise Exception(f"Token acquisition failed: {error}")
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        files: Optional[Dict] = None,
        content_type: str = "application/json"
    ) -> requests.Response:
        """Make authenticated request to Graph API"""
        token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
        }
        
        if content_type and not files:
            headers["Content-Type"] = content_type
        
        url = f"{self.GRAPH_API_BASE}{endpoint}"
        
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data if method in ["POST", "PUT", "PATCH"] and not files else None,
            data=files
        )
        
        return response


class OutlookEmailClient(MicrosoftGraphClient):
    """Client for Outlook email operations"""
    
    def __init__(self, user_email: str, **kwargs):
        super().__init__(**kwargs)
        self.user_email = user_email
    
    def get_emails_with_attachments(
        self,
        folder: str = "inbox",
        unread_only: bool = True,
        since_datetime: Optional[datetime] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Fetch emails that have attachments
        
        Args:
            folder: Email folder (inbox, sentitems, etc.)
            unread_only: Only fetch unread emails
            since_datetime: Only fetch emails after this datetime
            limit: Maximum number of emails to fetch
        """
        # Build filter query
        filters = ["hasAttachments eq true"]
        
        if unread_only:
            filters.append("isRead eq false")
        
        if since_datetime:
            iso_date = since_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            filters.append(f"receivedDateTime ge {iso_date}")
        
        filter_query = " and ".join(filters)
        
        endpoint = f"/users/{self.user_email}/mailFolders/{folder}/messages"
        params = f"?$filter={filter_query}&$top={limit}&$orderby=receivedDateTime desc"
        
        response = self._make_request("GET", endpoint + params)
        
        if response.status_code == 200:
            emails = response.json().get("value", [])
            logger.info(f"Found {len(emails)} emails with attachments")
            return emails
        else:
            logger.error(f"Failed to fetch emails: {response.text}")
            return []
    
    def get_email_attachments(self, message_id: str) -> List[Dict[str, Any]]:
        """Get all attachments for a specific email"""
        endpoint = f"/users/{self.user_email}/messages/{message_id}/attachments"
        
        response = self._make_request("GET", endpoint)
        
        if response.status_code == 200:
            attachments = response.json().get("value", [])
            logger.info(f"Found {len(attachments)} attachments for message {message_id}")
            return attachments
        else:
            logger.error(f"Failed to fetch attachments: {response.text}")
            return []
    
    def mark_email_as_read(self, message_id: str) -> bool:
        """Mark an email as read"""
        endpoint = f"/users/{self.user_email}/messages/{message_id}"
        data = {"isRead": True}
        
        response = self._make_request("PATCH", endpoint, data=data)
        
        if response.status_code == 200:
            logger.info(f"Marked message {message_id} as read")
            return True
        else:
            logger.error(f"Failed to mark message as read: {response.text}")
            return False


class OneDriveClient(MicrosoftGraphClient):
    """Client for OneDrive operations"""
    
    def __init__(self, user_email: str, **kwargs):
        super().__init__(**kwargs)
        self.user_email = user_email
    
    def create_folder(self, folder_path: str) -> Optional[Dict[str, Any]]:
        """
        Create a folder in OneDrive (creates parent folders if needed)
        
        Args:
            folder_path: Path like "/EmailAttachments/2024/January"
        """
        # Split path into parts
        parts = [p for p in folder_path.split("/") if p]
        current_path = ""
        
        for part in parts:
            parent_path = current_path if current_path else "root"
            endpoint = f"/users/{self.user_email}/drive/{parent_path}/children"
            
            # Check if folder exists
            check_endpoint = f"/users/{self.user_email}/drive/root:/{current_path}/{part}" if current_path else f"/users/{self.user_email}/drive/root:/{part}"
            check_response = self._make_request("GET", check_endpoint)
            
            if check_response.status_code == 200:
                current_path = f"{current_path}/{part}" if current_path else part
                continue
            
            # Create folder
            if current_path:
                create_endpoint = f"/users/{self.user_email}/drive/root:/{current_path}:/children"
            else:
                create_endpoint = f"/users/{self.user_email}/drive/root/children"
            
            data = {
                "name": part,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "fail"
            }
            
            response = self._make_request("POST", create_endpoint, data=data)
            
            if response.status_code in [201, 200]:
                logger.info(f"Created folder: {part}")
                current_path = f"{current_path}/{part}" if current_path else part
            elif response.status_code == 409:
                # Folder already exists
                current_path = f"{current_path}/{part}" if current_path else part
            else:
                logger.error(f"Failed to create folder {part}: {response.text}")
                return None
        
        return {"path": folder_path}
    
    def upload_file(
        self,
        folder_path: str,
        file_name: str,
        file_content: bytes,
        conflict_behavior: str = "rename"
    ) -> Optional[Dict[str, Any]]:
        """
        Upload a file to OneDrive
        
        Args:
            folder_path: Destination folder path
            file_name: Name of the file
            file_content: File content as bytes
            conflict_behavior: What to do on conflict (rename, replace, fail)
        """
        # Sanitize file name
        safe_file_name = self._sanitize_filename(file_name)
        
        # For files larger than 4MB, use upload session
        if len(file_content) > 4 * 1024 * 1024:
            return self._upload_large_file(folder_path, safe_file_name, file_content)
        
        # Small file upload
        endpoint = f"/users/{self.user_email}/drive/root:/{folder_path}/{safe_file_name}:/content"
        params = f"?@microsoft.graph.conflictBehavior={conflict_behavior}"
        
        token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }
        
        url = f"{self.GRAPH_API_BASE}{endpoint}{params}"
        response = requests.put(url, headers=headers, data=file_content)
        
        if response.status_code in [200, 201]:
            result = response.json()
            logger.info(f"Uploaded file: {safe_file_name} to {folder_path}")
            return result
        else:
            logger.error(f"Failed to upload file: {response.text}")
            return None
    
    def _upload_large_file(
        self,
        folder_path: str,
        file_name: str,
        file_content: bytes
    ) -> Optional[Dict[str, Any]]:
        """Upload large files using upload session"""
        # Create upload session
        endpoint = f"/users/{self.user_email}/drive/root:/{folder_path}/{file_name}:/createUploadSession"
        data = {
            "item": {
                "@microsoft.graph.conflictBehavior": "rename",
                "name": file_name
            }
        }
        
        response = self._make_request("POST", endpoint, data=data)
        
        if response.status_code != 200:
            logger.error(f"Failed to create upload session: {response.text}")
            return None
        
        upload_url = response.json().get("uploadUrl")
        
        # Upload in chunks
        chunk_size = 10 * 1024 * 1024  # 10 MB chunks
        file_size = len(file_content)
        
        for i in range(0, file_size, chunk_size):
            chunk_end = min(i + chunk_size - 1, file_size - 1)
            chunk = file_content[i:chunk_end + 1]
            
            headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {i}-{chunk_end}/{file_size}"
            }
            
            response = requests.put(upload_url, headers=headers, data=chunk)
            
            if response.status_code not in [200, 201, 202]:
                logger.error(f"Failed to upload chunk: {response.text}")
                return None
        
        logger.info(f"Uploaded large file: {file_name}")
        return response.json()
    
    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """Remove invalid characters from filename"""
        invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename


class EmailAttachmentSync:
    """
    Main class to sync email attachments to OneDrive
    """
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        user_email: str,
        onedrive_base_folder: str = "/EmailAttachments"
    ):
        self.user_email = user_email
        self.onedrive_base_folder = onedrive_base_folder.strip("/")
        
        # Initialize clients
        auth_params = {
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id
        }
        
        self.outlook_client = OutlookEmailClient(
            user_email=user_email,
            **auth_params
        )
        
        self.onedrive_client = OneDriveClient(
            user_email=user_email,
            **auth_params
        )
        
        # Track processed emails
        self.processed_file = "processed_emails.json"
        self.processed_emails = self._load_processed_emails()
    
    def _load_processed_emails(self) -> set:
        """Load set of already processed email IDs"""
        if os.path.exists(self.processed_file):
            with open(self.processed_file, "r") as f:
                return set(json.load(f))
        return set()
    
    def _save_processed_emails(self):
        """Save processed email IDs"""
        with open(self.processed_file, "w") as f:
            json.dump(list(self.processed_emails), f)
    
    def _generate_folder_path(
        self,
        email: Dict[str, Any],
        organize_by: str = "date"
    ) -> str:
        """
        Generate folder path based on organization strategy
        
        Args:
            email: Email data
            organize_by: Organization strategy (date, sender, subject)
        """
        base = self.onedrive_base_folder
        
        if organize_by == "date":
            received = email.get("receivedDateTime", "")
            if received:
                dt = datetime.fromisoformat(received.replace("Z", "+00:00"))
                return f"{base}/{dt.year}/{dt.strftime('%m-%B')}/{dt.strftime('%d')}"
            return f"{base}/Unknown"
        
        elif organize_by == "sender":
            sender = email.get("from", {}).get("emailAddress", {}).get("address", "Unknown")
            # Sanitize sender email for folder name
            safe_sender = sender.replace("@", "_at_").replace(".", "_")
            return f"{base}/{safe_sender}"
        
        elif organize_by == "subject":
            subject = email.get("subject", "No Subject")
            # Sanitize subject for folder name
            safe_subject = "".join(c if c.isalnum() or c in " -_" else "_" for c in subject)
            safe_subject = safe_subject[:50]  # Limit length
            return f"{base}/{safe_subject}"
        
        return base
    
    def process_emails(
        self,
        folder: str = "inbox",
        unread_only: bool = True,
        since_hours: int = 24,
        organize_by: str = "date",
        mark_as_read: bool = True
    ) -> Dict[str, Any]:
        """
        Process emails and save attachments to OneDrive
        
        Args:
            folder: Email folder to check
            unread_only: Only process unread emails
            since_hours: Only check emails from last N hours
            organize_by: How to organize files (date, sender, subject)
            mark_as_read: Mark processed emails as read
        
        Returns:
            Summary of processed items
        """
        summary = {
            "emails_processed": 0,
            "attachments_saved": 0,
            "errors": [],
            "saved_files": []
        }
        
        since_datetime = datetime.utcnow() - timedelta(hours=since_hours)
        
        # Get emails with attachments
        emails = self.outlook_client.get_emails_with_attachments(
            folder=folder,
            unread_only=unread_only,
            since_datetime=since_datetime
        )
        
        for email in emails:
            message_id = email.get("id")
            
            # Skip if already processed
            if message_id in self.processed_emails:
                logger.info(f"Skipping already processed email: {email.get('subject')}")
                continue
            
            logger.info(f"Processing email: {email.get('subject')}")
            
            try:
                # Get attachments
                attachments = self.outlook_client.get_email_attachments(message_id)
                
                # Generate folder path
                folder_path = self._generate_folder_path(email, organize_by)
                
                # Create folder if needed
                self.onedrive_client.create_folder(folder_path)
                
                for attachment in attachments:
                    # Skip non-file attachments (like inline images references)
                    if attachment.get("@odata.type") != "#microsoft.graph.fileAttachment":
                        continue
                    
                    file_name = attachment.get("name", "unknown_file")
                    content_bytes = attachment.get("contentBytes", "")
                    
                    if not content_bytes:
                        continue
                    
                    # Decode base64 content
                    import base64
                    file_content = base64.b64decode(content_bytes)
                    
                    # Upload to OneDrive
                    result = self.onedrive_client.upload_file(
                        folder_path=folder_path,
                        file_name=file_name,
                        file_content=file_content
                    )
                    
                    if result:
                        summary["attachments_saved"] += 1
                        summary["saved_files"].append({
                            "name": file_name,
                            "path": folder_path,
                            "email_subject": email.get("subject")
                        })
                
                # Mark as processed
                self.processed_emails.add(message_id)
                summary["emails_processed"] += 1
                
                # Mark email as read if requested
                if mark_as_read:
                    self.outlook_client.mark_email_as_read(message_id)
                
            except Exception as e:
                error_msg = f"Error processing email {email.get('subject')}: {str(e)}"
                logger.error(error_msg)
                summary["errors"].append(error_msg)
        
        # Save processed emails
        self._save_processed_emails()
        
        logger.info(f"Sync complete. Emails: {summary['emails_processed']}, Attachments: {summary['attachments_saved']}")
        return summary


def run_sync():
    """Run the sync process"""
    # Load configuration
    config = {
        "client_id": os.getenv("AZURE_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_CLIENT_SECRET"),
        "tenant_id": os.getenv("AZURE_TENANT_ID"),
        "user_email": os.getenv("USER_EMAIL"),
        "onedrive_base_folder": os.getenv("ONEDRIVE_FOLDER_PATH", "/EmailAttachments")
    }
    
    # Validate configuration
    missing = [k for k, v in config.items() if not v]
    if missing:
        logger.error(f"Missing configuration: {missing}")
        return
    
    # Create sync instance
    sync = EmailAttachmentSync(**config)
    
    # Run sync
    summary = sync.process_emails(
        folder="inbox",
        unread_only=True,
        since_hours=24,
        organize_by="date",  # Options: date, sender, subject
        mark_as_read=True
    )
    
    return summary


def run_scheduled():
    """Run sync on a schedule"""
    import schedule
    
    interval = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
    
    logger.info(f"Starting scheduled sync every {interval} minutes")
    
    # Run immediately on start
    run_sync()
    
    # Schedule regular runs
    schedule.every(interval).minutes.do(run_sync)
    
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--scheduled":
        run_scheduled()
    else:
        run_sync()