"""
Script to fetch DREF/EAP Summary documents from IFRC GO Admin API.

This script:
1. Fetches appeal documents from the IFRC API
2. Filters documents where type == "DREF/EAP Summary"
3. Creates a pandas DataFrame with id, name, and document_url
4. Saves the data as Excel
5. Downloads all documents to a local folder
"""

import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse
import time

# Get the project root directory (one level up from scripts folder)
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Load environment variables from project root
load_dotenv(PROJECT_ROOT / ".env")

# Configuration
API_ENDPOINT = "https://goadmin-stage.ifrc.org/api/v2/appeal_document/"
API_TOKEN = os.getenv("IFRC_API_TOKEN")
DOCUMENT_TYPE_FILTER = "DREF/EAP Summary"

# Output paths - documents folder at project root level
DOCUMENTS_FOLDER = PROJECT_ROOT / "downloaded_documents"
OUTPUT_EXCEL = DOCUMENTS_FOLDER / "appeal_documents.xlsx"


def fetch_all_appeal_documents() -> list:
    """
    Fetch all appeal documents from the API, handling pagination with limit and offset.
    
    Returns:
        list: All appeal documents from the API
    """
    if not API_TOKEN:
        raise ValueError("IFRC_API_TOKEN not found in environment variables!")
    
    headers = {
        "Authorization": API_TOKEN
    }
    
    all_documents = []
    limit = 50  # Records per page
    offset = 0
    
    print("Fetching appeal documents from API...")
    
    while True:
        url = f"{API_ENDPOINT}?limit={limit}&offset={offset}"
        print(f"  Fetching records {offset + 1} to {offset + limit}...")
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        results = data.get("results", [])
        
        if not results:
            # No more results, stop pagination
            break
        
        all_documents.extend(results)
        
        # Check if we've fetched all records
        total_count = data.get("count", 0)
        if offset + limit >= total_count:
            break
        
        offset += limit
    
    print(f"Total documents fetched: {len(all_documents)}")
    return all_documents


def filter_by_type(documents: list, doc_type: str) -> list:
    """
    Filter documents by their type field.
    
    Args:
        documents: List of document dictionaries
        doc_type: The type to filter by
        
    Returns:
        list: Filtered documents
    """
    filtered = [doc for doc in documents if doc.get("type") == doc_type]
    print(f"Documents matching type '{doc_type}': {len(filtered)}")
    return filtered


def create_dataframe(documents: list) -> pd.DataFrame:
    """
    Create a pandas DataFrame with only the required fields.
    
    Args:
        documents: List of document dictionaries
        
    Returns:
        pd.DataFrame: DataFrame with id, name, and document_url columns
    """
    data = []
    for doc in documents:
        data.append({
            "id": doc.get("id"),
            "name": doc.get("name"),
            "document_url": doc.get("document_url") or doc.get("document")
        })
    
    df = pd.DataFrame(data)
    print(f"\nDataFrame created with {len(df)} rows")
    print(df.head())
    return df


def save_to_excel(df: pd.DataFrame, filename: Path):
    """
    Save the DataFrame to an Excel file.
    
    Args:
        df: The DataFrame to save
        filename: Output file path
    """
    # Ensure the parent folder exists
    filename.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(filename, index=False)
    print(f"\nData saved to: {filename}")


def download_documents(df: pd.DataFrame, folder: str):
    """
    Download all documents from their URLs into a folder.
    
    Args:
        df: DataFrame containing document_url column
        folder: Folder to save documents to
    """
    # Create the folder if it doesn't exist
    folder_path = Path(folder)
    folder_path.mkdir(parents=True, exist_ok=True)
    
    print(f"\nDownloading documents to: {folder_path.absolute()}")
    
    headers = {
        "Authorization": API_TOKEN
    }
    
    successful = 0
    failed = 0
    
    for idx, row in df.iterrows():
        doc_url = row["document_url"]
        doc_id = row["id"]
        doc_name = row["name"]
        
        if not doc_url:
            print(f"  [{idx + 1}/{len(df)}] Skipping ID {doc_id} - No URL available")
            failed += 1
            continue
        
        try:
            # Extract filename from URL or create one
            parsed_url = urlparse(doc_url)
            original_filename = os.path.basename(parsed_url.path)
            
            # Create a safe filename using ID and original name
            if original_filename:
                safe_filename = f"{doc_id}_{original_filename}"
            else:
                # Use name field if URL doesn't have a filename
                safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in str(doc_name))
                safe_filename = f"{doc_id}_{safe_name}"
            
            # Ensure the filename has a .pdf extension
            if not safe_filename.lower().endswith('.pdf'):
                safe_filename = f"{safe_filename}.pdf"
            
            output_path = folder_path / safe_filename
            
            print(f"  [{idx + 1}/{len(df)}] Downloading: {safe_filename[:60]}...")
            
            # Download the file
            response = requests.get(doc_url, headers=headers, stream=True)
            response.raise_for_status()
            
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            successful += 1
            
            # Small delay to be nice to the server
            time.sleep(0.2)
            
        except Exception as e:
            print(f"  [{idx + 1}/{len(df)}] FAILED to download ID {doc_id}: {e}")
            failed += 1
    
    print(f"\nDownload complete!")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")


def main():
    """Main execution function."""
    print("=" * 60)
    print("IFRC Appeal Document Fetcher")
    print("=" * 60)
    
    # Step 1: Fetch all documents from API
    all_documents = fetch_all_appeal_documents()
    
    # Step 2: Filter by type
    filtered_documents = filter_by_type(all_documents, DOCUMENT_TYPE_FILTER)
    
    if not filtered_documents:
        print("No documents found matching the filter criteria!")
        return
    
    # Step 3: Create DataFrame with required fields
    df = create_dataframe(filtered_documents)
    
    # Step 4: Save to Excel
    save_to_excel(df, OUTPUT_EXCEL)
    
    # Step 5: Download all documents
    download_documents(df, DOCUMENTS_FOLDER)
    
    print("\n" + "=" * 60)
    print("Process completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
