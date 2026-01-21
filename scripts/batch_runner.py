"""
Batch Processor for extracted EAP triggers using Google Gemini Batch API.

This script implements the "Official Batch API" workflow:
1. Process 5 PDFs locally to generate payloads
2. Create a JSONL file with these payloads
3. Submit a Batch Job to Google (50% cost discount)
4. Poll for completion
5. Retrieve results and update the main JSON output
"""

import json
import time
import re
import os
from pathlib import Path
from typing import List, Dict, Any
import requests
import sys
from google import genai
from google.genai import types

# Add project root to path so we can import from scripts.config
# This works whether run as module or script
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scripts.config import (
    GEMINI_API_KEY,
    GEMINI_MODEL,
    DOCUMENTS_FOLDER,
    OUTPUT_FOLDER,
    LLM_TEMPERATURE,
    LLM_MAX_TOKENS
)
from scripts.llm_extractor import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from scripts.main import (
    prepare_pdf_payload, 
    load_document_metadata, 
    extract_document_id
)

# Configuration
BATCH_SIZE = 5
POLL_INTERVAL_SECONDS = 30
BATCH_STATE_FILE = OUTPUT_FOLDER / "batch_state.json"

class BatchProcessor:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not found!")
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.metadata = load_document_metadata()
        OUTPUT_FOLDER.mkdir(exist_ok=True)
        
    def get_processed_files(self) -> set:
        """Load list of already processed files from existing output JSON."""
        output_file = OUTPUT_FOLDER / "extracted_triggers.json"
        if not output_file.exists():
            return set()
            
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Handle list or dict format (currently it's a list based on main.py)
                if isinstance(data, list):
                    return {item.get('file') for item in data if item.get('file')}
                return set()
        except Exception as e:
            print(f"Error reading existing results: {e}")
            return set()

    def create_jsonl_payload(self, pdf_files: List[Path]) -> tuple[Path, List[dict], List[str]]:
        """
        Create a JSONL file for the batch request.
        Returns: 
            - Path to generated JSONL file
            - List of result metadata dictionaries (to be filled later)
            - List of request IDs
        """
        jsonl_path = OUTPUT_FOLDER / f"batch_payload_{int(time.time())}.jsonl"
        result_metadatas = []
        request_ids = []
        
        print(f"  Preparing payloads for {len(pdf_files)} files...")
        
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for pdf_path in pdf_files:
                # Use the imported function from main.py
                payload_text, result_meta = prepare_pdf_payload(pdf_path, self.metadata)
                
                if not payload_text:
                    print(f"    Skipping {pdf_path.name} (failed to create payload)")
                    continue
                
                # Construct the JSONL entry for Batch API
                # ID must be unique per request in the batch
                request_id = f"req_{extract_document_id(pdf_path.name)}_{int(time.time())}"
                
                # Create the request object structure
                # Note: The structure depends on whether we use the SDK helper or raw JSON
                # Using standard generation request structure
                user_prompt = USER_PROMPT_TEMPLATE.format(content=payload_text)
                
                request_body = {
                    "request": {
                        "contents": [
                            {"role": "user", "parts": [{"text": user_prompt}]}
                        ],
                        "generationConfig": {
                            "temperature": LLM_TEMPERATURE,
                            "maxOutputTokens": LLM_MAX_TOKENS,
                            "responseMimeType": "application/json",
                        },
                        "systemInstruction": {
                             "parts": [{"text": SYSTEM_PROMPT}]
                        }
                    },
                    "custom_id": request_id
                }
                
                f.write(json.dumps(request_body) + "\n")
                
                # Store metadata to link back results later
                result_meta["custom_id"] = request_id
                result_metadatas.append(result_meta)
                request_ids.append(request_id)
                
        return jsonl_path, result_metadatas, request_ids

    def submit_batch_job(self, jsonl_path: Path) -> str:
        """Upload file and submit batch job."""
        print(f"  Uploading {jsonl_path.name}...")
        
        # 1. Upload file
        batch_file = self.client.files.upload(
            file=jsonl_path,
            config={'mime_type': 'application/json'}
        )
        print(f"  File uploaded: {batch_file.name}")
        
        # 2. Create batch job
        print(f"  Submitting batch job (Model: {GEMINI_MODEL})...")
        batch_job = self.client.batches.create(
            model=GEMINI_MODEL,
            src=batch_file.name,
        )
        
        print(f"  Batch job started! ID: {batch_job.name}")
        return batch_job.name

    def wait_for_job(self, job_name: str):
        """Poll job status until complete."""
        print(f"  Waiting for job completion (polling every {POLL_INTERVAL_SECONDS}s)...")
        while True:
            job = self.client.batches.get(name=job_name)
            print(f"    Status: {job.state}")
            
            if job.state == "ACTIVE":
                pass # Still running
            elif job.state == "SUCCEEDED":
                print("  Job completed successfully!")
                return job
            elif job.state == "FAILED":
                raise RuntimeError(f"Batch job failed: {job.error}")
            elif job.state == "CANCELLED":
                raise RuntimeError("Batch job was cancelled")
                
            time.sleep(POLL_INTERVAL_SECONDS)

    def retrieve_and_save_results(self, job, result_metadatas: List[dict]):
        """Download results and merge with metadata."""
        print("  Retrieving results...")
        
        # Get output file URI
        output_file_name = job.output_file.name
        
        # Download the content (it's also a JSONL)
        # The SDK might have a helper, but typically we request the file content
        response = requests.get(f"https://generativelanguage.googleapis.com/v1beta/{output_file_name}?key={GEMINI_API_KEY}")
        
        if response.status_code != 200:
             # Try via client file API if direct download fails
             # (SDK specific implementation)
             content = self.client.files.get(name=output_file_name).download()
             # Decode if bytes
             if hasattr(content, 'decode'):
                 output_content = content.decode('utf-8')
             else:
                 output_content = str(content) # Fallback
        else:
            output_content = response.text
            
        # Parse output JSONL
        # Map custom_id -> result
        results_map = {}
        for line in output_content.strip().split('\n'):
            if not line.strip(): continue
            try:
                item = json.loads(line)
                c_id = item.get("custom_id")
                
                # Extract the generated text
                # Structure: response -> candidates -> content -> parts -> text
                response_data = item.get("response", {})
                
                # Check for errors in individual items
                if "error" in response_data:
                    print(f"    Item error for {c_id}: {response_data['error']}")
                    results_map[c_id] = {"error": str(response_data['error'])}
                    continue
                    
                candidates = response_data.get("candidates", [])
                if candidates:
                    parts = candidates[0].get("content", {}).get("parts", [])
                    if parts:
                        text = parts[0].get("text", "")
                        # Parse the JSON from the LLM text
                        try:
                            # Clean markdown
                            clean_text = text.replace("```json", "").replace("```", "").strip()
                            llm_json = json.loads(clean_text)
                            results_map[c_id] = llm_json
                        except json.JSONDecodeError:
                            results_map[c_id] = {"error": "JSON Parse Error on output"}
            except Exception as e:
                print(f"    Error parsing result line: {e}")

        # Merge with metadata and save to main JSON
        self.update_main_json(results_map, result_metadatas)

    def update_main_json(self, results_map: dict, result_metadatas: List[dict]):
        """Update existing JSON file with new results."""
        output_file = OUTPUT_FOLDER / "extracted_triggers.json"
        
        # Load existing
        current_data = []
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                try:
                    current_data = json.load(f)
                except: pass
        
        # Process new results
        new_entries = []
        for meta in result_metadatas:
            c_id = meta.get("custom_id")
            llm_result = results_map.get(c_id)
            
            if not llm_result:
                meta["status"] = "error"
                meta["error"] = "No response from Batch API"
            elif "error" in llm_result:
                meta["status"] = "error"
                meta["error"] = llm_result["error"]
            else:
                # Success - Fill in the data
                meta["document_language"] = llm_result.get("document_language")
                meta["trigger_mechanism"] = llm_result.get("trigger_mechanism")
                meta["notes"] = llm_result.get("notes", [])
                
                # Format triggers
                raw_triggers = llm_result.get("triggers", [])
                formatted_triggers = {}
                for i, trigger in enumerate(raw_triggers, 1):
                    trigger_key = f"trigger_statement_{i}"
                    formatted_triggers[trigger_key] = {
                        "statement": trigger.get("trigger_statement", ""),
                        "statement_english": trigger.get("statement_english"),
                        "thresholds": trigger.get("thresholds", []),
                        "source_authority": trigger.get("source_authority"),
                        "lead_time": trigger.get("lead_time"),
                        "geographic_scope": trigger.get("geographic_scope"),
                        "is_conditional": trigger.get("is_conditional"),
                        "condition_dependency": trigger.get("condition_dependency"),
                        "preliminary_actions": trigger.get("preliminary_actions"),
                        "page_ref": trigger.get("page_ref")
                    }
                meta["triggers"] = formatted_triggers
            
            # Remove temporary custom_id
            if "custom_id" in meta:
                del meta["custom_id"]
                
            new_entries.append(meta)
            
        # combine and save
        current_data.extend(new_entries)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(current_data, f, indent=2, ensure_ascii=False)
            
        print(f"  Saved {len(new_entries)} new results to {output_file.name}")


def run_batch_process():
    """Main entry point for batch processing."""
    print("Starting Batch Processor...")
    processor = BatchProcessor()
    
    # Get all PDF files
    all_pdfs = [f for f in DOCUMENTS_FOLDER.iterdir() 
                if f.is_file() and f.suffix.lower() == '.pdf']
    
    print(f"Found {len(all_pdfs)} PDFs total.")
    
    # Filter processed
    processed_files = processor.get_processed_files()
    files_to_process = [f for f in all_pdfs if f.name not in processed_files]
    
    print(f"Files already processed: {len(processed_files)}")
    print(f"Files remaining: {len(files_to_process)}")
    
    if not files_to_process:
        print("All files processed! Exiting.")
        return

    # Process all remaining files in batches
    while files_to_process:
        batch_files = files_to_process[:BATCH_SIZE]
        print(f"\n{'='*40}")
        print(f"Starting Batch of {len(batch_files)} files")
        print(f"Files: {[f.name for f in batch_files]}")
        print(f"{'='*40}")
        
        # 1. Create Payload
        jsonl_path, result_metas, req_ids = processor.create_jsonl_payload(batch_files)
        
        if not req_ids:
            print("No valid payloads created in this batch. Skipping.")
            # Remove failed files from the list so we don't loop forever
            # (In reality, remove processed files is handled by refreshing list or slicing)
            # Simpler: just break if something gets stuck, but here we assume result_metas matches
            pass
        else:
            # 2. Submit Job
            try:
                job_name = processor.submit_batch_job(jsonl_path)
                
                # 3. Wait
                job = processor.wait_for_job(job_name)
                
                # 4. Retrieve
                processor.retrieve_and_save_results(job, result_metas)
                
            except Exception as e:
                print(f"\nCRITICAL ERROR during batch processing: {e}")
                # Optional: break or continue? strict error handling is better
                print("Stopping execution to prevent cascading failures.")
                break
            
            # Cleanup jsonl
            if jsonl_path.exists():
                os.remove(jsonl_path)
                print("Cleaned up temporary JSONL file.")
        
        # Update remaining files list - effectively removing the ones we just did
        # Re-fetch processed set to be sure (safest way)
        processed_files = processor.get_processed_files()
        all_to_process_now = [f for f in all_pdfs if f.name not in processed_files]
        
        # If no progress made, we must break to avoid infinite loop
        if len(all_to_process_now) == len(files_to_process):
             print("Warning: Number of remaining files did not decrease. Stopping to avoid infinite loop.")
             break
             
        files_to_process = all_to_process_now
        print(f"\nBatch complete. Files remaining: {len(files_to_process)}")
        
        # Small pause between batches
        time.sleep(5)

if __name__ == "__main__":
    run_batch_process()
