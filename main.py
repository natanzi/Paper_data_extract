import os
import re
import json
import csv
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Regular expression patterns and validation placeholders
doi_pattern = r'\b10\.\d{4,9}/[-.\_;()/:A-Za-z0-9]+\b'
issn_pattern = r'\b\d{4}-\d{3}[0-9Xx]\b'

def is_valid_doi(doi):
    return True  # Placeholder

def is_valid_issn(issn):
    return True  # Placeholder

def extract_and_write_pdf(pdf_path, csv_writer):
    try:
        pdf = PdfReader(pdf_path)
        metadata = pdf.metadata
        title = metadata.get('/Title', "Unknown")
        author = metadata.get('/Author', "Unknown")
        
        text = "".join(page.extract_text() for page in pdf.pages)
        
        # Extract and validate
        dois = [doi for doi in re.findall(doi_pattern, text) if is_valid_doi(doi)]
        issns = [issn for issn in re.findall(issn_pattern, text) if is_valid_issn(issn)]
        
        # Write directly to CSV
        csv_writer.writerow([title, author, "; ".join(dois), "; ".join(issns)])
    except Exception as e:
        logger.exception("Error processing %s: %s", pdf_path, e)

def process_folder(folder, csv_file):
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Author', 'DOIs', 'ISSNs'])
        
        pdf_files = [pdf_file for pdf_file in folder.iterdir() if pdf_file.suffix == '.pdf']
        with ThreadPoolExecutor() as executor:
            futures = []
            for pdf_file in pdf_files:
                # Schedule the execution
                future = executor.submit(extract_and_write_pdf, pdf_file, writer)
                futures.append(future)
                
            # Progress tracking
            for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing PDFs"):
                pass  # Each future writes directly to CSV upon completion

pdf_dir = r'C:\.....'
csv_file = r'C:\....Papers\output.csv'

folder = Path(pdf_dir)
process_folder(folder, csv_file)
