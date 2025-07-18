import json
import boto3
import fitz  # PyMuPDF
import re
from io import BytesIO

s3 = boto3.client("s3")

def extract_number_blocks(page):
    blocks = page.get_text("dict")["blocks"]
    footer_blocks = []

    page_height = page.rect.height

    # Only consider blocks in the bottom 20% of the page
    for block in blocks:
        if "lines" not in block:
            continue
        y0 = block["bbox"][1]
        if y0 > page_height * 0.8:
            footer_blocks.append(block)

    digits = []
    for block in footer_blocks:
        for line in block["lines"]:
            for span in line["spans"]:
                text = span["text"].strip()
                # Collect individual digits or short numbers
                if re.fullmatch(r"\d{1,4}", text):
                    digits.append((span["bbox"][1], text))  # use vertical position to sort

    # Sort top to bottom (y), then join digits
    digits.sort()
    combined = "".join(d[1] for d in digits)
    return combined if combined.isdigit() else None

def extract_valid_number(text, last_number):
    if not text or not text.isdigit():
        return None

    number = int(text)
    if number in {2020, 2021, 2022, 2023, 2024, 2025,2026}:
        return None
    if number <= 0:
        return None
    if number > last_number + 2:
        return None
    return number

def lambda_handler(event, context):
    # bucket = event["bucket"]
    # key = event["key"]
    bucket = "super-rm-lambda" # event.get("bucket", "your-bucket-name")    
    key = "infosys-ar-24.pdf"# event.get("key", "your-file.pdf")

    obj = s3.get_object(Bucket=bucket, Key=key)
    pdf_bytes = obj["Body"].read()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    results = []
    last_number = 0

    for i, page in enumerate(doc):
        raw_text = extract_number_blocks(page)
        detected = extract_valid_number(raw_text, last_number)

        if detected is None:
            last_number += 1
            detected = last_number
        else:
            last_number = detected

        results.append({
            "pdf_page": i + 1,
            "extracted_number": detected
        })

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }
