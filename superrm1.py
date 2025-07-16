import json
import boto3
import os
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from collections import defaultdict

s3 = boto3.client("s3")

def lambda_handler(event, context):
    print("event:",event)
    s3_key = "infosys-ar-24.pdf" #event["s3_key"]
    # index = event["index"]
    index = event.get("response", {}).get("output", [])
    print("Index: ", index)
    
    # Now use `index` as usual
    # for section in index:
    #     title = section.get("title")
    #     page = section.get("page")
    #     category = section.get("category")
    # bucket = os.environ["BUCKET_NAME"]
    bucket="super-rm-lambda"

    # Step 1: Download original PDF
    pdf_obj = s3.get_object(Bucket=bucket, Key=s3_key)
    pdf_bytes = pdf_obj["Body"].read()
    reader = PdfReader(BytesIO(pdf_bytes))
    total_pages = len(reader.pages)
    print("total_pages: ", total_pages)

    # Step 2: Clean index and generate page ranges
    filtered = [i for i in index if isinstance(i.get("page"), int)]
    filtered.sort(key=lambda x: x["page"])
    for i in range(len(filtered) - 1):
        filtered[i]["end"] = filtered[i + 1]["page"]
    filtered[-1]["end"] = total_pages + 1

    # Step 3: Group by category (excluding "Other")
    category_pages = defaultdict(list)
    for section in filtered:
        category = section.get("category", "Other")
        print("category: ",category)
        if category == "Other":
            continue
        start, end = section["page"] - 1, section["end"] - 1
        category_pages[category].extend(range(start, end))

    # Step 4: Create and upload PDFs per category
    uploaded_keys = []
    for category, pages in category_pages.items():
        writer = PdfWriter()
        for page_num in sorted(set(pages)):
            writer.add_page(reader.pages[page_num])
        
        pdf_out = BytesIO()
        writer.write(pdf_out)
        pdf_out.seek(0)
        
        key = f"chunks/{category.lower().replace(' ', '_')}.pdf"
        s3.upload_fileobj(pdf_out, bucket, key)
        uploaded_keys.append({
            "category": category,
            "s3_key": key
        })

    return {
        "status": "ok",
        "outputs": uploaded_keys
    }
