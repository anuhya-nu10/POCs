import json
import boto3
import fitz  # PyMuPDF
from io import BytesIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # bucket = event["bucket"]
    # key = event["key"]
    bucket = "super-rm-lambda" # event.get("bucket", "your-bucket-name")    
    key = "infosys-ar-24.pdf"# event.get("key", "your-file.pdf")  

    # Download PDF from S3 into memory
    pdf_object = s3.get_object(Bucket=bucket, Key=key)
    pdf_stream = BytesIO(pdf_object["Body"].read())

    # Open PDF using fitz
    doc = fitz.open(stream=pdf_stream, filetype="pdf")
    output = []

    for i, page in enumerate(doc):
        text = page.get_text("text")  # Include footer/header text
        output.append({
            "pdf_page": i + 1,
            "text": text.strip()  + " \n <end of page>"
        })

    # Optional: save to another S3 bucket/key if needed
    # s3.put_object(Bucket=bucket, Key="extracted_pages.json", Body=json.dumps(output))
    print(output)
    return {
        "statusCode": 200,
        "body": json.dumps(output)
    }
