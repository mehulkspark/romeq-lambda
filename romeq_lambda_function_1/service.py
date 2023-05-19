import re
import fitz
import requests
import json
import boto3

lambda_client = boto3.client('lambda',region_name='ap-south-1')
lambda_function_name = "romeq_textractor_ocr"

def lambda_handler(page, url):
    event = {"page": page,
             "document_uri": url}
    print("Invoking Lambda Function", lambda_function_name)
    invoke_response = lambda_client.invoke(FunctionName=lambda_function_name,
                                           InvocationType='RequestResponse',
                                           Payload = json.dumps(event))
    print("Invocation Response from Lambda", invoke_response)
    response = invoke_response["Payload"].read()
    return response

def handle(event, context):
    pattern = re.compile("scanned by camscanner", re.IGNORECASE)
    page_number = event.get('page')
    document_uri = event.get('url')
    print("Page Number", page_number)
    print("URL", document_uri)
    response = requests.get(document_uri, verify=False)
    print("Response", response)
    try:
        doc = fitz.open(stream=response.content, filetype='pdf')
    except:
        return {'status': 'error', "page": page_number, 'message': 'Content error'}
    page = doc[page_number]
    page_text = page.getText('text').encode("ascii", errors="ignore").decode().strip()
    page_text = pattern.sub("", page_text).strip()
    if not page_text:
        response = lambda_handler(page_number, document_uri)
        text_result = json.loads(response)
        return text_result
    else:
        text = page_text
    return {'status': 'success', "page": page_number, 'message': text}
