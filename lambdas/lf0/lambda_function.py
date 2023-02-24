import json
from datetime import datetime
from uuid import uuid4
import boto3

def create_error(code: int, message: str) -> dict:
    return dict(
        code=code,
        message=message
    )

def create_unstructured_message(id, text, timestamp):
    return dict(
        id=id,
        text=text,
        timestamp=timestamp
    )

def create_message(u_message, type: str = "unstructured"):
    message = dict()
    message["type"] = type
    message["unstructured"] = u_message
    return message   

def create_bot_response(messages: list):
    return dict(
        messages=messages
    )        
        
def create_simple_message(msg_text: str):
    dt = datetime.now()
    ts = datetime.timestamp(dt)
    unstructured = create_unstructured_message(
        str(uuid4()),
        msg_text,
        ts)
    
    message = create_message(unstructured)
    bot_response = create_bot_response([message])
    return bot_response
    
def parse_response(response: dict):
    metadata = response["ResponseMetadata"]
    http_status = metadata["HTTPStatusCode"]
    # if http_status == 200:
    #     messages = response['messages']
    # else:
    messages = response['messages']
    return messages[0]['content']
        
    
def post_to_bot(event):
    client = boto3.client('lexv2-runtime')
    
    msg: str = event['messages'][0]['unstructured']['text']
    print(f"Parsed message: {msg}")
    
    response = client.recognize_text(
        botId='GV9YXEA5BJ',
        botAliasId='VEY7QVDD0A',
        localeId='en_US',
        sessionId='testsession',
        text=msg,
    )
    
    return parse_response(response)
    
    print(f"received response from lex: {response}")

def lambda_handler(event, context):
    print(f"event: {event}")
    print(f"context: {context}")
    rsp_msg: str = post_to_bot(event)
    # resp: dict = create_simple_message("Hello World!")
    resp: dict = create_simple_message(rsp_msg)
    
    return resp
