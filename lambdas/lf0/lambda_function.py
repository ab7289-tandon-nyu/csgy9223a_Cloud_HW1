import json
from datetime import datetime
from uuid import uuid4

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

def lambda_handler(event, context):
    resp: dict = create_simple_message("Hello World!")
    
    return resp
