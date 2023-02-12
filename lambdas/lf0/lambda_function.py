import json
import uuid
import json
from datetime import datetime

class Error():
    def __init__(self, code, message):
        self.code = code
        self.message = message

class UnstructuredMessage():
    def __init__(self, id, text, timestamp):
        self.id = id
        self.text = text
        self.timestamp = timestamp
        
class Message():
    def __init__(self, unstructured, type: str = "unstructured"):
        self.type = type
        self.unstructured = unstructured
        
class BotResponse():
    def __init__(self, messages):
        self.messages = messages
    
    def toJson(self):
        pass
        
def create_simple_message(msg_text: str):
    dt = datetime.now()
    ts = datetime.timestamp(dt)
    unstructured = UnstructuredMessage(
        str(uuid.uuid4()),
        msg_text,
        ts)
    
    message = Message(unstructured)
    return BotResponse([ message ])

def lambda_handler(event, context):
    resp = create_simple_message("Hello World!")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        'event': event,
        'data': json.dumps(resp.__dict__)
    }
