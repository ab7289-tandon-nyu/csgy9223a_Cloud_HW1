"""
This sample demonstrates an implementation of the Lex Code Hook Interface
in order to serve a sample bot which manages reservations for hotel rooms and car rentals.
Bot, Intent, and Slot models which are compatible with this sample can be found in the Lex Console
as part of the 'BookTrip' template.

For instructions on how to set up and test this bot, as well as additional samples,
visit the Lex Getting Started documentation http://docs.aws.amazon.com/lex/latest/dg/getting-started.html.
"""

import json
import datetime
import time
import os
import dateutil.parser
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# --- Helpers that build all of the responses ---


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# --- Helper Functions ---


def safe_int(n):
    """
    Safely convert n value to int.
    """
    if n is not None:
        return int(n)
    return n


def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def isvalid_city(city):
    valid_cities = ['new york', 'los angeles', 'chicago', 'houston', 'philadelphia', 'phoenix', 'san antonio',
                    'san diego', 'dallas', 'san jose', 'austin', 'jacksonville', 'san francisco', 'indianapolis',
                    'columbus', 'fort worth', 'charlotte', 'detroit', 'el paso', 'seattle', 'denver', 'washington dc',
                    'memphis', 'boston', 'nashville', 'baltimore', 'portland']
    return city.lower() in valid_cities
    
    
def isvalid_cuisine(cuisine):
    valid_cuisines = [ 'vegetarian', 'seafood', 'indian', 'chinese', 'american', 'italian', 'japanese',
        'mexican', 'mediterranean', 'vegan', 'chicken', 'steak', 'noodles', 'fast food', 'deli', 
        'convenience', 'sandwiches', 'desserts', 'burgers', 'salad', 'coffee', 'thai', 'brazilian', ]
    return cuisine.lower() in valid_cuisines


def validate_dining(slots: dict) -> dict:
    location = try_ex(lambda: slots['Location'])
    cuisine = try_ex(lambda: slots['Cuisine'])
    date = try_ex(lambda: slots['date'])
    time = try_ex(lambda: slots['time'])
    count = try_ex(lambda: slots['count'])
    phone = try_ex(lambda: slots['phone'])
    
    if location and not isvalid_city(location):
        return build_validation_result(
            False,
            "Location",
            "We currently do not support {} as a valid Location. Can you try a different city?".format(location)
        )
        
    if cuisine and not isvalid_cuisine(cuisine):
        return build_validation_result(
            False,
            "Cuisine",
            "We currently do not support {} as a valid Cuisine. Can you try a different one?".format(location)
        )
    
    if date:
        if not isvalid_date(date):
            return build_validation_result(False, 'date', 'I did not understand your reservation date.  When would you like to make your reservation?')
        if datetime.datetime.strptime(date, '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False, 'date', 'Reservations must be scheduled at least one day in advance.  Can you try a different date?')
            
    
    if count is not None and (count < 1 or count > 8):
        return build_validation_result(
            False,
            'count',
            'You can make a reservations for from one to 8 guests.  How many guests will be attending?'
        )
        
    
    return { 'isValid': True }


""" --- Functions that control the bot's behavior --- """

def handle_greet(intent_request):
    """
    Handles the initial greeting
    """
    logger.debug("Recieved GreetingIntent\nintent_request: {}".format(json.dumps(intent_request)))
    
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Hi there, how can I help you?'
        }
    )
    
def handle_dining_intent(intent_request: dict) -> dict:
    location = try_ex(lambda: intent_request['currentIntent']['slots']['Location'])
    cuisine = try_ex(lambda: intent_request['currentIntent']['slots']['Cuisine'])
    date = try_ex(lambda: intent_request['currentIntent']['slots']['date'])
    time = try_ex(lambda: intent_request['currentIntent']['slots']['time'])
    count = try_ex(lambda: intent_request['currentIntent']['slots']['count'])
    phone = try_ex(lambda: intent_request['currentIntent']['slots']['phone'])
    
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
    if intent_request['invocationSource'] == 'DialogCodeHook':
        # validate any slots which have been specified. If any are invalid re-elicit for their value
        validation_result = validate_dining(intent_request['currentIntent']['slots'])
        if not validation_result['isValid']:
            
            slots = intent_request['currentIntent']['slots']
            slots[validation_result['violatedSlot']] = None
            
            return elicit_slot(
                    session_attributes,
                    intent_request['currentIntent']['name'],
                    slots,
                    validation_result['violatedSlot'],
                    validation_result['message'],
                )
        
        # continue eliciting slots if need be
        return delegate(session_attributes, intent_request['currentIntent']['slots'])
    
    # TODO push info to SQS
    
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': ("Thanks, you're all set! You should receive my suggestions "
            "via SMS in a few minutes!")
        }
    )
    

def handle_thank_you(intent_request: dict) -> dict:
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        "Fulfilled",
        {
            "contentType": "PlainText",
            "content": "Thanks for chatting with me!"
        }
    )


# --- Intents ---


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'GreetingIntent':
        return handle_greet(intent_request)
    elif intent_name == 'DiningingSuggestionIntent':
        return handle_dining_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        raise handle_thank_you(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
