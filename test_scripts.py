import pandas as pd
from auth import auth
from transform import clean_response
from load import read_s3

key = 'call_details.csv'
s3_resource, s3_client, s3_bucket,alchemyEngine=auth('pgadminaccess.ini')
response = read_s3(s3_bucket, s3_resource, key) #Load the file from S3 into a dataframe
response = clean_response(response) #the written functions are dependant on this


def test_create_call_data_function():
    #Arrange
    from transform import create_call_data
    #Act
    call_data = create_call_data(response) #send the s3 csv data to the function
    expected_headers = ['Call ID','Call date','Length','Direction','Called number','Customer ID']
    headers = list(call_data.columns.values) #list the column headings from the dataframe
    #Assert
    assert len(response) == len(call_data)
    assert expected_headers == headers


def test_create_agents_calls():
    #Arrange
    from transform import create_agents_calls
    expected_headers = ['Call ID','Customer ID','Agent name','Agent group']
    #Act
    agents_calls = create_agents_calls(response) #send the s3 csv data to the function
    headers = list(agents_calls.columns.values) #list the column headings from the dataframe
    #Assert
    assert len(response) == len(agents_calls)
    assert expected_headers == headers

def test_create_call_metadata():
    #Arrange
    from transform import create_call_metadata
    expected_headers = ['Call ID','Displeased','Happy','Uncertain','Disappointed','Disappointed customer',
                        'Displeased customer','Happy customer','Uncertain customer','Displeased agent',
                        'Happy agent','Uncertain agent','Disappointed agent']
    #Act
    call_metadata = create_call_metadata(response)
    headers = list(call_metadata.columns.values) #list the column headings from the dataframe
    #Assert
    assert len(response) == len(call_metadata)
    assert expected_headers == headers
    
    
def test_create_call_metrics():
    #Arrange
    from transform import create_call_metrics
    expected_headers = ['Call ID','Displeased EI/min','Happy EI/min','Uncertain EI/min','Disappointed EI/min',
                                 'Displeased EI/min customer','Happy EI/min customer','Uncertain EI/min customer',
                                 'Disappointed EI/min customer','Displeased EI/min agent','Happy EI/min agent',
                                 'Uncertain EI/min agent','Disappointed EI/min agent']
    #Act
    call_metrics = create_call_metrics(response)
    headers = list(call_metrics.columns.values) #list the column headings from the dataframe
    #Assert
    assert len(response) == len(call_metrics)
    assert expected_headers == headers

def test_create_callrecorder_metadata():
    #Arrange
    from transform import create_callrecorder_metadata
    expected_headers = ['Call ID','Music','Speech','Crosstalk','Silence',
                               'Speech rate','Speech volume ratio','Articulation quality','Opeator dominance percent',
                               'Customer dominance percent']
    #Act
    callrecorder_metadata = create_callrecorder_metadata(response)
    headers = list(callrecorder_metadata.columns.values) #list the column headings from the dataframe
    #Assert
    print('expected;')
    print(expected_headers)
    print('actual')
    print (headers)
    assert len(response) == len(callrecorder_metadata)
    assert expected_headers == headers

def test_create_audio_metrics():
    #Arrange
    from transform import create_audio_metrics
    expected_headers = ['Call ID','Pitch','Fragmentation','Global score','Overall line quality','Dynamic range',
                      'Crest factor','Flat factor','Effective bit depth','RMS level','Signal-to-noise']
    #Act
    audio_metrics = create_audio_metrics(response)
    headers = list(audio_metrics.columns.values) #list the column headings from the dataframe
    #Assert
    assert len(response) == len(audio_metrics)
    assert expected_headers == headers


def test_get_data_from_s3():
    #Arrange
    from load import read_s3
    key = 'call_details.csv'
    s3_resource, s3_client, s3_bucket,alchemyEngine=auth('pgadminaccess.ini')
    #Act
    response = read_s3(s3_bucket, s3_resource, key) #Load the file from S3 into a dataframe
    headers = list(response.columns.values) #list the column headings from the dataframe
    #Assert
    assert len(headers) ==82 #this confirms that the correct number of headers have been loaded
    assert headers[0] == "Call ID" #first column check
    assert headers[81] == "Level 4: Whatsapp" #last column check