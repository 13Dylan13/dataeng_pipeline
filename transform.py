import pandas as pd


def create_call_data(response):
    calls_data = response[['Call ID','Call date','Length','Direction','Called number','Customer ID']].copy()
    return calls_data

def create_agents_calls(response):
    agents_calls = response[['Call ID','Customer ID','Agent name','Agent group']]
    return agents_calls


def create_call_metadata(response):
    emotion_metadata = response[['Call ID','Displeased','Happy','Uncertain','Disappointed','Disappointed customer',
                                   'Displeased customer','Happy customer','Uncertain customer','Displeased agent','Happy agent',
                                    'Uncertain agent','Disappointed agent']]
    return emotion_metadata
    
    
def create_call_metrics(response):
    emotion_metrics = response[['Call ID','Displeased EI/min','Happy EI/min','Uncertain EI/min','Disappointed EI/min',
                                 'Displeased EI/min customer','Happy EI/min customer','Uncertain EI/min customer',
                                 'Disappointed EI/min customer','Displeased EI/min agent','Happy EI/min agent',
                                 'Uncertain EI/min agent','Disappointed EI/min agent']]
    return emotion_metrics

def create_callrecorder_metadata(response):
    recorder_call_metadata = response[['Call ID','Music','Speech','Crosstalk','Silence',
                               'Speech rate','Speech volume ratio','Articulation quality','Opeator dominance percent',
                               'Customer dominance percent']]
    remove_percent_symbol = ['Opeator dominance percent','Customer dominance percent']
    recorder_call_metadata[remove_percent_symbol] = recorder_call_metadata[remove_percent_symbol].replace({'%':''}, regex=True)
    return recorder_call_metadata

def create_audio_metrics(response):
    audio_metrics = response[['Call ID','Pitch','Fragmentation','Global score','Overall line quality','Dynamic range',
                      'Crest factor','Flat factor','Effective bit depth','RMS level','Signal-to-noise']]
    return audio_metrics

   

def split_categories(call_categories,poss_cats):
    import numpy as np
    length = len(poss_cats)
    n=0
    call_categories = call_categories[['Call ID','Call categories']]
    while n < length:
        call_categories[poss_cats[n]] = call_categories["Call categories"].str.find(poss_cats[n])
        n=n+1
    call_categories[call_categories == -1] = np.nan #replace not found -1 with nan

    return call_categories


def list_to_dataframe(listvariable, type):
     columns = ['Call ID',type]
     results = pd.DataFrame.from_records(listvariable,columns=columns) #list to dataframe, ready for export
     results['Call ID']=results['Call ID'].astype('int') #ensuring that the Call ID isn't exported as a float
     return results
     
def clean_response(response):
    response.columns = response.columns.str.replace("[()]", "", regex=True) #brackets causing problems in the DB load
    response.columns = response.columns.str.replace("[%]", "percent", regex=True) #brackets causing problems in the DB load
    return response
