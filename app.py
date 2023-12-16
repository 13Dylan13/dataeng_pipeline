from auth import auth
from transform import create_call_data
from transform import  create_call_metadata
from transform import create_call_metrics
from transform import create_callrecorder_metadata
from transform import create_audio_metrics
from transform import split_categories
from transform import list_to_dataframe
from transform import clean_response
from transform import create_agents_calls
from build_categorisations import tag_loop
from build_categorisations import category_loop
from load import load
from load import read_s3
import logging
from datetime import datetime

#create variables
key = 'call_details.csv'
category_list = []
tag_list = []

#poss_cats define which 'categoroes' from the software will be sent to the database
poss_cats = ["Engineer","Factory","Free of Charge","Level 2: Apps","Level 2: Apps Services",
             "Level 2: Battery and Power","Level 2: Call Quality","Level 2: Physical damage",
             "Level 3: Embedded Apps","Level 3 - Other Third Party Apps","Level 3: Productivity",
             "Level 3: Samsung Apps","Level 3: SocialMedia","Marketing","Phone Lock",
             "Proof of purchase","Repair","Repair Booking","Repair Chasing","Transfer",
             "Troubleshooting","Screen Issues","Warranty Expired"]
poss_tags = ['Level 4: Apple Music','Level 4: Chrome','Level 4: DisneyPlus','Level 4: Facebook',
             'Level 4: Google Wallet','Level 4: Instagram','Level 4: Messenger','Level 4: Netflix',
             'Level 4: Smart Things','Level 4: SmartTutor','Level 4: Smart Switch','Level 4: SnapChat',
             'Level 4: Spotify','Level 4: TickTock','Level 4: Whatsapp','Call categories']

logging.basicConfig(filename='pipele.log', encoding='utf-8', level=logging.DEBUG)
logging.info(f'{datetime.now()} - Start pipeline...')
logging.info(f'{datetime.now()} - Start authentication...')

#Authenticate database & s3
s3_resource, s3_client, s3_bucket,alchemyEngine=auth('pgadminaccess.ini')
logging.info(f'{datetime.now()} - authentication completed...')

#Extract S3 csv
logging.info(f'{datetime.now()} - getting data from S3...')
response = read_s3(s3_bucket, s3_resource, key)
response = clean_response(response) #fixing issues with the column names for the DB load
logging.info(f'{datetime.now()} - S3 data loaded..')

#transform
logging.info(f'{datetime.now()} - Start creating none categorical tables...')
call_data = create_call_data(response)
agents_calls = create_agents_calls(response)
call_metadata = create_call_metadata(response)
call_metrics = create_call_metrics(response)
callrecorder_metadata = create_callrecorder_metadata(response)
audio_metrics = create_audio_metrics(response)
logging.info(f'{datetime.now()} - Start tranformation, spliting call tags...')
tag_list, call_categories = tag_loop(response,poss_tags,tag_list)
logging.info(f'{datetime.now()} - Start tranformation, spliting categories...')
calls_categorised = split_categories(call_categories,poss_cats)
logging.info(f'{datetime.now()} - Start tranformation, create category list...')
category_list = category_loop(poss_cats,calls_categorised,category_list)
logging.info(f'{datetime.now()} - transformation completed...')
logging.info(f'{datetime.now()} - change list to dataframe...') #Database ready
calls_and_categories = list_to_dataframe(category_list, 'Category')
calls_and_tags = list_to_dataframe(tag_list, 'Tag')

#load into database
logging.info(f'{datetime.now()} - Start loading...')
load(alchemyEngine, call_data,'call_data')
load(alchemyEngine, agents_calls,'agent_call')
load(alchemyEngine, calls_and_categories, 'calls_and_categories')
load(alchemyEngine, calls_and_tags, 'calls_and_tags')
load(alchemyEngine, call_metadata, 'call_metadata')
load(alchemyEngine, call_metrics, 'call_metrics')
load(alchemyEngine, callrecorder_metadata, 'callrecorder_metadata')
load(alchemyEngine, audio_metrics, 'audio_metrics')
logging.info(f'{datetime.now()} - loading completed...')

