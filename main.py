# python FastAPI wrapper for Tigris HTTP API
# follows the quickstart as seen at https://www.tigrisdata.com/docs/quickstarts/quickstart-http/

import os, json, requests
from fastapi import FastAPI
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
TOKEN = '.'
URI = os.getenv('TIGRIS_URI')
PROJECT = os.getenv('TIGRIS_PROJECT')


@app.get("/")
async def root():
    # get token for other operations
    global TOKEN
    url = 'https://' + URI + '/v1/auth/token'

    head = {'content-type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.getenv('TIGRIS_ID'),
        'client_secret': os.getenv('TIGRIS_SECRET')
    }

    resp = requests.post(url, data=data, headers=head)
    rdict = resp.json()
    TOKEN = rdict["access_token"]

    return resp.status_code


@app.get("/create-database/{name}")
async def create_database(name: str):
    # in the docs: name = 'sampledb'
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/branches/' + name + '/create'
    head = {'Authorization': 'Bearer ' + TOKEN}

    resp = requests.post(url, headers=head)
    return resp.status_code


@app.get("/create-update-collection/{name}")
async def create_collection(name: str):
    # in the docs: name = 'users'
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + name + '/createOrUpdate'
    head = {
        'Authorization': 'Bearer ' + TOKEN,
        'content-type': 'application/json'
    }

    data = {
        'schema': {
            'title': name,
            'properties': {
                'balance': {
                    'type': 'number',
                    'searchIndex': True
                },
                'id': {
                    'type': 'integer',
                    'format': 'int32',
                    'autoGenerate': True
                },
                'name': {
                    'type': 'string',
                    'searchIndex': True
                }
            },
            'primary key': ['id']
        }
    }

    datj = json.dumps(data)
    resp = requests.post(url, data=datj, headers=head)
    return resp.json()


@app.get("/list-collection/{name}")
async def list_collection(name: str):
    # in the docs: name = 'users'
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + name + '/describe'
    head = {'Authorization': 'Bearer ' + TOKEN}

    resp = requests.post(url, headers=head)
    return resp.json()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
