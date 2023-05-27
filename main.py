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

    # return resp.status_code
    return 'Your token is good for: ' + str(rdict["expires_in"]) + ' seconds!'


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


@app.get("/insert-documents/{collection}")
async def insert_documents(collection: str):
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + collection + '/documents/insert'
    head = {
        'Authorization': 'Bearer ' + TOKEN,
        'content-type': 'application/json'
    }

    data = {
        'documents': [
            {
                'name': 'Jania McGrory',
                'balance': 6045.7
            },
            {
                'name': 'Bunny Instone',
                'balance': 2948.87
            },
            {
                'name': 'Elon Musk',
                'balance': 245025.65
            },
            {
                'name': 'Elvis Presley',
                'balance': 0
            }
        ]
    }

    datj = json.dumps(data)
    resp = requests.post(url, data=datj, headers=head)
    return resp.json()


@app.get("/read-document/{collection}/{field}/{value}")
async def read_document(collection: str, field: str, value):
    # the name parameter here is for the collection name (users)
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + collection + '/documents/read'
    head = {
        'Authorization': 'Bearer ' + TOKEN,
        'content-type': 'application/json'
    }

    data = {
        'filter': {
            field: value
        }
    }

    datj = json.dumps(data)
    resp = requests.post(url, data=datj, headers=head)
    return resp.json()


@app.get("/update-document/{collection}/{id}/{field}/{value}")
async def update_document(collection: str, id: int, field: str, value):
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + collection + '/documents/update'
    head = {
        'Authorization': 'Bearer ' + TOKEN,
        'content-type': 'application/json'
    }

    if field == 'balance':
        val = float(value)
    else:
        val = str(value)

    data = {
        'fields': {
            '$set': {
                field: val
            }
        },
        'filter': {
            'id': id
        }
    }

    datj = json.dumps(data)
    resp = requests.put(url, data=datj, headers=head)
    return resp.json()


@app.get("/search-documents/{collection}")
async def search_documents(collection: str, query):
    # there will have to be extensive modifications here to allow for the various filters and such that could happen!
    # for now just going with the simple solution in the quickstart
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + collection + '/documents/search'
    head = {
        'Authorization': 'Bearer ' + TOKEN,
        'content-type': 'application/json'
    }

    data = {
        'q': 'bunny',
        'search_fields': ['name'],
        'filter': {
            'balance': {
                '$gt': 500
            }
        }
    }

    datj = json.dumps(data)
    resp = requests.post(url, data=datj, headers=head)
    return resp.json()


@app.get("/delete-document/{collection}/{id}")
async def delete_document(collection: str, id: int):
    # slightly different from the quickstart, only allowing for one delete at a time
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + collection + '/documents/delete'
    head = {
        'Authorization': 'Bearer ' + TOKEN,
        'content-type': 'application/json'
    }

    # I kept the syntax here the same to illustrate the filter, but there is no record id 0
    data = {
        'filter': {
            '$or': [
                {'id': 0},
                {'id': 4}
            ]
        }
    }

    datj = json.dumps(data)
    resp = requests.delete(url, data=datj, headers=head)
    return resp.json()


@app.get("/delete-collection/{collection}")
async def delete_collection(collection: str):
    url = 'https://' + URI + '/v1/projects/' + PROJECT + '/database/collections/' + collection + '/drop'
    head = {'Authorization': 'Bearer ' + TOKEN}

    resp = requests.delete(url, headers=head)
    return resp.json()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
