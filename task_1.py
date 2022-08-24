from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.test
collection = db.test
result_collection = db.test_result

pipe = [{
            '$unwind': {
                'path': "$sessions"
            }
        }, {
            '$unwind': {
                'path': '$sessions.actions',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$group': {
                '_id': {'number': "$number", 'type': '$sessions.actions.type'},
                'count': {"$sum": 1},
                'last': {'$max': "$sessions.actions.created_at"}
            }
        }, {
            '$group': {
                '_id': "$_id.number",
                'number': {'$first': '$_id.number'},
                'actions': {
                    '$push': {
                        'type': '$_id.type',
                        'last': '$last',
                        'count': '$count',
                    }
                }
            }
        }, {
            '$project': {
                '_id': 0
            }
        }]

result = list(collection.aggregate(pipe))

for user in result:
    temp = []
    for i in user['actions']:
        temp.append(i['type'])
    if 'create' not in temp:
        user['actions'].append({'type': 'create', 'last': None, 'count': 0})

    if 'read' not in temp:
        user['actions'].append({'type': 'read', 'last': None, 'count': 0})

    if 'update' not in temp:
        user['actions'].append({'type': 'update', 'last': None, 'count': 0})

    if 'delete' not in temp:
        user['actions'].append({'type': 'delete', 'last': None, 'count': 0})

for user in result:
    result_id = result_collection.insert_one(user).inserted_id
