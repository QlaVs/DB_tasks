import pymongo
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.test
accruals = db.accrual
payments = db.payment
result = db.result
unused_payments = db.unused_payments

used_pays = {}
unused_pays = set()

for pay in payments.find().sort("date", pymongo.ASCENDING):
    f = True
    oldest_debt = None
    for debt in accruals.find({'date': {'$lte': pay['date']},
                               '_id': {'$nin': list(used_pays.values())}}).sort("date", pymongo.DESCENDING):
        if debt['month'] == pay['month']:
            used_pays[pay['_id']] = debt['_id']
            f = False
            break

        oldest_debt = debt['_id']

    if not f:
        continue
    elif oldest_debt is None:
        unused_pays.add(pay['_id'])
    else:
        used_pays[pay['_id']] = oldest_debt

for unused_pay in unused_pays:
    item = payments.find_one({'_id': unused_pay})
    unused_payment_id = unused_payments.insert_one(item).inserted_id

for used_pay in used_pays:
    p = payments.find_one({'_id': used_pay})
    ac = accruals.find_one({'_id': used_pays[used_pay]})
    result_item = {
        'payment': p,
        'accrual': ac
    }
    result_id = result.insert_one(result_item).inserted_id
