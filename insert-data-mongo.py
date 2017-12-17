from pymongo import MongoClient

DATABASE_NAME = 'zalo'
COLLECTION_NAME = 'hackathon'

mongo_client = MongoClient()
collection = mongo_client.get_database(DATABASE_NAME).get_collection(COLLECTION_NAME)

with open("data.txt", "rb") as file:
    lines = file.readlines()

    user_item_count = dict()

    for line in lines:
        token = line.decode('utf-8').split('\t')

        user = token[0]
        item = token[1]

        if user not in user_item_count:
            user_item_count[user] = dict()

        if item not in user_item_count[user]:
            user_item_count[user][item] = 0

        user_item_count[user][item] = user_item_count[user][item] + 1

    for user, items in user_item_count.items():
        for item, count in items.items():
            collection.insert({'user': user, 'item': item, 'count': count})
            #print({'user': user, 'item': item, 'count': count})
