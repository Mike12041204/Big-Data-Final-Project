from pymongo import MongoClient
from src.rawLayer import rawLayer
from src.cleanLayer import cleanLayer
from src.aggregateLayer import aggregateLayer

def main():

    # Connect to MongoDB 
    client = MongoClient('mongodb+srv://user:user@cluster0.3phzlyt.mongodb.net/')
    # Reset database
    db = client['database']
    collection = db['collection']
    collection.delete_many({})

    rawLayer(client)

    cleanLayer(client)

    aggregateLayer(client)


if __name__ == "__main__":
    main()
