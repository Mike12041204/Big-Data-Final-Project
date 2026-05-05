import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pymongo import MongoClient
from project.src.rawLayer import rawLayer
from project.src.cleanLayer import cleanLayer
from project.src.aggregateLayer import aggregateLayer

def project():

    # Connect to MongoDB 
    client = MongoClient('mongodb+srv://user:user@cluster0.3phzlyt.mongodb.net/')
    client['raw']['rawData'].delete_many({})

    print(" - Wiped database")

    rawLayer(client)

    cleanLayer(client)

    aggregateLayer(client)