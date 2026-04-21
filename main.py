from pymongo import MongoClient
from src.preprocess import preprocess
from src.process import process
from src.postprocess import postprocess

def main():

    # Connect to MongoDB 
    client = MongoClient('mongodb+srv://user:user@cluster0.3phzlyt.mongodb.net/')

    preprocess(client)

    process(client)

    postprocess(client)

if __name__ == "__main__":
    main()