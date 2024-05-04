import asyncio
from datetime import datetime, date, timedelta
import json

from motor.motor_asyncio import AsyncIOMotorClient
from telebot.async_telebot import AsyncTeleBot
from telebot.types import Message

from config import settings
from schemas import AggregateRequest


bot = AsyncTeleBot(token=settings.BOT_TOKEN)

mongo_client = AsyncIOMotorClient(settings.MONGO_URL)
company_db = mongo_client["company"]
salary_collection = company_db["salary"]


@bot.message_handler(content_types="text")
async def get_aggregated_salaries(message: Message):
    try:
        data = AggregateRequest(**json.loads(message.json["text"]))
    except TypeError:
        await bot.send_message(message.chat.id, "Invalid request body")

    start = datetime.strptime(data.dt_from, "%Y-%m-%dT%H:%M:%S")
    end = datetime.strptime(data.dt_upto, "%Y-%m-%dT%H:%M:%S")

    if data.group_type.value == "month":
        diff = end.month - start.month + 1

        response = {
            "dataset": [0 for _ in range(diff)], 
            "labels": [date(start.year, month, 1).strftime("%Y-%m-%dT%H:%M:%S") for month in range(start.month, end.month + 1)]
        }

        async for doc in salary_collection.aggregate([
            {"$match": {"dt": {"$gte": start, "$lte": end}}},
            {"$group": {"_id": {"year": {"$year": "$dt"}, "month": {"$month": "$dt"}}, "dataset": {"$sum": "$value"}}},
            {"$sort": {"_id.year": 1, "_id.month": 1}},
        ]):
            label = date(doc['_id']['year'], doc['_id']['month'], 1).strftime("%Y-%m-%dT%H:%M:%S")
            index = response["labels"].index(label)

            response["dataset"][index] = doc["dataset"]
    elif data.group_type.value == "day":
        diff = (end - start).days + 1

        labels = []
        start_date = start
        while start_date <= end:
            labels.append(start_date.strftime("%Y-%m-%dT%H:%M:%S"))
            start_date += timedelta(days=1)

        response = {"dataset": [0 for _ in range(diff)], "labels": labels}

        async for doc in salary_collection.aggregate([
            {"$match": {"dt": {"$gte": start, "$lte": end}}},
            {"$group": {"_id": {"year": {"$year": "$dt"}, "month": {"$month": "$dt"}, "day": {"$dayOfMonth": "$dt"}}, "dataset": {"$sum": "$value"}}},
            {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1}},
        ]):
            label = date(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']).strftime("%Y-%m-%dT%H:%M:%S")
            index = response["labels"].index(label)

            response["dataset"][index] = doc["dataset"]
    else: # data.group_type.value == "hour"
        labels = []
        start_date = start
        while start_date <= end:
            labels.append(start_date.strftime("%Y-%m-%dT%H:%M:%S"))
            start_date += timedelta(hours=1)

        response = {
            "dataset": [0 for _ in range(len(labels))],
            "labels": labels
        }

        async for doc in salary_collection.aggregate([
            {"$match": {"dt": {"$gte": start, "$lte": end}}},
            {"$group": {"_id": {"year": {"$year": "$dt"}, "month": {"$month": "$dt"}, "day": {"$dayOfMonth": "$dt"}, "hour": {"$hour": "$dt"}}, "dataset": {"$sum": "$value"}}},
            {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1, "_id.hour": 1}},
        ]):
            label = datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day'], doc['_id']['hour']).strftime("%Y-%m-%dT%H:%M:%S")
            index = response["labels"].index(label)

            response["dataset"][index] = doc["dataset"]

    await bot.send_message(message.chat.id, json.dumps(response, indent=4))


async def main():
    print("Started Bot")
    await bot.polling(non_stop=True)
    print("Finished Bot")


if __name__ == "__main__":
    asyncio.run(main())