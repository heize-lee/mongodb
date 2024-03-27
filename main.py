from pymongo import MongoClient

# MongoDB 인스턴스에 연결
# client = MongoClient('mongodb://hanslab.org:27117/')
client = MongoClient('mongodb://likelion:1234@hanslab.org:27117/')

# 데이터베이스 선택 (없으면 새로 생성됨)
# db = client['tutorial_db_imjonghan']
# db = client['tutorial_db_jihyun']

# 컬렉션 선택 (없으면 새로 생성됨) -> sql table
# collection = db['tutorial_collection']
# collection = db['tutorial_collection_jihyun_test']

#데이터베이스 조회
# db.list_collection_names() 
# print(db.list_collection_names())

# client.list_database_names()

# # 새 문서 생성 및 삽입
# document = {"name": "John Doe", "age": 30, "city": "New York"}
# collection.insert_one(document)

# document = {"name": "jihyun", "age": 100, "city": "Seoul"}
# collection.insert_one(document)

# document = {"name": "OpenAI", "age": 100, "country": "America"}
# collection.insert_one(document)


# # 모든 문서 조회
# for doc in collection.find():
#     print(doc)

# # 특정 조건에 맞는 문서 조회
# query = {"city": "Seoul"}
# documents = collection.find(query)
# for doc in documents:
#     print(doc)


# # 문서 업데이트
# collection.update_one(
#     {"name": "jihyun"},  # 조건
#     {"$set": {"age": 50}}  # 변경할 내용
# )

# # 모든 문서 조회
# for doc in collection.find():
#     print(doc)

# # 문서 삭제
# collection.delete_one({"name": "John Doe"})


# ##############
# 심화: 입력값에서 J로 시작하지 않는 값을 하나 더 추가해서 입력
# 업데이트, 딜리트, 수행하면서 db collection(users)의 내용 확인

# db = client['tutorial_db_jihyun']
# users = db.users

# # 'email' 필드에 대한 인덱스 생성, 오름차순
# users.create_index([('email', 1)], unique=True)

# # 여러문서 삽입
# try:
#     users.insert_many([
#         {"name": "John Doe", "email": "john@example.com"},
#         {"name": "Jane Doe", "email": "jane@example.com"},
#         {"name": "Jihyun", "email": "jihyun@example.com"},
#         {"name": "Heize", "email": "heize@example.com"},
#     ])
#     print("Documents inserted successfully.")
# except Exception as e:
#     print("An error occurred:", e)

# #update
# try:
#     result = users.update_many(
#         {"name": {"$regex": "^J"}},  # 이름이 J로 시작하는 모든 문서
#         {"$set": {"status": "verified"}}
#     )
#     print(f"{result.matched_count} documents matched, {result.modified_count} documents updated.")
# except Exception as e:
#     print("An error occurred:", e)

# #delete
# try:
#     result = users.delete_many({"status": "verified"})
#     print(f"{result.deleted_count} documents deleted.")
# except Exception as e:
#     print("An error occurred:", e)

# #print
# for doc in users.find():
#     print(doc)


# 데이터베이스와 컬렉션 선택
db = client['tutorial_db_jihyun']
db['users'].drop()  
users = db['users']

try:
    # 이메일 필드가 존재하는 문서만 업데이트
    result = users.update_many({"email": {"$exists": True}}, {"$unset": {"email": ""}})
    print(f"{result.modified_count} documents updated.")
except Exception as e:
    print("An error occurred:", e)

collection = db['users']

# 기존 데이터가 있다면 삭제 (새로운 실습을 위해)
collection.delete_many({})

# 샘플 데이터 삽입(단위 억)
sample_users = [
    {"name": "Alice", "balance": 18},
    {"name": "Bob", "balance": 25},
    {"name": "Charlie", "balance": 35},
    {"name": "David", "balance": 45},
    {"name": "Eve", "balance": 55},
    {"name": "Frank", "balance": 65}
]

collection.insert_many(sample_users)

pipeline = [
    {"$addFields": {
        "rc": {
            "$switch": {
                "branches": [
                    {"case": {"$lte": ["$age", 60]}, "then": "rich+"},
                    {"case": {"$lte": ["$age", 40]}, "then": "rich"},
                    {"case": {"$lte": ["$age", 20]}, "then": "rich-"},
                ],
                "default": "soso"
            }
        }
    }}
]

results = collection.aggregate(pipeline)

for result in results:
    print(result)