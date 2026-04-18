import json

sample_json = """{
"id": 143,
"name": "Silicon Valley",
"type": "Scripted",
"language": "English",
"genres": [
"Comedy"
],
"network": {
"id": 8,
"name": "HBO",
"country": {
"name": "United States",
"code": "US",
"timezone": "America/New_York"
}
}
}"""

document = json.loads(sample_json)

print(document)