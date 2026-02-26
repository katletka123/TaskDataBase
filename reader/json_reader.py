import json
class JsonReader:
    def read_json(self, file_data: str):
        with open(file_data, "r", encoding="utf-8") as f:
            return json.load(f)