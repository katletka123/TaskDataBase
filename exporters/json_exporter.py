import json
from exporters.exporter import Exporter

class JSONExporter(Exporter):
    def export(self, column_names, rows, file_name):
        result = [dict(zip(column_names, row)) for row in rows]

        with open(f"{file_name}.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)