from exporters.exporter import Exporter
import xml.etree.ElementTree as ET
from xml.dom import minidom


class XMLExporter(Exporter):
    def export(self,column_names, rows, file_name):
        root = ET.Element("data")
        for row in rows:
            row_tag = ET.SubElement(root, "row")
            for col_name, value in zip(column_names, row):
                element = ET.SubElement(row_tag, col_name)
                element.text = str(value)

        xml_str = ET.tostring(root, encoding='utf-8')
        parsed = minidom.parseString(xml_str)
        pretty_xml = parsed.toprettyxml(indent="  ")

        with open(f"{file_name}.xml", "w", encoding="utf-8") as f:
            f.write(pretty_xml)

