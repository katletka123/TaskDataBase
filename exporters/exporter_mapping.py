from exporters.json_exporter import JSONExporter
from exporters.xml_exporter import XMLExporter

exporter_mapping={
    "xml":XMLExporter(),
    "json":JSONExporter()
}
