import argparse
from pathlib import Path
from xml_parser import MedlineXMLParser, ClinicalTrialsXMLParser, ExtraAbstractTXTParser
from elasticsearch import Elasticsearch


if "__name__" == __main__:
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--directory", type="str", required=True)
    parser.add_argument("-m", "--model", type="int", required=True, help="1: MedlineXMLParser 2: ClinicalTrialsXMLParser 3:ExtraAbstractTXTParser")
    parser.add_argument("-i", "--elastic_ip", type="str", default="localhost", required=False)

    args = parser.parse_args()
    directory = Path(args.directory)
    Parser = {1: MedlineXMLParser, 2: ClinicalTrialsXMLParser, 3: ExtraAbstractTXTParser}[args.model]

    elastic_ip = args.elastic_ip
    es = Elasticsearch(hosts=[{'host': elastic_ip, 'port': 9200}])

    parser = Parser(es)
    parser.get_all_files_and_process(directory)
