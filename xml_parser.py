import os
from abc import ABCMeta, abstractmethod
import xml.etree.ElementTree as ET
from nltk.tokenize import word_tokenize

class Parser:

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

    def get_all_files_and_process(self, path):
        """
            Process all the files in the target directory and subdirectory
        """
        if os.path.isfile(path) and self.ext in path:
            self.process_file(path)
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    if self.ext in file:
                        self.process_file(os.path.join(root, file))
        else:
            raise FileNotFound(f"Can't found {path}")

    def process_file(self, file_path):
        """
            - ead the content from the file and tokenize the content.
            - Store the tokenized object to elasticsearch database
        """
        content = self.read_file(file_path)
        parsed_list = self.parse(content)
        for obj in parsed_list:
            obj["FilePath"] = file_path
            self.store(obj)

    def read_file(self, file):
        with open(file, "r") as f:
            content = f.read(file)
        return content

    @abstractmethod
    def parse(self, content):
        pass

    @abstractmethod
    def store(self):
        pass

    @abstractmethod
    def tokenize(self):
        pass


class MedlineXMLParser(Parser):
    def __init__(self, es):
        self.es = es
        self.ext = ".xml"

    def parse(self, content):
        root = ET.fromstring(content)
        parsed_list = []

        for article in root.findall("PubmedArticle"):
            obj = {}
            try:
                obj["PMID"] = article.find("./MedlineCitation/PMID").text
                obj["ArticleTitle"] = article.find("./MedlineCitation/Article/ArticleTitle").text
                obj["AbstractText"] = article.find("./MedlineCitation/Article/Abstract/AbstractText").text
            # If tag can't found, we ignore the item for now
            except AttributeError:
                continue
            self.tokenize(obj)
            parsed_list.append(obj)

        return parsed_list

    def tokenize(self, obj):
        obj["ArticleTitle"] = word_tokenize(obj["ArticleTitle"])
        obj["AbstractText"] = word_tokenize(obj["AbstractText"])

    def store(self, obj):
        self.es.index(index="medlinexml", body=obj)


class ClinicalTrialsXMLParser(Parser):
    def __init__(self, es):
        self.es = es
        self.ext = ".xml"

    def parse(self, content):
        root = ET.fromstring(content)
        obj = {}
        obj["nct_id"] = next(root.iter("nct_id")).text
        obj["brief_summary"] = root.find("./brief_summary/textblock").text
        obj["detailed_description"] = root.find("./detailed_description/textblock").text
        obj["criteria"] = root.find("./eligibility/criteria/textblock")
        obj["gender"] = root.find("./eligibility/gender").text
        obj["minimum_age"] = root.find("./eligibility/minimum_age").text
        obj["maximum_age"] = root.find("./eligibility/maximum_age").text
        obj["mesh_term"] = ". ".join([term.text for term in root.findall("./condition_browse/mesh_term")])

        return [obj]

    def tokenize(self, obj):
        obj["brief_summary"] = word_tokenize(obj["brief_summary"])
        obj["detailed_description"] = word_tokenize(obj["detailed_description"])

    def store(self, obj):
        self.es.index(index="clinicaltrialsxml", body=obj)


class ExtraAbstractTXTParser(Parser):
    def __init__(self, es):
        self.es = es
        self.ext = ".txt"

    def parse(self, content):
        obj = {}
        for text in content.split("\n"):
            if not text or "Meeting:" in text:
                continue
            if "Title:":
                obj["Title"] = text[6:]
            else:
                obj["Text"] = text

        return [obj]

    def tokenize(self, obj):
        obj["Title"] = word_tokenize(obj["Title"])
        obj["Text"] = word_tokenize(obj["Text"])

    def store(self, obj):
        self.es.index(index="extraabstracttxt", body=obj)
