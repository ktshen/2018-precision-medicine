import os
import queue
import threading
from abc import ABCMeta, abstractmethod
import xml.etree.ElementTree as ET
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# The amount of threads to process the file
THREADS_NUM = 16
stop_words = set(stopwords.words('english'))

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
            q = queue.Queue()
            for root, dirs, files in os.walk(path):
                for file in files:
                    if self.ext in file:
                        q.put(os.path.join(root, file))
            threads = []
            for i in range(THREADS_NUM):
                thread = threading.Thread(target=self.thread_worker, args=(q,))
                thread.start()
                threads.append(thread)
            q.join()
            for t in threads:
                t.join()
        else:
            raise FileNotFound(f"Can't found {path}")

    def thread_worker(self, q):
        while not q.empty():
            file = q.get()
            self.process_file(file)
            q.task_done()

    def process_file(self, file_path):
        """
            - ead the content from the file and tokenize the content.
            - Store the tokenized object to elasticsearch database
        """
        print("Processing {0}...".format(file_path))
        content = self.read_file(file_path)
        parsed_list = self.parse(content)
        for obj in parsed_list:
            obj["FilePath"] = file_path
            self.store(obj)

    def read_file(self, file):
        with open(file, "r") as f:
            content = f.read()
        return content

    @abstractmethod
    def parse(self, content):
        pass

    @abstractmethod
    def store(self):
        pass

    @staticmethod
    def tokenize(string):
        filtered_tokens = [w for w in word_tokenize(string) if not w in stop_words]
        return ' '.join(filtered_tokens)


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
                obj["ArticleTitle"] = self.tokenize(article.find("./MedlineCitation/Article/ArticleTitle").text)
                obj["AbstractText"] = self.tokenize(article.find("./MedlineCitation/Article/Abstract/AbstractText").text)
                keyword_list = article.find("./MedlineCitation/KeywordList/Keyword")
                if keyword_list:
                    obj["KeywordList"] = ". ".join([keyword.text for keyword in keyword_list])
                chemical_list = article.find("./MedlineCitation/ChemicalList")
                if chemical_list:
                    obj["ChemicalList"] =". ".join([chemical.text for chemical in chemical_list.findall("./Chemical/NameOfSubstance")])
                mesh_list = article.find("./MedlineCitation/MeshHeadingList")
                if mesh_list:
                    obj["MeshHeadingList"] =". ".join([mesh.text for mesh in mesh_list.findall("./MeshHeading/DescriptorName")])
            # If tag can't found, we ignore the item for now
            except AttributeError:
                continue

            parsed_list.append(obj)

        return parsed_list

    def store(self, obj):
        self.es.index(index="medlinexml", body=obj)


class ClinicalTrialsXMLParser(Parser):
    def __init__(self, es):
        self.es = es
        self.ext = ".xml"

    def parse(self, content):
        root = ET.fromstring(content)
        obj = {}
        try:
            obj["nct_id"] = next(root.iter("nct_id")).text
            obj["brief_summary"] = self.tokenize(root.find("./brief_summary/textblock").text)
            detailed_description = root.find("./detailed_description/textblock").text
            if detailed_description:
                obj["detailed_description"] = self.tokenize(detailed_description)
            criteria = root.find("./eligibility/criteria/textblock")
            if criteria:
                obj["criteria"] = self.tokenize(criteria)
            gender = root.find("./eligibility/gender").text
            if gender:
                obj["gender"] = gender

            minimum_age = root.find("./eligibility/minimum_age").text
            if minimum_age:
                obj["minimum_age"] = minimum_age
            maximum_age = root.find("./eligibility/maximum_age").text
            if maximum_age:
                obj["maximum_age"] = minimum_age
            mesh_term = root.findall("./condition_browse/mesh_term")
            if mesh_term:
                obj["mesh_term"] = ". ".join([term.text for term in mesh_term])
        except AttributeError:
            return []

        return [obj]

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
            if "Title:" in text:
                obj["Title"] = text[6:]
            else:
                obj["Text"] = self.tokenize(text)

        return [obj]

    def store(self, obj):
        self.es.index(index="extraabstracttxt", body=obj)
