import os
import queue
import threading
import time
import traceback
from abc import ABCMeta, abstractmethod
import xml.etree.ElementTree as ET
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from elasticsearch.exceptions import ConnectionTimeout

stop_words = set(stopwords.words('english'))

class Parser:

    __metaclass__ = ABCMeta

    def __init__(self, es, threads_num):
        self.es = es
        self.threads_num = threads_num

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
            for i in range(self.threads_num):
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
        if self.check_if_store_already(file_path):
            return
        content = self.read_file(file_path)
        try:
            parsed_list = self.parse(content)
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            traceback.print_exc()
            return False
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

    def store(self, obj):
        """
            Try several times if connection timeout
        """
        counter = 5
        while counter > 0:
            try:
                self.es.index(index=self.index, body=obj)
                counter = 0
            except ConnectionTimeout:
                counter -= 1
                time.sleep(1)

    @staticmethod
    def tokenize(string):
        filtered_tokens = [w for w in word_tokenize(string) if not w in stop_words]
        return ' '.join(filtered_tokens)

    def check_if_store_already(self, term):
        body =  { "query": { "bool": { "must": { "match_phrase": {"FilePath": term } } } } }
        resp = self.es.search(index=self.index, body=body)
        if resp["hits"]["total"]["value"] == 0:
            return False
        else:
            return True


class MedlineXMLParser(Parser):
    def __init__(self, es, threads_num):
        super().__init__(es, threads_num)
        self.ext = ".xml"
        self.index = "medlinexml"

    def parse(self, content):
        root = ET.fromstring(content)
        parsed_list = []

        for article in root.findall("PubmedArticle"):
            obj = {}
            pmid = article.find("./MedlineCitation/PMID")
            if pmid is None or pmid.text is None:
                continue
            obj["PMID"] = pmid.text
            article_title = article.find("./MedlineCitation/Article/ArticleTitle")
            abstract_text = article.find("./MedlineCitation/Article/Abstract/AbstractText")
            if article_title is None or abstract_text is None or pmid.text is None or article_title.text is None or abstract_text.text is None:
                continue
            obj["ArticleTitle"] = self.tokenize(article_title.text)
            obj["AbstractText"] = self.tokenize(abstract_text.text)
            keyword_list = article.find("./MedlineCitation/KeywordList/Keyword")
            if keyword_list:
                obj["KeywordList"] = ". ".join([keyword.text for keyword in keyword_list])
            chemical_list = article.find("./MedlineCitation/ChemicalList")
            if chemical_list:
                obj["ChemicalList"] =". ".join([chemical.text for chemical in chemical_list.findall("./Chemical/NameOfSubstance")])
            mesh_list = article.find("./MedlineCitation/MeshHeadingList")
            if mesh_list:
                obj["MeshHeadingList"] =". ".join([mesh.text for mesh in mesh_list.findall("./MeshHeading/DescriptorName")])

            parsed_list.append(obj)

        return parsed_list





class ClinicalTrialsXMLParser(Parser):
    def __init__(self, es, threads_num):
        super().__init__(es, threads_num)
        self.ext = ".xml"
        self.index = "clinicaltrialsxml"

    def parse(self, content):
        root = ET.fromstring(content)
        obj = {}
        nct_id = root.find("./id_info/nct_id")
        brief_summary = root.find("./brief_summary/textblock")
        if nct_id is None or brief_summary is None or nct_id.text is None or brief_summary.text is None:
            return []
        obj["nct_id"] = nct_id.text
        obj["brief_summary"] = self.tokenize(brief_summary.text)
        detailed_description = root.find("./detailed_description/textblock")
        if detailed_description and detailed_description.text:
            obj["detailed_description"] = self.tokenize(detailed_description.text)
        criteria = root.find("./eligibility/criteria/textblock")
        if criteria and criteria.text:
            obj["criteria"] = self.tokenize(criteria.text)
        gender = root.find("./eligibility/gender")
        if gender and gender.text:
            obj["gender"] = gender.text
        minimum_age = root.find("./eligibility/minimum_age")
        if minimum_age and minimum_age.text:
            obj["minimum_age"] = minimum_age.text
        maximum_age = root.find("./eligibility/maximum_age")
        if maximum_age and maximum_age.text:
            obj["maximum_age"] = minimum_age.text
        mesh_term = root.findall("./condition_browse/mesh_term")
        if mesh_term:
            obj["mesh_term"] = ". ".join([term.text for term in mesh_term])

        return [obj]


class ExtraAbstractTXTParser(Parser):
    def __init__(self, threads_num)):
        super().__init__(es, threads_num)
        self.ext = ".txt"
        self.index = "extraabstracttxt"

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
