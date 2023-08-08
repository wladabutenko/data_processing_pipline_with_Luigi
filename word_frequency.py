import requests
import luigi
from bs4 import BeautifulSoup
from collections import Counter
import pickle
import io
import codecs


class GetTopBooks(luigi.Task):
    """
     Get list of the most popular books from Project Gutenberg
    """

    def output(self):
        return luigi.LocalTarget('data/books_list.txt')

    def run(self):
        resp = requests.get('http://www.gutenberg.org/browse/scores/top')

        soap = BeautifulSoup(resp.content, 'html.parser')

        page_header = soap.find_all("h2", string="Top 100 EBooks yesterday")[0]
        list_top = page_header.find_next_sibling("ol")

        with self.output().open("w") as f:
            for result in list_top.select("li>a"):
                if "/ebooks/" in result["href"]:
                    f.write("http://www.gutenberg.org{link}.txt.utf-8\n"
                    .format(
                        link=result["href"]
                    )
                    )


class DownloadBooks(luigi.Task):
    """
    Download a specified list of books
    """
    FileID = luigi.IntParameter()

    REPLACE_LIST = """.,"';_[]:*-"""

    def requires(self):
        return GetTopBooks()

    def output(self):
        return luigi.LocalTarget("data/downloads/{}.txt".format(self.FileID))

    def run(self):
        with self.input().open("r") as i:
            book_lines = i.read().splitlines()

            if self.FileID < 0 or self.FileID >= len(book_lines):
                raise ValueError("Invalid FileID. The index is out of range.")

            URL = book_lines[self.FileID]

            book_downloads = requests.get(URL)
            book_text = book_downloads.text

            for char in self.REPLACE_LIST:
                book_text = book_text.replace(char, " ")

            book_text = book_text.lower()

            with io.open(self.output().path, mode="w", encoding="utf-8") as outfile:
                outfile.write(book_text)


class CountWords(luigi.Task):
    """
        Count the frequency of the most common words from a file
        """

    FileID = luigi.IntParameter()

    def requires(self):
        return DownloadBooks(FileID=self.FileID)

    def output(self):
        return luigi.LocalTarget(
            "data/counts/count_{}.pickle".format(self.FileID),
            format=luigi.format.Nop
        )

    def run(self):
        with codecs.open(self.input().path, "r", encoding='ISO-8859-1') as i:
            content = i.read()

        word_count = Counter(content.split())

        with self.output().open("wb") as outfile:
            pickle.dump(word_count, outfile)


class GlobalParams(luigi.Config):
    """Defining Configuration Parameters in the pipline. These will allow to customize how many
    books to analyze and the number of words to include in the results.

    To set parameters that are shared among tasks, it is needed to create a Config() class. Other pipeline stages
    can reference the parameters defined in the Config() class."""
    NumberBooks = luigi.IntParameter(default=10)
    NumberTopWords = luigi.IntParameter(default=500)


class TopWords(luigi.Task):
    """
    Aggregate the count results from the different files
    """

    def requires(self):
        required_inputs = []
        for i in range(GlobalParams().NumberBooks):
            required_inputs.append(CountWords(FileID=i))
        return required_inputs

    def output(self):
        return luigi.LocalTarget("data/summary.txt")

    def run(self):
        total_count = Counter()
        for inputs in self.input():
            with inputs.open("rb") as infile:
                next_counter = pickle.load(infile)
                total_count += next_counter

        with codecs.open(self.output().path, "w", encoding='utf-8') as f:
            for item in total_count.most_common(GlobalParams().NumberTopWords):
                f.write("{0: <15}{1}\n".format(*item))