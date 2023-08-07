import requests
import luigi
from bs4 import BeautifulSoup


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
            url = i.read().splitlines()[self.FileID]

            with self.output().open("w") as outfile:
                book_downloads = requests.get(url)
                book_text = book_downloads.text

                for char in self.REPLACE_LIST:
                    book_text = book_text.replace(char, " ")

                book_text = book_text.lower()
                outfile.write(book_text)
