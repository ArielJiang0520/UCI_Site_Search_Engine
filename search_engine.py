from PyQt5.QtWidgets import QWidget, QApplication, QLabel, QLineEdit, QPushButton, QVBoxLayout, QListWidgetItem, QListWidget
from PyQt5.Qt import QFont
from PyQt5.QtCore import Qt
import mysql.connector
import sys
import os
import json
import lxml.html
from lxml.html.clean import Cleaner

import engine

class SearchEngine(QWidget):
    def __init__(self):
        super().__init__()
        self.setup_window()
##        self.setup_connection()
        
        
    def setup_window(self):
        '''Sets up the window size, icon, components, and style'''
        self.height = 1600
        self.width = 2400
        self.x = 400
        self.y = 150
        
        self.setGeometry(self.x, self.y, self.width, self.height)
        self.setFixedSize(self.width, self.height)
        self.setWindowTitle("Search Engine")
        
        self.font = QFont("Verdana", 12)
        
        self.search_btn = self.create_search_btn() 
        self.search_box = self.create_search_box()
        self.results_lbl = QLabel("No results to show", self)
        self.results_lbl.move(10, self.height - 40)
        self.results_lbl.setFixedSize(300, 30)
        self.results_box = self.create_results_box()
        
        with open("stylesheet.css") as ss:
            style = ss.read()
            self.search_btn.setStyleSheet(style)
            self.search_box.setStyleSheet(style)
            self.results_box.setStyleSheet(style)
            
            
##    def setup_connection(self):
##        '''Sets up a connection to the database'''
##        self.database = mysql.connector.connect(
##            host = "localhost",
##            user = "root",
##            password = "password"
##        )
##        self.db_cursor = self.database.cursor()
        
    
    def create_search_box(self) -> QLineEdit:
        '''Creates the search box where the user specifies a query'''
        search_box = QLineEdit(self)
        search_box.setFont(self.font)
        search_box.setFixedSize(self.width - self.search_btn.width() - 15, 80)
        search_box.move(10,10)
        search_box.returnPressed.connect(self.search)
        search_box.setFocus(True)
        return search_box
        
        
    def create_search_btn(self) -> QPushButton:
        '''Creates the search button that will send a request to the database to
        retrieve the relevant search results
        '''
        search_btn = QPushButton("Search", self)
        search_btn.setFont(self.font)
        search_btn.setFixedSize(280, 80)
        search_btn.move(self.width - search_btn.width() - 10, 10)
        search_btn.clicked.connect(self.search)
        search_btn.setCursor(Qt.PointingHandCursor)
        return search_btn
        
        
    def create_results_box(self) -> QWidget:
        '''Creates the area where the SearchResultItems will be shown'''
        search_results = QListWidget(self)
        search_results.setFont(QFont("Verdana", 9))
        search_results.setFixedSize(self.width - 20, self.height - self.search_box.height() - 30 - self.results_lbl.height())
        search_results.move(10, self.search_box.height() + 20)
        return search_results

##    def get_file_name(self, url,):
##        addr = url_file_map[url].split("/")
##        dir = addr[0]
##        file = addr[1]
##        return os.path.join(".", self.WEBPAGES_RAW_NAME, dir, file)
        
    def search(self):
        '''Searches the database for relevant pages, then updates the results box
        to show any matching results
        '''
        # Clear the previous search results
        self.results_box.clear()

        # search index for documents/urls relevant to searh_query
        global FILE_URL_MAP
        search_query = self.search_box.text()
        results, num_results = engine.search(search_query)
        url_list = []
        if len(results[0]) == 0:
            return
        WEBPAGES_RAW_NAME = "WEBPAGES_RAW"
        JSON_FILE_NAME = os.path.join(".", WEBPAGES_RAW_NAME, "bookkeeping.json")
        FILE_URL_MAP = json.load(open(JSON_FILE_NAME), encoding="utf-8")
            
        cleaner = Cleaner()
        for filename in results:
            with open(os.path.join(".", WEBPAGES_RAW_NAME, filename),"rb") as fp:
                html_string = fp.read()
                cleaned_document = lxml.html.document_fromstring(cleaner.clean_html(html_string))
                #print((FILE_URL_MAP[filename],cleaned_document.text_content()))
                url_list.append((FILE_URL_MAP[filename],cleaned_document.text_content()))
        for i in url_list:
            print(i[0])
            
        #results = 5 * [(search_query, f"www.{search_query}.com", "example cont\nent")]
        self.show_results(search_query, url_list, num_results)
        
        
    def show_results(self, query: str, results: list, n: int) -> None:
        '''Displays the results from the search if there are any, or displays
        a message that says results were found
        '''
        if len(results) == 0:
            self.results_box.addItem(QLabel("No results to show!"))
        for result in results:
            item = QListWidgetItem(self.format_result(result))
            self.results_box.addItem(item)
        self.update_results_label(n)
            
            
    def update_results_label(self, num: int or str) -> None:
        '''Updates the results label to show the number of results
        that have been found
        '''
        if num == 0:
            self.results_lbl.setText("No results to show")
        else:
            self.results_lbl.setText(f"Found {num} results")
        
        
    @staticmethod
    def format_result(result: (str, str)) -> None:
        '''Formats the result so that the title of the site is on one line, the 
        URL is on another line, and then the content preview is broken up into
        several lines if necessary
        '''
       
        # Break results tuple down to respective parts
        url, content = result
        
        # Format longer content strings to display up to three lines
        formatted_content = ""
        count = 0
        for char in content[:400]:
            if count == 100:
                formatted_content += "\n"
                count = 0
            if count == 0 and char == " " or char == '\n':
                continue
            if count == 0:
                formatted_content += "  "
            formatted_content += char
            count += 1
            
        if len(formatted_content) > 380:
            formatted_content = formatted_content.rstrip() + "..."
            
        return f'{url}\n{formatted_content}'
        
    
if __name__ == "__main__":
       
    

    qapp = QApplication([])
    SE = SearchEngine()
    SE.show()
    qapp.exec_()
    SE.database.close()
