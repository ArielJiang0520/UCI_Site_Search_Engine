from __future__ import print_function
import json
import os
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import mysql.connector
from collections import defaultdict
import lxml.html
from lxml.html.clean import Cleaner
import re
import time


class Error(Exception):
   pass

class NoIndex(Error):
   pass

class TooManyNumbers(Error):
   pass

class Corpus:
   """
   This class is responsible for handling corpus related functionalities like mapping a url to its local file name
   """

   # The corpus directory name
   WEBPAGES_RAW_NAME = "WEBPAGES_RAW"
   # The corpus JSON mapping file
   JSON_FILE_NAME = os.path.join(".", WEBPAGES_RAW_NAME, "test.json")

   def __init__(self):
     self.file_url_map = json.load(open(self.JSON_FILE_NAME), encoding="utf-8")
     self.url_file_map = dict()
     for key in self.file_url_map:
         self.url_file_map[self.file_url_map[key]] = key


   def get_file_name(self, url):
     """
     Given a url, this method looks up for a local file in the corpus and, if existed, returns the file address. Otherwise
     returns None
     """
   ##        url = url.strip()
   ##        parsed_url = urlparse(url)
   ##        url = url[len(parsed_url.scheme) + 3:]
     if url in self.url_file_map:
         #print('here')
         addr = self.url_file_map[url].split("/")
         dir = addr[0]
         file = addr[1]
         return os.path.join(".", self.WEBPAGES_RAW_NAME, dir, file)
     return None

# List Node class and helper functions (to set up problem)

class LN:
 def __init__(self,documentID,frequency,next=None):
     self.documentID = documentID
     self.frequency = frequency
     self.next = next

def is_valid(url):
     """
     Function returns True or False based on whether the url has to be fetched or not. This is a great place to
     filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
     in this method
     """
     parsed = urlparse(url)
     for i in set(['calendar','login']):
         if i in parsed.query:
             return False
     if has_repetitive_pattern(parsed.path) or is_super_long(parsed.query, parsed.path):
         return False
     if re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                 + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                 + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                 + "|thmx|mso|arff|rtf|jar|csv" \
                                 + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):
         return False
     return True
     
     
def has_repetitive_pattern(path):
 """
 Function that checks whether the path of the url has repeating paths
 """
 parts = path.split("/")
 
 d = defaultdict(int)
 lastword = ""
 for each in parts:
     d[each] += 1
     if d[each] > 3:
         return True
 return False

def is_super_long(query, path):
 return (len(path) > 200 or len(query) > 50)

class Index:
   def __init__(self):
      self.dict = defaultdict(int)
      self.dictDF = defaultdict(int)
      self.corpus = Corpus()
      self.mydb = mysql.connector.connect(
         host = 'localhost',
         user = 'root',
         passwd = '123456',
         database = 'CS121')
      self.mycursor = self.mydb.cursor()

   def log(self,text):
      with open("log.txt","a") as l:
         l.write(text)
         l.write('\n')

   def clear_log(self):
      try:
         os.remove("log.txt")
      except:
         pass
     
   def addDF(self):
      for key in self.dictDF:
         sql = '''INSERT INTO DocumentFrequency(term,documentFrequency)
   VALUES (%s, %s)'''
         val =(key,self.dictDF[key])
         self.mycursor.execute(sql,val)
         self.mydb.commit()
         
   '''add a word to self.dict accordingly'''
   def addWord(self,word):
      if len(word) <= 35 and not self.single_digit(word):
         self.dict[word]+=1

   def is_alphanumeric(self,n):
      return ((n>=48 and n<=57) or (n>=97 and n <=122))

   def is_numeric(self,n):
      return (n>=48 and n<=57)

   def is_number(self,s):
      try:
         float(s)
         return True
      except ValueError:
         return False

   def single_digit(self,s):
      try:
         if (int(s) in set([0,1,2,3,4,5,6,7,8,9])):
            return True
         return False
      except ValueError:
         return False
      

   '''takes a cleaned html string and start to tokenize it'''
   def tokenize(self,doc):
      word = ""
      length = 0
      consecutive_number_counter = 0
      total_number_counter = 0
      for i in doc:
         if total_number_counter >= 400 or consecutive_number_counter >= 100:
             raise TooManyNumbers
         length += 1
         i = i.lower()
         n = ord(i)
         if self.is_alphanumeric(n):
             word+=i; 
             if length == len(doc):
                  if self.is_number(word):
                     total_number_counter+=1
                     consecutive_number_counter+=1
                  else:
                     consecutive_number_counter = 0
                  self.addWord(word)
                 
         elif (n==46 or n==45):
             if word != "" and self.is_numeric(ord(word[-1])) and self.is_numeric(ord(doc[length])):
                  word+=i;
             else:
                  if word != "":
                     if self.is_number(word):
                        total_number_counter+=1
                        consecutive_number_counter+=1
                     else:
                        consecutive_number_counter = 0
                     self.addWord(word)
                  word = ""
         else: 
             if word != "":
                  if self.is_number(word):
                     total_number_counter+=1
                     consecutive_number_counter+=1
                  else:
                     consecutive_number_counter = 0
                  self.addWord(word)
             word = ""
      return

   def addTF(self,fn):
      for key in self.dict:
         try:
             sql = '''INSERT INTO TermFrequency(term,documentID,termfrequency)
   VALUES (%s, %s, %s)'''
             val =(key,fn,self.dict[key])
             self.mycursor.execute(sql,val)
             self.mydb.commit()
         except Exception as e:
             print(e,key,fn)
      return

   def percentage(self,number):
      return float((number/36926)*100)
                      
   def indexing(self):
      self.clear_log()
      cleaner = Cleaner()
      counter = 0
      for url in self.corpus.url_file_map:
         try:
            self.dict=defaultdict(int)

            file_name = self.corpus.get_file_name(url)
            #print(str(file_name))
            self.log(str(file_name))
##            counter += 1
##            print("Progress {:10.4f}".format((self.percentage(counter))),end='\n')

            if is_valid(url):
               with open(file_name,"rb") as fp:
                    
                     start = time.time()
                     
                     html_string = fp.read()
                     document = lxml.html.document_fromstring(html_string)
                     metaContent = document.cssselect('meta[name="robots"]')
                     if len(metaContent)>0 and ('noindex' in metaContent[0].get('content')):
                         raise NoIndex

                     spaced_html = re.sub("</", " </", html_string.decode("utf-8"))
                     spaced_html = bytes(spaced_html,encoding="utf-8")


                     cleaned_document = lxml.html.document_fromstring(cleaner.clean_html(spaced_html))
                     text = re.sub(' +',' ',cleaned_document.text_content())

                     self.tokenize(text)
                     self.addTF(str(file_name))
                     for key in self.dict:
                         self.dictDF[key]+=1

                     end = time.time()

                     if (end-start) > 10:
                        self.log("Extremely long time")
                     self.log(str(end-start))

             #input('continue?')
            else:
                 #print("Invalid URL. passed")
                 self.log("Invalid URL. passed")

         except NoIndex:
             #print("Noindex. passed")
             self.log("Noindex. passed")
             pass

         except TooManyNumbers:
             #print("Document has too many numbers. passed")
             self.log("Document has too many numbers. passed")
             pass

         except Exception as e:
            self.log(str(e))
            pass

##      self.addDF()
     
      return


if __name__ == "__main__":
 i = Index()  
 i.indexing()



