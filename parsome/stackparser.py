import bz2
import xml.sax
import xml.sax.handler
 
class Handler(xml.sax.handler.ContentHandler):
    def characters(self, data):
        self.__buffer = data
 
    def startElement(self, name, attrs):
        if name == "title":
            self.__buffer = ""
        if name == "text":
            self.__buffer2 = attrs.getValue("bytes")
            
    def endElement(self, name):
        if name == "title":
            print(self.__buffer)
        if name == "text":
            print(self.__buffer)
 
with bz2.open("parsome/enwiki.xml.bz2", "r") as stream:
    xml.sax.parse(stream, Handler())