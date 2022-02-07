import xml.etree.ElementTree as ET
import os

tree = ET.parse(r'/home/rita/Desktop/aws_keys/aws_keys.xml'.format(os.path))

def getData(Data):
    root = tree.getroot()
    for ele in root.findall('add'):
        key = ele.get('key')
        value = ele.get('value')
        if(Data == key):
            result = value