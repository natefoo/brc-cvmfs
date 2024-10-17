#!/usr/bin/env python3
import sys
import xml.etree.ElementTree as ET


tree = ET.parse(sys.argv[1])
root = tree.getroot()
for child in root:
    #table_name = child.attrib["name"]
    columns_tag = child.find("columns")
    file_tag = child.find("file")
    columns = [x.strip() for x in columns_tag.text.split(",")]
    if "name" not in columns:
        continue
    dbkey_column = columns.index("value")
    name_column = columns.index("name")
    file_path = file_tag.attrib["path"]
    print(f"{file_path}\t{dbkey_column}\t{name_column}")
    
