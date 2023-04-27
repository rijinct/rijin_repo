'''
Created on 26-Feb-2020

@author: rithomas
'''
import xml.etree.ElementTree as ET
import subprocess
import lxml.etree
import os

def modify_and_regenerate_xml(input_xml_filename, output_xml_filename, tag_xpath_loc, executor, subproc_script):
    tree = ET.parse(input_xml_filename)
    root = tree.getroot()
    print(tag_xpath_loc)
    for elem in root.findall(tag_xpath_loc):
        print (elem.tag)
        #exit(0)
        #print(subproc_script)
        process = subprocess.Popen([executor, subproc_script, "{}".format(elem.text)], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].decode('utf-8').strip()
        #print(process)
        elem.text = str(process).replace('[', '').replace(']', '')
        print (elem.tag, elem.text, elem.attrib)
        if elem.tag == 'ultimateSourcetable':
            elem.text = 'hi'
            #redo
        #exit(0)
    #tree.write(output_xml_filename)

    
def transform_xml(process_xsl_filename, input_xml_filename, output_xml_filename, write_format):
    parser = lxml.etree.XMLParser(ns_clean=True, recover=True)
    input_xml_parser = lxml.etree.parse(input_xml_filename, parser)
    process_xsl = lxml.etree.parse(process_xsl_filename)
    xsl_transform = lxml.etree.XSLT(process_xsl)
    transform_xml = xsl_transform(input_xml_parser)
    if write_format == 'a':
        write_or_append_to_xml_file(output_xml_filename, transform_xml, write_format)
    else:
        write_or_append_to_xml_file(output_xml_filename, transform_xml, write_format)

        
def get_absolute_filename(output_filename):
    return os.popen("readlink -f {0}".format(output_filename)).read()

    
def write_or_append_to_xml_file(output_xml_filename, transform_xml, write_format):  
    with open(output_xml_filename, write_format) as f:
        f.write(str(transform_xml))
        
if __name__ == "__main__":
   input_xml_filename = r'C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\conf\test.xml'
   output_xml_filename= r'C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\conf\output.xml'
   process_xsl_filename= r'C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\conf\technicalMetadata.xsl'
   tag_xpath_loc = "./row/"
   executor = 'python'
   subproc_script = 'echoVal.py'
   #print('test')
   #print(os.getcwd())
   print('hi')
   #modify_and_regenerate_xml(input_xml_filename, output_xml_filename, tag_xpath_loc, executor, subproc_script)
   transform_xml(process_xsl_filename, input_xml_filename, output_xml_filename, 'w')
    
