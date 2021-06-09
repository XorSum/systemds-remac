import os
import sys
import re
import codecs

def find_str(rootdir,pattern):
    l = list(os.walk(rootdir))
    p = re.compile(pattern)
    for path,dir_list,file_list in l:
        for file_name in file_list:
            name = os.path.join(path, file_name)
            try:
                with codecs.open(name,'r','utf-8') as f:
                    c = f.read()
                    if p.search(c) is not None:
                        print(name)
            except:
                pass
if __name__ =='__main__':
    find_str('./src','uak+')
