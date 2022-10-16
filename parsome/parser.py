# Python standard lib
import multiprocessing
import re
import threading
import time
import os
import xml
import bz2
import argparse
import json
import re
# Non-standard libs
# None
# Local modules
import wiki_reader
import cleaning
# import processing



def _dbg(_var):
    with open('debug.txt', 'w') as f:
        print(_var, file=f)


def identity(x):
    return x

clean = lambda x: cleaning.post_clean(cleaning.simple_clean(x))
nlp = identity


def page_to_dict(page_content):

    # pat = r'((?<=(\=\=)).+?(?=(\=\=)))'
    pat = r'(?<=\=\=)(.+?)(?=\=\=)'
    groups = re.findall(pat, page_content)

    print(groups)

    page_dict = dict()
    for e in groups:
        if not e[0] == '=':
            page_dict[e] = dict()
            _last = e
        else:
            if (e[0] == '=') and (not e[0:2] == '=='):
                page_dict[_last][e] = ""
            else:
                page_dict[_last][e] += page_dict[_last][e]
    
    return page_dict


def get_page_structure(page_content):

    # pat = r'((?<=(\=\=)).+?(?=(\=\=)))'
    pat = r'(?<=\=\=)(.+?)(?=\=\=)'
    groups = re.findall(pat, page_content)
    
    return groups


def parse_page_text(page_content):
    
    structured_page = dict()

    page_splitters = get_page_structure(page_content)

    # print(page_splitters)
    if len(page_splitters) == 0:
        return page_content

    structured_page['_Summary'] = page_content.split('=='+page_splitters[0]+'==')[0]

    def create_fixes(marker):
        try:
            if (marker[0] != '=') and (marker[-1] != '='):
                return '=='+marker+'=='
            elif (marker[0] == '=') and (marker[1] != '='):
                return '=='+marker+'==='
            elif (marker[0:2] == '=='):
                return '=='+marker+'===='
        except IndexError:
            # print('Problem marker:')
            # print(marker)
            # _dbg((page_splitters,page_content))
            pass

        return None
    
    fixed_page_splitters = [create_fixes(x) for x in page_splitters if len(x) > 1]

    # print(fixed_page_splitters)

    for k in range(1, len(fixed_page_splitters)):
        prefix = None
        suffix = None

        prefix = fixed_page_splitters[k-1]
        suffix = fixed_page_splitters[k]

        # result = re.search(prefix+'(.*)'+suffix, page_content)
        # structured_page[page_splitters[k-1]] = result.group(1)

        test_str = page_content
        sub1 = prefix
        sub2 = suffix 

        # getting index of substrings
        idx1 = test_str.index(sub1)
        idx2 = test_str.index(sub2)
        
        # length of substring 1 is added to
        # get string from next character
        res = test_str[idx1 + len(sub1) + 1: idx2]

        structured_page[page_splitters[k-1]] = res
    
    structured_page[fixed_page_splitters[-1]] = page_content.split(fixed_page_splitters[-1])[-1]
    
    return structured_page


def process_article():
    while not (shutdown and aq.empty()):
    
        page_title, source = aq.get()
        
        text = clean(source)
        # doc = nlp(text)

        # sents = []
        # for s in doc.sents:
        #     if len(sents) > 0:
        #         # Fix some spacy sentence splitting errors by joining sentences if they don't end in a period
        #         if len(str(sents[-1]).strip()) and str(sents[-1]).strip()[-1] != ".":
        #             sents[-1] += str(s)
        #             continue
        #     sents.append(str(s))

        # out_text = "\n".join(sents)
        # fq.put(json.dumps({"page": page_title, "sentences":out_text}))
        
        # if page_title == 'Analysis':
        #     print("I know analysis!")
        #     # fq.put(json.dumps({"page": page_title, "content": text, "source": source}))
        #     # https://stackoverflow.com/questions/18337407/saving-utf-8-texts-with-json-dumps-as-utf-8-not-as-a-u-escape-sequence
            
        #     with open('myfile.txt', 'w') as f:
        #         print(parse_page_text(text), file=f)
            
        #     fq.put(json.dumps({"page": page_title, "content": page_to_dict(text)}, ensure_ascii=False).encode('utf8').decode('utf8'))

        fq.put(json.dumps({"page": page_title, \
            'levels': get_page_structure(text),\
            "content": parse_page_text(text)}, \
                ensure_ascii=False).encode('utf8').decode('utf8'))


def write_out():
    while not (shutdown and fq.empty()):
        line = fq.get()
        out_file.write(line+"\n")

def display():
    while True:
        print("Queue sizes: aq={0} fq={1}. Read: {2}".format(
            aq.qsize(), 
            fq.qsize(), 
            reader.status_count))
        time.sleep(5)


if __name__ == "__main__":
    shutdown = False
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-w', "--wiki", help="wiki dump file .xml.bz2")
    # parser.add_argument('-o', "--out", help="final file .txt")
    # args = parser.parse_args()

    class Arg:
        def __init__(self) -> None:
            self.out = 'parsed.txt'
            self.wiki = 'parsome/enwiki.xml.bz2'
    args = Arg()
    
    manager = multiprocessing.Manager()
    fq = manager.Queue(maxsize=2000)
    aq = manager.Queue(maxsize=2000)
    
    # wiki = bz2.BZ2File(args.wiki)
    # wiki = bz2.open(args.wiki, 'r')
    wiki = bz2.open(args.wiki, 'rt', encoding='utf-8')

    if os.path.exists(args.out):
        os.remove(args.out)
    out_file = open(os.path.join(args.out),"w", encoding='utf8')

    # exit(1)
    if os.path.exists('debug.txt'):
        os.remove('debug.txt')

    reader = wiki_reader.WikiReader(lambda ns: ns == 0, aq.put)

    status = threading.Thread(target=display, args=())
    status.start() 

    processes = []
    for _ in range(5):
        process = multiprocessing.Process(target=process_article)
        process.start()

    write_thread = threading.Thread(target=write_out)
    write_thread.start()

    xml.sax.parse(wiki, reader)
    shutdown = True