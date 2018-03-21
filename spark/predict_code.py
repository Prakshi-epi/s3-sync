def xml2txt(ModelBucket, data):
    import os
    import os.path as op
    import codecs
    import logging
    from lxml import etree
    from xml.etree import ElementTree as ET
    from text_unidecode import unidecode
    from pdb import set_trace
    # """
    # Convert XML to Text format and save it in given output folder

    # Parameters
    # ----------
    # filepath: str
    #     path of a XML file

    # namespaces_dict: dict
    #     XML namespaces dictionary.
    #     Currently using alto format xml namespaces

    #     Example:
    #     >>> from xml.etree import ElementTree as ET
    #     >>> tree = ET.parse('example.xml')
    #     >>> root = tree.getroot()
    #     >>> root.tag
    #     # '{http://www.loc.gov/standards/alto/ns-v3#}alto'

    # output_folder: str
    #     folder path to save txt files converted from xml files.

    # Returns
    # -------
    # None
    # """
#    if not op.exists(output_folder):
#        os.mkdir(output_folder)
#    abs_fname = op.basename(filepath).split(".")[0]
#    logger.info("Converting:  " + abs_fname)
#    data1 = unidecode(data)
    import sys
    reload(sys)
    out_dir = negexdownload(ModelBucket)
    sys.setdefaultencoding('utf8')
    parser = etree.XMLParser(recover=True)
    tree = ET.fromstring(str(data), parser=parser)
    xmlns = tree.tag.split('}')[0].strip('{')
    final =[]
    namespaces_dict = {'alto-1': 'http://schema.ccs-gmbh.com/ALTO',
             'alto-2': 'http://www.loc.gov/standards/alto/ns-v2#',
             'alto-3': 'http://www.loc.gov/standards/alto/ns-v3#'}    
    # if xmlns in namespaces_dict.values():
        # final = []
        #        txt_fpath = op.join(output_folder, abs_fname + ".txt")
        #        with codecs.open(txt_fpath, 'w', encoding='utf-8') as fw:
    for page in tree.iterfind('.//{%s}Page' % xmlns):
            # Add Custom page breaker
        final.append("\n<EPI_PAGEBREAKER>\n\n")
        for lines in page.iterfind('.//{%s}TextLine' % xmlns):
            line_content = []
            for line in lines.findall('{%s}String' % xmlns):
                text = line.attrib.get('CONTENT')
                line_content.append(text)
            final.append(" ".join(line_content))

    final_txt = "\n".join([line for line in final])
    return final_txt


def testing(data):
    """
        Function will read textfile and will create list of sentences. Now. list of sentences
        will be given as input to spacy model and prediction are stored in json format
    :param nlp2: contains Spacy Model
    :param path: Input File
    :param output_path: Output File path

    """
    # fname = data[0]

    # Converting Text file into List of list of words in a sentence
    import re
    import codecs
    import spacy
    from subprocess import call
    import os
    import random
    import pandas as pd
    import glob
    import sys

    raw = ''.join([i if ord(i) < 128 else ' ' for i in data])
    
    # Pattern for splitting text
    pattern = '[.?]\s+[A-Z]'
    regex = re.compile(pattern)

    # Replacing newline with space
    raw = raw.replace('\n', ' ')

    # list of sentences starting index
    sent_index = [sent.start() + 1 for sent in regex.finditer(raw)]
    start = 0
    # Generate list of sentences
    sentence_list = []
    for idx in sent_index:
        sentence_list.append(raw[start:idx])
        start = idx
    sentence_list.append(raw[start:len(raw)])

        
#    out_dir = modeldownload()
    out_dir = "/mnt/tmp/model-final"
    print("Loading from", out_dir)
    nlp2 = spacy.load(out_dir)

        
    pred = []
    prev = 0
    prev_section = None
    # Prediction in batches
    for doc in nlp2.pipe(sentence_list, batch_size=50, n_threads=4):
        # Prediction
        for ent in doc.ents:
            # print type(ent.text)
            temp = ent.text
            temp = temp.strip()
            curr_section = prev_section
            if ent.label_ == u'SectionHeader':
                prev_section = temp
                curr_section = None
            if temp  == u'':
                continue
            else:
                pred.append({'text': temp, 'start': ent.start_char + prev, 'end': ent.end_char + prev,
                             'type': ent.label_, 'section': curr_section})
        prev += len(doc.text)

    # ['text', 'start', 'end', 'type', 'section']
    return pred


def assert_func(ModelBucket, data):
    import sys
    sys.path.append('/mnt/tmp')
    reload(sys)
    from negex.modifiers import Modifier
    from negex.sentence_segmenter import SentenceSegmenter
    from negex.textutils import substringSieve, reMatch, strip_non_ascii
    
    modifier_obj = Modifier()
    segementer_obj = SentenceSegmenter(split_on_newlines=True)
    data = data.decode('utf-8').replace('\r','\n')
    raw = ''.join([i if ord(i) < 128 else '\n' for i in data])
    neg_offsets = modifier_obj.identify_negex(raw)
    sentence_list = segementer_obj.split_sentences_punkt(data)
    sentence_ids = segementer_obj.generate_sentence_ids(sentence_list)
    model = modeldownload(ModelBucket)
    candidates = testing(data)
    final_output = modifier_obj.negex(candidates, neg_offsets, sentence_ids)
    
    # ['text', 'start', 'end', 'type', 'section', 'assertion']
    return final_output

def modeldownload(ModelBucket):
    try:
        import boto3
        import os
        cwd = os.getcwd()
        #    logger.info('****current directory is*:{}'.format(cwd))
        bucket = ModelBucket
        prefix = 'model-final'
        all_keys = []
        testpath = '/mnt/tmp/'
        model_path = '/mnt/tmp/' + prefix
        subdir1 = model_path + '/ner'
        subdir2 = model_path + '/vocab'
        negex_path = testpath + 'negex'
        s3_client=boto3.client('s3')
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = [object['Key'] for object in objects.get('Contents', [])]
        all_keys.extend(keys)
        if not os.path.exists(model_path):
            ret1 = os.mkdir(model_path)
            ret2 = os.mkdir(subdir1)
            ret3 = os.mkdir(subdir2)
        for s3_key in all_keys:
            if not s3_key.endswith("/"):
                s3_client.download_file(bucket, s3_key, testpath + s3_key)
        print "Model download complete"
        return model_path
    except OSError:
        return "/mnt/tmp/model-final"

def negexdownload(ModelBucket):
    import boto3
    import os
    s3_client=boto3.client('s3')
    negex_objects = s3_client.list_objects_v2(Bucket=ModelBucket, Prefix='negex')
    negex_keys = [object['Key'] for object in negex_objects.get('Contents', [])]
    negex_path = '/mnt/tmp/negex'
    if not os.path.exists(negex_path):
        path = os.mkdir(negex_path)
    for s3_key in negex_keys:
        if not s3_key.endswith("/"):
            s3_client.download_file(ModelBucket, s3_key, '/mnt/tmp/' + s3_key)
        print "Model download complete"
    return 'mnt/tmp/negex'


def s3Upload(OutputBucket, predicted_data):
    import json
    import boto3
    client = boto3.client('s3')
    for list1 in predicted_data:
        processed = []
        file_name = list1[0].split('.')[0]
        text = list1[1]
        data = json.dumps(text)
        client.put_object(Body=json.dumps(text), Bucket=OutputBucket, Key=file_name+ '.json', ServerSideEncryption='aws:kms')
    return file_name
