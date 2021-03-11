from warcio.archiveiterator import ArchiveIterator

with open('s3a://commoncrawl/crawl-data/CC-MAIN-2020-05/segments/1579250589560.16/warc/CC-MAIN-20200117123339-20200117151339-00000.warc.gz', 'rb') as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            print(record.rec_headers.get_header('WARC-Target-URI'))