import extruct
import requests
import pprint
from w3lib.html import get_base_url
from rdflib.plugin import register, Serializer, Parser
register('json-ld', Serializer, 'rdflib_jsonld.serializer', 'JsonLDSerializer')
register('json-ld', Parser, 'rdflib_jsonld.parser', 'JsonLDParser')
import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='cc')
objs = list(cdx.iter('http://www.randstadusa.com/jobs/*',
                     from_ts='202006', to='202103',
                     filter=['status:200']))

with open('test.html', 'wb') as f:
    f.write(objs[0].content)

pp = pprint.PrettyPrinter(indent=2)
r = requests.get('http://www.randstadusa.com/jobs/search/4/824761/java-app-developer-312021em_malvern/')
base_url = get_base_url(r.text, r.url)
data = extruct.extract(r.text, base_url, syntaxes=['microdata', 'opengraph', 'rdfa'])
[data for data in extruct.extract(objs[0].content)['json-ld'] if data['@type'] == 'JobPosting']
pp.pprint(data)
