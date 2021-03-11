import extruct
import requests
import pprint
from w3lib.html import get_base_url
from rdflib.plugin import register, Serializer, Parser
register('json-ld', Serializer, 'rdflib_jsonld.serializer', 'JsonLDSerializer')
register('json-ld', Parser, 'rdflib_jsonld.parser', 'JsonLDParser')


pp = pprint.PrettyPrinter(indent=2)
r = requests.get('http://www.randstadusa.com/jobs/search/computer-and-mathematical-occupations/')
base_url = get_base_url(r.text, r.url)
data = extruct.extract(r.text, base_url=base_url)
pp.pprint(data)