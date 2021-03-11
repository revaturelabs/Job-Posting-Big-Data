import cdx_toolkit
cdx = cdx_toolkit.CDXFetcher(source='cc')
objs = list(cdx.iter('careers.upwork.com/jobs/search*',
                     from_ts='202001', to='202001',
                     filter=['status:200']))

with open('test.html', 'wb') as f:
    f.write(objs[0].content)