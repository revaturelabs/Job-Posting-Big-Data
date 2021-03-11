import cdx_toolkit
url = 'https://www.reddit.com/r/dataisbeautiful/*'
cdx = cdx_toolkit.CDXFetcher(source='cc')
objs = list(cdx.iter(url, from_ts='202002', to='202006', 
                     limit=5, filter='=status:200'))
[o.data for o in objs]