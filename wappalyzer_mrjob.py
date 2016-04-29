import re
from urlparse import urlparse
import gzip
import boto
import warc
from wad.detection import TIMEOUT, Detector
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from mrjob.job import MRJob

EMAIL_REGEX = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""

class EmailCounter(MRJob):
  def process_record(self, record):
    """ Override process_record with your mapper """
    raise NotImplementedError('Process record needs to be customized')

  def mapper(self, _, line):
    f = None
    #if self.options.runner in ['inline']:
    #  print self.options.runner + "lol"
    #  print 'Loading local file {}'.format(line)
    #  f = warc.WARCFile(fileobj=gzip.open(line))
    #else:
    conn = boto.connect_s3(anon=True)
    pds = conn.get_bucket('aws-publicdatasets')
    k = Key(pds, line)
    f = warc.WARCFile(fileobj=GzipStreamFile(k))

    for i, record in enumerate(f):
      if record['Content-Type'] == 'application/http; msgtype=response':
        payload = record.payload.read()
        headers, body = payload.split('\r\n\r\n', 1)
        data = []
        #data = data + Detector().check_headers(headers)
        data = data + Detector().check_script(body)
        data = data + Detector().check_html(body)
        data = {"tech": data, "url":record.url, "date":record.date, "domain": urlparse(record.url).netloc}
        yield data, 1


if __name__ == '__main__':
  EmailCounter.run()
