import re
from collections import Counter
import gzip
import boto
import warc

from boto.s3.key import Key
from gzipstream import GzipStreamFile
from mrjob.job import MRJob


class CCJob(MRJob):
  def configure_options(self):
    super(CCJob, self).configure_options()
    self.add_passthrough_option('--source',help="Source location of the common crawl data (s3 or file)")
      
  def process_record(self, record):
    """
    Override process_record with your mapper
    """
    raise NotImplementedError('Process record needs to be customized')

  def mapper(self, _, line):
    f = None
    ## If we're on EC2 or running on a Hadoop cluster, pull files via S3
    #if self.options.source in ['s3' ]:
    print 'Downloading ...'
    # Connect to Amazon S3 using anonymous credentials
    conn = boto.connect_s3(anon=True)
    pds = conn.get_bucket('aws-publicdatasets')
    # Start a connection to one of the WARC files
    k = Key(pds, line)
    f = warc.WARCFile(fileobj=GzipStreamFile(k))
    ## If we're local, use files on the local file system
    #else:
    #  print 'Loading local file {}'.format(line)
    #  f = warc.WARCFile(fileobj=gzip.open(line))
    ###
    for i, record in enumerate(f):
      for key, value in self.process_record(record):
        yield key, value
      self.increment_counter('commoncrawl', 'processed_records', 1)

  # TODO: Make the combiner use the reducer by default
  """
  def combiner(self, key, value):
    yield key, sum(value)

  def reducer(self, key, value):
    yield key, sum(value)
  """



def get_tag_count(data, ctr=None):
  """Extract the names and total usage count of all the opening HTML tags in the document"""
  if ctr is None:
    ctr = Counter()
  # Convert the document to lower case as HTML tags are case insensitive
  ctr.update(HTML_TAG_PATTERN.findall(data.lower()))
  return ctr
# Optimization: compile the regular expression once so it's not done each time
# The regular expression looks for (1) a tag name using letters (assumes lowercased input) and numbers
# and (2) allows an optional for a space and then extra parameters, eventually ended by a closing >
HTML_TAG_PATTERN = re.compile('<([a-z0-9]+)[^>]*>')
# Let's check to make sure the tag counter works as expected
assert get_tag_count('<html><a href="..."></a><h1 /><br/><p><p></p></p>') == {'html': 1, 'a': 1, 'p': 2, 'h1': 1, 'br': 1}


class TagCounter(CCJob):
  def process_record(self, record):
    # WARC records have three different types:
    #  ["application/warc-fields", "application/http; msgtype=request", "application/http; msgtype=response"]
    # We're only interested in the HTTP responses
    if record['Content-Type'] == 'application/http; msgtype=response':
      payload = record.payload.read()
      # The HTTP response is defined by a specification: first part is headers (metadata)
      # and then following two CRLFs (newlines) has the data for the response
      headers, body = payload.split('\r\n\r\n', 1)
      if 'Content-Type: text/html' in headers:
        # We avoid creating a new Counter for each page as that's actually quite slow
        tag_count = get_tag_count(body)
        for tag, count in tag_count.items():
          yield tag, count
        self.increment_counter('commoncrawl', 'processed_pages', 1)

if __name__ == '__main__':
  TagCounter.run()
