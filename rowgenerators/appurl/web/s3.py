# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from rowgenerators.appurl.web.web import WebUrl
from rowgenerators import parse_app_url

class S3Url(WebUrl):
    """Convert an S3 proto url into the public access form"""

    def __init__(self, url=None, downloader=None, **kwargs):
        # Save for auth_url()
        self._orig_url = url
        self._orig_kwargs = dict(kwargs.items())

        kwargs['proto'] = 's3'
        super().__init__(url,downloader=downloader, **kwargs)

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 's3';

    @property
    def auth_resource_url(self):
        """Return the orginal S3: version of the url, with a resource_url format that will trigger boto auth"""
        return 's3://{bucket}/{key}'.format(bucket=self.bucket_name, key=self.key)


    @property
    def resource_url(self):
        url_template = 'https://s3.amazonaws.com/{bucket}/{key}'
        return url_template.format(bucket=self.bucket_name, key=self.key)

    @property
    def resource_file(self):

        from rowgenerators.appurl import parse_app_url
        from rowgenerators.appurl.util import file_ext
        from os.path import basename, join, dirname

        return basename(self.resource_url)

    @property
    def resource_format(self):

        from rowgenerators.appurl import parse_app_url
        from rowgenerators.appurl.util import file_ext
        from os.path import basename, join, dirname

        if self._resource_format:
            return self._resource_format
        else:
            return file_ext(self.resource_file)

    def join_dir(self, s):

        from rowgenerators.appurl import parse_app_url
        from rowgenerators.appurl.util import file_ext
        from os.path import basename, join, dirname
        import pathlib

        try:
            path = s.path
        except AttributeError:
            path = parse_app_url(s).path

        # If there is a netloc, it's an absolute URL
        if s.netloc:
            return s

        new_key = str(pathlib.PurePosixPath(dirname(self.key)).joinpath(path))

        return parse_app_url('s3://{bucket}/{key}'.format(bucket=self.bucket_name.strip('/'), key=new_key.lstrip('/')))

    @property
    def bucket_name(self):
        return self.netloc

    @property
    def key(self):
        """S3 storage key, the file path"""
        return '' if not self.path else self.path.strip('/')

    @property
    def object(self):
        """Return the boto object for this source"""
        import boto3

        s3 = boto3.resource('s3')

        return s3.Object(self.bucket_name, self.key)

    def list(self):
        """List the top 'directory' of a S3 URL. Does not list recursively.  """
        import boto3
        import yaml
        client = boto3.client('s3')

        paginator = client.get_paginator('list_objects')

        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=self.path.lstrip('/'), Delimiter='/'):

            if not result:
                continue

            #print(yaml.safe_dump(result))

            # Contents, Name, Prefix, Delimiter, CommonPrefixes
            for e in result.get('Contents',[]):
                if e:
                    if e.get('Key') == result.get('Prefix'):
                        # The requext was for a single file, not a prefix
                        yield parse_app_url("s3://" + self.bucket_name +result.get('Delimiter') + e.get('Key'))
                    else:
                        yield parse_app_url("s3://"+self.bucket_name+result.get('Prefix')+result.get('Delimiter')+e.get('Key'))

            for e in result.get('CommonPrefixes',[]):
                if e:
                    yield parse_app_url("s3://" + self.bucket_name +result.get('Delimiter') + e.get('Prefix'))

    def list_recursive(self):

        for e in self.list():

            if e.path.endswith('/'):
                yield from e.list_recursive()
            else:
                yield e


    @property
    def signed_resource_url(self):
        import boto3

        s3 = boto3.client('s3')

        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': self.key
            }
        )

        return url

