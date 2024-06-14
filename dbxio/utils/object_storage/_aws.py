import re
from typing import Optional

import attrs

from dbxio.utils.object_storage.object_storage import ObjectStorage


@attrs.define
class _S3StorageImpl(ObjectStorage):
    bucket_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    object_key: str = attrs.field(validator=attrs.validators.instance_of(str))
    domain_name: Optional[str] = None

    # scheme: str = attrs.field(default='s3', init=False)
    scheme = 's3'
    # bucket_name/object_key
    url_regex = re.compile(r'^(?P<bucket_name>[^/]+)/(?P<object_key>.*)$')

    def to_url(self) -> str:
        return f'{self.scheme}://{self.bucket_name}/{self.object_key}'
