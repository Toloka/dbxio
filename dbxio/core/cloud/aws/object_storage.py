import re
from typing import Optional

import attrs

from dbxio.core.cloud.aws import AWS_ClOUD_PROVIDER_NAME
from dbxio.core.cloud.client.object_storage import ObjectStorageClient


@attrs.define
class _S3StorageClientImpl(ObjectStorageClient):
    bucket_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    object_key: str = attrs.field(validator=attrs.validators.instance_of(str))
    domain_name: Optional[str] = None

    scheme = 's3'
    cloud_provider_name = AWS_ClOUD_PROVIDER_NAME
    # bucket_name/object_key
    url_regex = re.compile(r'^(?P<bucket_name>[^/]+)/(?P<object_key>.*)$')

    def to_url(self) -> str:
        return f'{self.scheme}://{self.bucket_name}/{self.object_key}'
