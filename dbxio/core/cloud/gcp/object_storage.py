import re
from typing import Optional

import attrs

from dbxio.core.cloud.client.object_storage import ObjectStorageClient
from dbxio.core.cloud.gcp import GCP_ClOUD_PROVIDER_NAME


@attrs.define
class _GCStorageClientImpl(ObjectStorageClient):
    bucket_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    object_key: str = attrs.field(validator=attrs.validators.instance_of(str))
    domain_name: Optional[str] = None

    scheme = 'gs'
    cloud_provider_name = GCP_ClOUD_PROVIDER_NAME
    # bucket_name/object_key
    url_regex = re.compile(r'^(?P<bucket_name>[^/]+)/(?P<object_key>.*)$')

    def to_url(self) -> str:
        return f'{self.scheme}://{self.bucket_name}/{self.object_key}'
