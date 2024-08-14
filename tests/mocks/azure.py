class MockAzureAccessToken:
    def __init__(self, token):
        self.token = token


class MockDefaultAzureCredential:
    def __init__(self, **kwargs):
        pass

    @staticmethod
    def get_token(scope: str) -> MockAzureAccessToken:
        return MockAzureAccessToken(f'azure_access_token_for_scope_{scope}')


class MockContainerClient:
    def __init__(self, container):
        self.container = container

    def upload_blob(self, name, data, overwrite=True):
        pass

    def delete_blob(self, name):
        pass

    def list_blobs(self, name_starts_with=None):
        return []


class MockBlobClient:
    def __init__(self):
        pass

    def download_blob(self):
        return b'blob_data'

    def upload_blob(self, data, blob_type=None, overwrite=True, max_concurrency=1):
        pass

    def delete_blob(self):
        pass

    def acquire_lease(self):
        pass


class MockBlobLeaseClient:
    def __init__(self, client):
        pass

    def break_lease(self):
        pass

    def delete_blob(self):
        pass


class MockBlobServiceClient:
    def __init__(self, account_url, credential):
        pass

    def get_container_client(self, container):
        return MockContainerClient(container)

    def get_blob_client(self, container, blob):
        return MockBlobClient()


class MockBlob:
    def __init__(self, blob_name):
        self.name = blob_name
