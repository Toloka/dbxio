class MockAzureAccessToken:
    def __init__(self, token):
        self.token = token


class MockDefaultAzureCredential:
    def __init__(self, **kwargs):
        pass

    @staticmethod
    def get_token(scope: str) -> MockAzureAccessToken:
        return MockAzureAccessToken(f'azure_access_token_for_scope_{scope}')
