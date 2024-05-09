def test_compile_storage_account_url():
    from dbxio.utils.blobs import compile_storage_account_url

    storage_name = 'storage_name'
    assert compile_storage_account_url(storage_name) == f'https://{storage_name}.blob.core.windows.net'


def test_get_blob_servie_client():
    from azure.core.credentials import AzureNamedKeyCredential
    from azure.storage.blob._shared.authentication import SharedKeyCredentialPolicy

    from dbxio.utils.blobs import get_blob_servie_client

    storage_name = 'storage_name'
    az_cred_provider = AzureNamedKeyCredential('account_name', 'account_key')
    client = get_blob_servie_client(storage_name, az_cred_provider)

    assert client.account_name == 'account_name'
    assert isinstance(client.credential, SharedKeyCredentialPolicy)
