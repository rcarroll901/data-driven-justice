from boxsdk import Client, OAuth2, CCGAuth
import os
import io
import pandas as pd

def _get_box_client():
    """Get authenticated Box client to interacting with Box SDK

    Requires 
        * Creating App within box with OAuth2 authentication enabled
        * .env file with BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_DEV_TOKEN
    """

    auth = OAuth2(
        client_id=os.environ['BOX_CLIENT_ID'],
        client_secret=os.environ['BOX_CLIENT_SECRET'],
        access_token=os.environ['BOX_DEV_TOKEN']
    )
    return Client(auth)

def download_file_from_box(file_id, download_path):
    """Downloads file from Box to local machine

    Args:
        file_id (str): numerical string which can be found in the file's URL in Box
            Example: Navigate to file you want to download in browser. The URL
            will be of the form: "https://app.box.com/file/12345678910111" which means
            the ID will be "12345678910111".
        download_path (str): path on local machine to download file

    Requires:
        * Creating App within box with OAuth2 authentication enabled
        * .env file with BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_DEV_TOKEN
    """
    client = _get_box_client()
    with open(download_path, 'wb') as f:
        client.file(file_id).download_to(f)

def read_box_excel_to_df(file_id):
    """Reads Excel file from Box and returns dataframe (with no need for 
    local cache)

    Args:
        file_id (str): numerical string which can be found in the file's URL in Box
            Example: Navigate to file you want to download in browser. The URL
            will be of the form: "https://app.box.com/file/12345678910111" which means
            the ID will be "12345678910111".

    Returns:
        pd.Dataframe: dataframe with content from Box Excel file
        
    """
    client = _get_box_client()
    s = client.file(file_id).content()
    return pd.read_excel(s)

if __name__ == '__main__':
    # download_file_from_box(
    #     file_id='1476681059130',
    #     download_path='test.xlsx'
    # )
    print(read_box_excel_to_df(file_id='1476681059130'))
