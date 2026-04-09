from imports import *

def load_creds(path):
    creds = {}
    with open(path, 'r') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                creds[key.strip()] = value.strip()
    return creds

creds = load_creds('creds.txt')

AWS_ACCESS_KEY_ID = creds['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = creds['AWS_SECRET_ACCESS_KEY']

class ParameterStoreClient:
    def __init__(self):
        self.session = boto3.session.Session(region_name='eu-central-1', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        self.client = self.session.client(service_name='ssm')

    def get_parameter_value(self, parameter_name):
        try:
            get_parameter_value_response = self.client.get_parameter(
                Name=parameter_name,
                WithDecryption=True
            )

        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
        except Exception as e:
            print("ParameterStore Error: ", e)
        else:
            if 'Parameter' in get_parameter_value_response:
                value = get_parameter_value_response['Parameter']['Value']
                return value

parameters = ParameterStoreClient()
clients = json.loads(parameters.get_parameter_value('/sharepoint_automations/client'))
secrets = json.loads(parameters.get_parameter_value('/sharepoint_automations/secrets'))

SHAREPOINT_CERT_PRIVATE_KEY = secrets.get('private_key')
SHAREPOINT_CERT_THUMBPRINT = secrets.get('thumbprint')
SHAREPOINT_CLIENT_ID = clients.get('client_id')
SHAREPOINT_TENANT_ID = clients.get('tenant_id')


class SharepointClient:
    def __init__(self, root_path):
        self.root = root_path
        self.cert_private_key = SHAREPOINT_CERT_PRIVATE_KEY
        self.cert_thumbprint = SHAREPOINT_CERT_THUMBPRINT
        self.client_id = SHAREPOINT_CLIENT_ID
        self.tenant_id = SHAREPOINT_TENANT_ID

        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"

        self.scopes = [
            "https://razrgroup.sharepoint.com/.default"
            ]
    def init_app(self):
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=self.authority,
            client_credential={
                "thumbprint": self.cert_thumbprint,
                "private_key": self.cert_private_key
            }
        )

    def init_session(self):
        if not hasattr(self, "app"):
            self.init_app()
        result = None
        result = self.app.acquire_token_for_client(scopes=self.scopes)
        if "access_token" in result:
            self.access_token = result["access_token"]
            self.headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json;odata=verbose"
            }
            self.session = requests.Session()
            self.session.headers.update(self.headers)
        else:
            print(result.get("error"))
            raise Exception("Failed to retrieve access token")
    
    
    def fetch_sharepoint_response(self, relative_url):
        sharepoint_url = f"{self.root}/_api/web/GetFileByServerRelativeUrl('{relative_url}')/$value"

        response = self.session.get(sharepoint_url, stream=True, timeout=300)

        return response

    def fetch_sharepoint_excel(self, relative_url, sheet_name):
            
        response = self.fetch_sharepoint_response(relative_url)

        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(response.content)
        bytes_file_obj.seek(0)
        df = pd.read_excel(bytes_file_obj,sheet_name, engine='openpyxl')

        return df

    def fetch_sharepoint_excel_large_files(self, relative_url, sheet_name):
        url = f"{self.root}/_api/web/GetFileByServerRelativeUrl('{relative_url}')/$value"
        response = self.session.get(url, stream=True, timeout=600)

        if response.status_code != 200:
            raise Exception(f"Download failed: {response.status_code} - {response.text}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            for chunk in response.iter_content(chunk_size=5 * 1024 * 1024):  # 5 MB
                tmp_file.write(chunk)
            tmp_file_path = tmp_file.name

        df = pd.read_excel(tmp_file_path, sheet_name=sheet_name, engine='openpyxl')
        os.remove(tmp_file_path)
        
        return df

    def fetch_sharepoint_excel_large_files_v2(self, relative_url, sheet_name):
        url = f"{self.root}/_api/web/GetFileByServerRelativeUrl('{relative_url}')/$value"
        response = self.session.get(url, stream=True, timeout=600)
        
        try:
            response.raise_for_status()
        except Exception as e:
            raise Exception(f"Download failed: {response.status_code} - {response.text}") from e

        # Save the streamed response to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx", mode='wb') as tmp_file:
            for chunk in response.iter_content(chunk_size=10 * 1024 * 1024):  # 10MB chunks
                if chunk:  # Filter out keep-alive chunks
                    tmp_file.write(chunk)
            tmp_file_path = tmp_file.name

        try:
            df = pd.read_excel(tmp_file_path, sheet_name=sheet_name, engine='openpyxl')
        except Exception as e:
            raise Exception(f"Failed to read Excel file from disk: {e}")
        finally:
            os.remove(tmp_file_path)

        return df

    
    def write_sharepoint_excel(self, site_url, library, df, file, folder=None):
        excel_buffer = io.BytesIO()
        excel_writer = pd.ExcelWriter(excel_buffer, engine='openpyxl')
        df.to_excel(excel_writer, index=False)
        excel_writer.close()
        excel_buffer.seek(0)
        excel_content = excel_buffer.read()
        
        if folder is not None:
            sharepoint_url = f"{site_url}/_api/web/GetFolderByServerRelativeUrl('{library}')/Folders/add(url='{folder}')/Files/add(url='{file}',overwrite=true)"
        else:
            sharepoint_url = f"{site_url}/_api/web/GetFolderByServerRelativeUrl('{library}')/Files/add(url='{file}',overwrite=true)"

        response = self.session.post(sharepoint_url, data=excel_content)
        
        if response.status_code == 200:
            return print(f"File '{file}' uploaded successfully.")
        else:
            return print(f"Failed to upload file: {response.status_code}, {response.text}") 

    # def update_sharepoint_excel(self, site_url, library, df, file, sheet_name, start_cell="A1"):
    #     file_url = f"{site_url}/_api/web/GetFileByServerRelativeUrl('{library}/{file}')/$value"
    #     response = self.session.get(file_url)
    #     if response.status_code != 200:
    #         print(f"Failed to download file: {response.status_code}")
    #         return
    
    #     bytes_file_obj = io.BytesIO(response.content)
    #     wb = openpyxl.load_workbook(bytes_file_obj)
    #     ws = wb[sheet_name]
    
    #     start_col = openpyxl.utils.cell.column_index_from_string(re.findall(r"[A-Z]+", start_cell)[0])
    #     start_row = int(re.findall(r"\d+", start_cell)[0])

    #     max_row = ws.max_row
    #     max_col = ws.max_column
    #     for row in ws.iter_rows(min_row=start_row, max_row=max_row,
    #                             min_col=start_col, max_col=max_col):
    #         for cell in row:
    #             cell.value = None
    
    #     for i, row in enumerate(df.values):
    #         for j, val in enumerate(row):
    #             ws.cell(row=start_row + i, column=start_col + j, value=str(val))
    
    #     output = io.BytesIO()
    #     wb.save(output)
    #     output.seek(0)
    
    #     upload_url = f"{site_url}/_api/web/GetFolderByServerRelativeUrl('{library}')/Files/add(url='{file}',overwrite=true)"
    #     upload_response = self.session.post(upload_url, data=output.read())
    
    #     if upload_response.status_code == 200:
    #         pass
    #     else:
    #         print(f"Failed to upload: {upload_response.status_code}, {upload_response.text}")

    def update_sharepoint_excel(self, site_url, library, df, file, sheet_name, start_cell="A1", date_cols=None, number_cols=None):
        file_url = f"{site_url}/_api/web/GetFileByServerRelativeUrl('{library}/{file}')/$value"
        response = self.session.get(file_url)
        if response.status_code != 200:
            print(f"Failed to download file: {response.status_code}")
            return

        bytes_file_obj = io.BytesIO(response.content)
        wb = openpyxl.load_workbook(bytes_file_obj)
        ws = wb[sheet_name]

        start_col = openpyxl.utils.cell.column_index_from_string(re.findall(r"[A-Z]+", start_cell)[0])
        start_row = int(re.findall(r"\d+", start_cell)[0])

        max_row = ws.max_row
        max_col = ws.max_column
        for row in ws.iter_rows(min_row=start_row, max_row=max_row,
                                min_col=start_col, max_col=max_col):
            for cell in row:
                cell.value = None

        header = df.columns.tolist()

        for i, row_vals in enumerate(df.values):
            for j, val in enumerate(row_vals):
                # cell = ws.cell(row=start_row + i, column=start_col + j, value=val)
                if isinstance(val, (datetime, pd.Timestamp)) and val.tzinfo is not None:
                    val = val.replace(tzinfo=None)

                cell = ws.cell(row=start_row + i, column=start_col + j, value=val)
                col_name = header[j]
                
                if col_name == "Days Bucket":
                    cell.number_format = '@'
                elif date_cols and col_name in date_cols and isinstance(val, (datetime, pd.Timestamp)):
                    cell.number_format = 'dd.mm.yyyy'
                elif number_cols and col_name in number_cols and isinstance(val, (int, float)):
                    cell.number_format = '0.00'

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        upload_url = f"{site_url}/_api/web/GetFolderByServerRelativeUrl('{library}')/Files/add(url='{file}',overwrite=true)"
        upload_response = self.session.post(upload_url, data=output.read())

        if upload_response.status_code == 200:
            print(f"File '{file}' updated successfully.")
        else:
            print(f"Failed to upload: {upload_response.status_code}, {upload_response.text}")
