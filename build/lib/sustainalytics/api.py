import requests
import pandas as pd
from pandas.io.json import json_normalize
from time import time
from tqdm import tqdm
import numpy as np
import itertools


# pd.set_option('display.max_columns', None)
class API(object):
    """
    API manages connection and collection of Sustainalytics.

    Public Attributes
    -----------------
    client_id : str
        a special ID provided by sustainalytics to client for authentication and authorization
    client_secretkey : str
        a special key provided by sustainalytics to client for authentication and authorization
    access_headers : dict
        a dictionary managing the api tokens
    fieldIds : list
        a list of identifiers i.e. ISINs, CUSIPs, SEDOLs, Entity Ids(Sustainalytics).
    universe_of_access : dataframe/json
        a collection of EntityIds and universe the client can access.
    productIDs : list
        a list of productIds the client can access

    full_definition : dataframe/json
        a collection of the field definitions and more so product, package and cluster information

    Private Attributes
    ------------------
    __universe_entity_ids : list
        a list of entityIds the client can access

    Public Methods
    -----------------
    get_access_headers()
        returns the access and authorization token to the api.

    get_fieldIDs()
        :returns a list of fieldIds

    get_fieldsInfo(dtype=json)
        :returns a collection containing the fields information accessible to clients
    get_fieldDefinitions(dtype=json)
        :returns a collection of field definitions
    get_productIDs()
        :returns a list of product IDs
    get_productsInfo(dtype=json)
        :returns a collection of products information
    get_packageIDs()
        :returns a list of package IDs
    get_packageInfo(dtype=json)
        :returns a collection of package information
    get_fieldClusterIDs()
        :returns a list of field cluster IDs
    get_fieldClusterInfo(dtype=json)
        :returns a collection of field cluster information

    get_fieldMappings(dtype=json)
        :returns a collections of fieldId mappings to their descriptive information

    get_fieldMappingDefinitions(dtype=json)
        :returns a collection of the field mappings definitions.
    get_universe_access(dtype=json)
        :returns a collection of entity ids and universe access of an account
    get_universe_entityIDs(dtype=json)
        :returns a list of entityIds the client can access
    get_fullFieldDefinitions(dtype=json)
        :returns a collection of fieldDefinitions
    get_pdfReportService(dtype=json):
        :returns manages the pdf report generation.
    get_pdfReportInfo(dtype=json)
        :returns a collection of pdf information
    get_pdfReportUrl(identifier=None,reportId=None,dtype=json)
        :returns URL pdf link for an entityId and a reportId

    get_data(dtype=json)
        :returns a collections of sustainalytics data to the client.
    
    Private Methods
    --------------
    __process_fieldsdata(field):
        :returns a processed list of fieldIds
    --
    """

    def __init__(self, client_id, client_secretkey):
        """
        Initialize connection with the API with client id and client_secretkey
        :param client_id:
        :param client_secretkey:
        """
        self.client_id = client_id
        self.client_secretkey = client_secretkey
        self.access_headers = self.get_access_headers()
        self.fieldIds = None
        self.universe_of_access = None
        self.productIDs = self.get_productIDs()
        # print(self.universe_of_access)
        #print(self.universe_of_access)
        self.__universe_entity_ids = None
        # full definition
        self.full_definition = self.get_fullFieldDefinitions(dtype='dataframe')

    def get_access_headers(self):
        """
        Get token from the system
        :return: access token
        """
        try:
            access_token_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
            }

            access_token_data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secretkey
            }

            access_token = requests.post('https://api.sustainalytics.com/auth/token', headers=access_token_headers,
                                         data=access_token_data,
                                         ).json()['access_token']

            access_headers = {
                'Accept': 'text/json',
                'Authorization': str('Bearer ' + access_token)}
            return access_headers
        except:
            raise ConnectionError('API Access Error: Please ensure the client_id and secret_key are valid else reach-out to your account manager for support')

    def get_fieldIDs(self):
        """
        Returns a list of field ids activated for the the client
        :return: a list
        """

        temp_data = self.get_fieldDefinitions(dtype='dataframe')

        if len(temp_data) > 0:
            return temp_data['fieldId'].tolist()
        else:
            return []

    def get_fieldsInfo(self, dtype='json'):
        """
        Returns a fieldidInfo activated for the the client
        :return: a list
        """
        temp_data = self.get_fieldDefinitions(dtype='dataframe')

        if len(temp_data) > 0:
            if dtype == 'json':
                return pd.Series(temp_data['fieldName'].values, index=temp_data['fieldId']).to_dict()
            else:
                return temp_data[['fieldId', 'fieldName']]
        else:
            return {}

    def get_fieldDefinitions(self, dtype='json'):
        """
        Returns the field definitions either as a dataframe or json
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        try:

            if dtype == 'json':
                temp_data = requests.get('https://api.sustainalytics.com/v1/FieldDefinitions',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/FieldDefinitions',
                                                      headers=self.access_headers, timeout=60).json())

        except:
            self.access_headers = self.get_access_headers()
            if dtype == 'json':

                temp_data = requests.get('https://api.sustainalytics.com/v1/FieldDefinitions',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/FieldDefinitions',
                                                      headers=self.access_headers, timeout=60).json())
        return temp_data

    def get_productIDs(self):
        """
        Returns a list of product ids activated for the the client
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        temp_data = self.get_fieldMappings(dtype='dataframe')

        if len(temp_data) > 0:
            return temp_data['productId'].tolist()
        else:
            return []

    def get_productsInfo(self, dtype='json'):
        """
        Returns products info for the clients
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        temp_data = self.get_fieldMappings(dtype='dataframe')

        if len(temp_data) > 0:
            if dtype == 'json':
                return pd.Series(temp_data['productName'].values, index=temp_data['productId']).to_dict()
            else:
                return temp_data[['productId', 'productName']]
        else:
            return {}

    def get_packageIds(self):
        """
        Returns a list of package ids activated for the the client
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        temp_data = self.get_fieldMappings(dtype='dataframe')
        temp_data = pd.DataFrame(list(itertools.chain.from_iterable(temp_data['packages'].tolist())))

        if len(temp_data) > 0:
            return temp_data['packageId'].tolist()
        else:
            return []

    def get_packageInfo(self, dtype='json'):
        """
        Returns products info for the clients
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        temp_data = self.get_fieldMappings(dtype='dataframe')
        temp_data = pd.DataFrame(list(itertools.chain.from_iterable(temp_data['packages'].tolist())))

        if len(temp_data) > 0:
            if dtype == 'json':
                return pd.Series(temp_data['packageName'].values, index=temp_data['packageId']).to_dict()
            else:
                return temp_data[['packageId', 'packageName']]
        else:
            return {}

    def get_fieldClusterIds(self):
        """
        Returns a list of fieldcluster ids activated for the the client
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        temp_data = self.get_fieldMappings(dtype='dataframe')
        temp_data = pd.DataFrame(list(itertools.chain.from_iterable(temp_data['packages'].tolist())))
        temp_data = pd.DataFrame(list(itertools.chain.from_iterable(temp_data['clusters'].tolist())))

        if len(temp_data) > 0:
            return temp_data['fieldClusterId'].tolist()
        else:
            return []

    def get_fieldClusterInfo(self, dtype='json'):
        """
        Returns fieldcluster info for the clients
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        temp_data = self.get_fieldMappings(dtype='dataframe')
        temp_data = pd.DataFrame(list(itertools.chain.from_iterable(temp_data['packages'].tolist())))
        temp_data = pd.DataFrame(list(itertools.chain.from_iterable(temp_data['clusters'].tolist())))

        if len(temp_data) > 0:
            if dtype == 'json':
                return pd.Series(temp_data['fieldClusterName'].values, index=temp_data['fieldClusterId']).to_dict()
            else:
                return temp_data[['fieldClusterId', 'fieldClusterName']]
        else:
            return {}

    def get_fieldMappings(self, dtype='json'):
        """
        Returns the field definitions either as a dataframe or json
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        try:

            if dtype == 'json':
                temp_data = requests.get('https://api.sustainalytics.com/v1/FieldMappings',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = json_normalize(requests.get('https://api.sustainalytics.com/v1/FieldMappings',
                                                        headers=self.access_headers, timeout=60).json())
                # primary_meta_cols = temp_data.columns.tolist().remove('packages')
                # temp_data = json_normalize(temp_data, record_path='packages', meta=primary_meta_cols)

        except:
            self.access_headers = self.get_access_headers()
            if dtype == 'json':

                temp_data = requests.get('https://api.sustainalytics.com/v1/FieldMappings',
                                         headers=self.access_headers, timeout=60).json()

            else:
                temp_data = json_normalize(
                    requests.get('https://api.sustainalytics.com/v1/FieldMappings', headers=self.access_headers,
                                 timeout=60).json())

                # JSON DENORMALIZATION
                # primary_meta_cols = temp_data.columns.tolist().remove('packages')
                # temp_data2 = json_normalize(temp_data,record_path='packages',meta=primary_meta_cols)
        return temp_data

    def get_fieldMappingDefinitions(self, dtype='json'):
        """
        Returns the field definitions either as a dataframe or json
        :param dtype: dataframe or json
        :return: requested Data formats
        """

        try:

            if dtype == 'json':
                temp_data = requests.get('https://api.sustainalytics.com/v1/FieldMappingDefinitions',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/FieldMappingDefinitions',
                                                      headers=self.access_headers, timeout=60).json())

        except:
            self.access_headers = self.get_access_headers()
            if dtype == 'json':

                temp_data = requests.get('https://api.sustainalytics.com/v1/FieldMappingDefinitions',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/FieldMappingDefinitions',
                                                      headers=self.access_headers, timeout=60).json())
        return temp_data

    def get_universe_access(self, dtype='json'):
        """
        Get all the companyids in the universes
        :param dtype: return type dataframe or json
        :return: json or dataframe
        """
        try:

            if dtype == 'json':
                temp_data = requests.get('https://api.sustainalytics.com/v1/UniverseOfAccess',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/UniverseOfAccess',
                                                      headers=self.access_headers, timeout=60).json())

        except:
            self.access_headers = self.get_access_headers()
            if dtype == 'json':

                temp_data = requests.get('https://api.sustainalytics.com/v1/UniverseOfAccess',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/UniverseOfAccess',
                                                      headers=self.access_headers, timeout=60).json())

        return temp_data

    def get_universe_entityIDs(self, keep_duplicates=False):
        """
        Returns a list of entityids in the Universe of Access for the client
        :return: list of entity ids
        """
        self.universe_of_access = self.get_universe_access(dtype='dataframe')
        self.__universe_entity_ids = list(itertools.chain.from_iterable(self.universe_of_access['entityIds'].tolist()))
        if keep_duplicates is True:
            return self.__universe_entity_ids
        else:
            return list(set(self.__universe_entity_ids))

    def __process_fieldsdata(self, field):
        """
        Return a processed dataframe
        :return: new_dataframe
        """
        if not bool(field) or field is np.nan:
            self.fieldIds = self.get_fieldIDs()
            fieldstr = [str(i) for i in self.fieldIds]
            self.fieldIds_default = dict.fromkeys(fieldstr, np.nan)
            return self.fieldIds_default
        else:
            return field

    def __process_definitions(self,value,src_df,match_length,src_id_name):
        """
        Process the definition file
        :param value: definition id
        :param src_df: dataframe file to lookup
        :param match_length: 2, 4 ,6
        :param src_id_name: name of definition id
        :return: id, idname
        """
        value_str = str(value)[:match_length]
        #print(src_df.columns)
        temp_df = src_df[src_df[src_id_name]==int(value_str)].copy()
        if len(temp_df)>=1:
            return temp_df.iat[0,0],temp_df.iat[0,1]
        else:
            return None,None

    def get_fullFieldDefinitions(self,dtype='json'):
        """
        Return all definitions
        :return: full dataframe of the definition mapping
        """
        field_info= self.get_fieldsInfo(dtype='dataframe')
        field_cluster = self.get_fieldClusterInfo(dtype='dataframe')
        packages = self.get_packageInfo(dtype='dataframe')
        products = self.get_productsInfo(dtype='dataframe')
        #Go up the ladder
        field_info['productId'],field_info['productName']= zip(*field_info.apply(lambda x:self.__process_definitions(x['fieldId'],products,2,'productId'),axis=1))
        field_info['packageId'], field_info['packageName'] = zip(*field_info.apply(lambda x: self.__process_definitions(x['fieldId'], packages, 4, 'packageId'), axis=1))
        field_info['fieldClusterId'], field_info['fieldClusterName'] = zip(*field_info.apply(lambda x: self.__process_definitions(x['fieldId'], field_cluster, 6, 'fieldClusterId'), axis=1))
        if dtype=='json':
            return field_info.to_json(orient='records')
        else:
            return field_info

    def get_pdfReportService(self, dtype='json'):
        """
        Get the PDF reports
        :return: info
        """
        try:

            if dtype == 'json':
                temp_data = requests.get('https://api.sustainalytics.com/v1/ReportService',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/ReportService',
                                                      headers=self.access_headers, timeout=60).json())

        except:
            self.access_headers = self.get_access_headers()
            if dtype == 'json':

                temp_data = requests.get('https://api.sustainalytics.com/v1/ReportService',
                                         headers=self.access_headers, timeout=60).json()
            else:
                temp_data = pd.DataFrame(requests.get('https://api.sustainalytics.com/v1/ReportService',
                                                      headers=self.access_headers, timeout=60).json())
        return temp_data

    def get_pdfReportUrl(self, identifier=None, reportId=None, dtype='json'):
        """
        Returns the URL of the PDF report
        :param identifier: Sustainalytics Entity identifier
        :param reportId: report ID
        :return: json
        """
        temp_data = pd.DataFrame()
        request_url = 'https://api.sustainalytics.com/v1/ReportService/url/'
        if identifier is not None and reportId is not None:
            request_url = request_url + str(identifier).strip(' \t\n') + "/" + str(reportId).strip(' \t\n')
            try:

                if dtype == 'json':
                    temp_data = requests.get(request_url,
                                             headers=self.access_headers, timeout=60).json()
                else:
                    temp_data = pd.DataFrame(requests.get(request_url,
                                                          headers=self.access_headers, timeout=60).json())

            except:
                self.access_headers = self.get_access_headers()
                if dtype == 'json':

                    temp_data = requests.get(request_url,
                                             headers=self.access_headers, timeout=60).json()
                else:
                    temp_data = pd.DataFrame(requests.get(request_url,
                                                          headers=self.access_headers, timeout=60).json())

            return temp_data
        else:
            return temp_data

    def get_pdfReportInfo(self, dtype='json'):
        """
        Returns a json of report IDs accessible to client
        :return: json or dataframe
        """
        temp_data = self.get_pdfReportService(dtype='dataframe')
        temp_data = pd.DataFrame(list(itertools.chain.from_iterable(temp_data['reports'].tolist())))

        if len(temp_data) > 0:
            if dtype == 'json':
                return pd.Series(temp_data['reportType'].values, index=temp_data['reportId']).to_dict()
            else:
                return temp_data[['reportId', 'reportType']].drop_duplicates()
        else:
            return {'Message':'Client has no pdf report access'}

    def get_data(self, identifiers, productIds=None, packageIds=None, fieldClusterIds=None, dtype='json', fieldIds=None,
                 chunk=50):
        """
        Get bulk data via sustainalytics API
        :return: json or Dataframe
        """
        data_pull_dt = pd.DataFrame()
        data_pull_json = []
        cntr = 0
        chunk_size = chunk
        assert chunk_size <= 100, "Chunk size should be less than or equal to 100."


        if len(identifiers) > 99:
            identiers_group_list = [identifiers[i:i + chunk_size] for i in
                                    range(0, len(identifiers), chunk_size)]
        else:
            identiers_group_list = [identifiers]

        with tqdm(total=len(identiers_group_list)) as pbar:

            for i, ids100 in enumerate(identiers_group_list):
                start = time()
                new_identifiers = ','.join([str(elem).strip() for elem in ids100])
                params = (('identifiers', new_identifiers),)
                # ADD THE PRODUCT ID
                if productIds is not None and isinstance(productIds, list) and len(productIds) > 0:
                    productIds_str = ','.join([str(elem) for elem in productIds])

                    params = params + (('productIds', productIds_str),)


                # For packages ids
                elif packageIds is not None and isinstance(packageIds, list) and len(packageIds) > 0:
                    params = params + (('packageIds', packageIds),)

                elif fieldClusterIds is not None and isinstance(fieldClusterIds, list) and len(fieldClusterIds) > 0:
                    fieldClusterIds_str = ','.join([str(elem) for elem in fieldClusterIds])
                    params = params + (('fieldClusterIds', fieldClusterIds_str),)
                else:
                    pass
                    # params = params + (('fieldIds', fieldIDlist_str),)

                # Prepare the cases of long fieldIds
                # gET IN BATCHES OR in BULK
                #CASE 1
                #if fieldClusterIds is None and productIds is None and packageIds is None and fieldIds is None:
                #CASE 2
                # if fieldClusterIds is None and productIds is None and packageIds is None and fieldIds is None:
                try:
                    # Managing Dataframes
                    requests_url = requests.get('https://api.sustainalytics.com/v1/DataService',
                                                headers=self.access_headers, params=params, timeout=180).json()
                    if dtype == 'json':
                        temp_data = requests_url

                    else:
                        temp_data = pd.DataFrame(requests_url)
                        # all_field_keys = set().union(*requests_url['fields'])
                except:
                    self.access_headers = self.get_access_headers()
                    requests_url = requests.get('https://api.sustainalytics.com/v1/DataService',
                                                headers=self.access_headers, params=params, timeout=180).json()

                    if dtype == 'json':
                        temp_data = requests_url
                        # data_pull_json = data_pull_json+temp_data
                    else:
                        # all_field_keys = set().union(*requests_url['fields'])
                        temp_data = pd.DataFrame(requests_url)

                # print(requests_url)



                if dtype == 'json':
                    data_pull_json = data_pull_json + temp_data
                    # print(data_pull_json)

                else:

                    # temp_data['fields'] = temp_data['fields'].fillna('{}')
                    temp_data['fields'] = temp_data['fields'].apply(self.__process_fieldsdata)

                    temp_fields = pd.DataFrame.from_records(temp_data['fields'])
                    temp_fields['identifier'] = temp_data['identifier']
                    temp_fields = temp_fields.set_index('identifier')
                    temp_data = temp_data.set_index('identifier')
                    temp_fields = temp_data.join(temp_fields)
                    data_pull_dt = data_pull_dt.append(temp_fields, sort=False)

                    # data_pull_dt = data_pull_dt.append(temp_data, sort=False)

                pbar.update(1)

            end = time()
            # print(end-start)

        # print(type(data_pull_json))
        if dtype == 'json':

            return data_pull_json
        else:
            # data_pull_dt.drop_duplicates(inplace=True)
            return data_pull_dt


