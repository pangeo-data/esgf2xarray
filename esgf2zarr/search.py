"""ESGF API Search Results to Pandas Dataframes
"""

from __future__ import print_function

import warnings
from tqdm.autonotebook import tqdm, trange
from datetime import datetime
import dask
import requests
import pandas as pd
from collections import OrderedDict

# API AT: https://github.com/ESGF/esgf.github.io/wiki/ESGF_Search_REST_API

def _check_doc_for_malformed_id(d):
    source_id = d['source_id'][0]
    expt_id = d['experiment_id'][0]
    if not  f"{source_id}_{expt_id}" in d['id']:
        raise ValueError(f"Dataset id {d['id']} is malformed")

def _maybe_squeze_values(d):
    def _maybe_squeeze(value):
        if isinstance(value, str):
            return value
        try:
            if len(value)==1:
                return value[0]
        except TypeError:
            return(value)
    return {k: _maybe_squeeze(v) for k, v in d.items()}

def _get_request(server, verbose=False, **payload):
    client = requests.session()
    url_keys = []
    url_keys = ["{}={}".format(k, payload[k]) for k in payload]
    url = "{}/?{}".format(server, "&".join(url_keys))
    if verbose:
        print(url)
    r = client.get(url)
    r.raise_for_status()
    resp = r.json()["response"]
    return resp

def _get_page_dataframe(server, expected_size, offset=0,
                        filter_server_url=None, verbose=False,
                        **payload):

    resp = _get_request(server, offset=offset, verbose=verbose, **payload)

    docs = resp["docs"]
    assert len(docs) == expected_size

    all_files = []
    for d in docs:
        try:
            _check_doc_for_malformed_id(d)
        except ValueError:
            continue
        dataset_id = d['dataset_id']
        item = OrderedDict(dataset_id=dataset_id, id=d['id'])
        target_urls = d.pop('url')
        item.update(_maybe_squeze_values(d))
        for f in target_urls:
            access_url, mime_type, service_type = f.split("|")
            if service_type == 'OPENDAP':
                access_url = access_url.replace('.html', '')
            if filter_server_url is None or filter_server_url in access_url:
                item.update({f'{service_type}_url': access_url})
                all_files.append(item)

    # dropping duplicates on checksum removes all identical files
    return pd.DataFrame(all_files).drop_duplicates(subset='checksum')


_get_page_dataframe_d = dask.delayed(_get_page_dataframe)


def _get_csrf_token(server):
    client = requests.session()
    client.get(server)
    if 'csrftoken' in client.cookies:
        # Django 1.6 and up
        csrftoken = client.cookies['csrftoken']
    else:
        # older versions
        csrftoken = client.cookies['csrf']
    return csrftoken


def esgf_search(server="https://esgf-node.llnl.gov/esg-search/search",
                project="CMIP6", page_size=10,
                # this option should not be necessary with local_node=True
                filter_server_url=None, local_node=True,
                verbose=False, format="application%2Fsolr%2Bjson",
                use_csrf=False, delayed=False, **search):


    payload = search
    payload["project"] = project
    payload["type"]= "File"

    if local_node:
        payload["distrib"] = "false"

    if use_csrf:
        payload["csrfmiddlewaretoken"] = _get_csrf_token(server)

    payload["format"] = format

    init_resp = _get_request(server, offset=0, limit=page_size,
                            verbose=verbose, **payload)
    num_found = int(init_resp["numFound"])

    if delayed:
        page_function = _get_page_dataframe_d
    else:
        page_function = _get_page_dataframe

    all_frames = []
    for offset in range(0, num_found, page_size):

        expected_size = (page_size if offset <= (num_found - page_size)
                         else (num_found - offset))
        df_d = page_function(server, expected_size, limit=page_size, offset=offset,
                             verbose=verbose,
                             filter_server_url=filter_server_url,
                             **payload)

        all_frames.append(df_d)

    if delayed:
        all_frames = dask.compute(*all_frames)

    return pd.concat(all_frames)
