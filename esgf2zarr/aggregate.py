"""Aggregattion functions for building xarray datasets from search results.
"""


from functools import reduce
import xarray as xr
import pandas as pd
import dask
from tqdm.autonotebook import tqdm, trange
from datetime import datetime

def dict_union(*dicts, merge_keys=['history', 'further_info_url'],
               drop_keys=['DODS_EXTRA.Unlimited_Dimension']):
    if len(dicts) > 2:
        return reduce(dict_union, dicts)
    elif len(dicts)==2:
        d1, d2 = dicts
        d = type(d1)()
        # union
        all_keys = set(d1) | set(d2)
        for k in all_keys:
            v1 = d1.get(k)
            v2 = d2.get(k)
            if (v1 is None and v2 is None) or k in drop_keys:
                pass
            elif v1 is None:
                d[k] = v2
            elif v2 is None:
                d[k] = v1
            elif v1==v2:
                d[k] = v1
            elif k in merge_keys:
                d[k] = '\n'.join([v1, v2])
        return d
    elif len(dicts)==1:
        return dicts[0]

def set_bnds_as_coords(ds):
    new_coords_vars = [var for var in ds.data_vars if 'bnds' in var or 'bounds' in var]
    ds = ds.set_coords(new_coords_vars)
    return ds

def fix_climatology_time(ds):
    for dim in ds.dims:
        if 'climatology' in ds[dim].attrs:
            ds = ds.rename({dim: dim + '_climatology'})
    return ds

def set_coords(ds):
    # there should only be one variable per file
    # everything else is coords
    varname = ds.attrs['variable_id']
    coord_vars = set(ds.data_vars) - {varname}
    ds = ds.set_coords(coord_vars)
    ds = fix_climatology_time(ds)
    return(ds)

def open_dataset(url, default_chunk_size='12MiB'):
    # try to use smaller chunks
    with dask.config.set({'array.chunk-size': '12MiB'}):
        ds = xr.open_dataset(url, chunks={'time': 'auto'}, decode_times=False)
    ds.attrs['history'] = f"{datetime.now()} xarray.open_dataset('{url}')"
    ds = set_coords(ds)
    return ds

open_dataset_delayed = dask.delayed(open_dataset)

def concat_timesteps(dsets, timevar='time'):
    if len(dsets)==1:
        return dsets[0]

    attrs = dict_union(*[ds.attrs for ds in dsets])

    # for nd-coordinates without time from first ensemble member to simplify merge
    first = dsets[0]

    def drop_unnecessary_coords(ds):
        ndcoords = set(ds.coords) - set(ds.dims)
        ndcoords_drop = [coord for coord in ndcoords if timevar not in ds[coord].dims]
        return ds.drop(ndcoords_drop)

    rest = [drop_unnecessary_coords(ds) for ds in dsets[1:]]
    objs_to_concat = [first] + rest

    ds = xr.concat(objs_to_concat, dim=timevar, coords='minimal')
    attrs['history'] += f"\n{datetime.now()} xarray.concat(<ALL_TIMESTEPS>, dim='{timevar}', coords='minimal')"
    ds.attrs = attrs
    return ds

def concat_ensembles(member_dsets, member_ids, join='outer'):
    if len(member_dsets)==1:
        return member_dsets[0]
    concat_dim = xr.DataArray(member_ids, dims='member_id', name='member_id')

    # warning: this function broke for the IPSL historical o3 variable because it
    # contained a mix of frequencies (monthly and climatology)
    # this was fixed by adding frequency="mon" to the search

    # merge attributes
    attrs = dict_union(*[ds.attrs for ds in member_dsets])

    # align first to deal with the fact that some ensemble members have different lengths
    # inner join keeps only overlapping segments of each ensemble
    # outer join gives us the longest possible record
    member_dsets_aligned = xr.align(*member_dsets, join=join)

    # keep only coordinates from first ensemble member to simplify merge
    first = member_dsets_aligned[0]
    rest = [mds.reset_coords(drop=True) for mds in member_dsets_aligned[1:]]
    objs_to_concat = [first] + rest

    ds = xr.concat(objs_to_concat, dim=concat_dim, coords='minimal')
    attrs['history'] += f"\n{datetime.now()} xarray.concat(<ALL_MEMBERS>, dim='member_id', coords='minimal')"
    ds.attrs = attrs
    return ds

def merge_vars(ds1, ds2):
    # merge two datasets at a time - designed for recursive merging
    # drop all variables from second that already exist in first's coordinates

    # I can't believe xarray doesn't have a merge that keeps attrs
    attrs = dict_union(ds1.attrs, ds2.attrs)

    # non dimension coords
    # could be skipping over
    ds1_ndcoords = set(ds1.coords) - set(ds1.dims)

    # edge case for variable 'ps', which is a coordinate in some datasets
    # and a data_var in its own dataset
    ds2_dropvars = set(ds2.variables).intersection(ds1_ndcoords)
    ds2_drop = ds2.drop(ds2_dropvars)

    ds = xr.merge([ds1, ds2_drop])
    ds.attrs = attrs
    return ds

def merge_recursive(dsets):
    dsm = reduce(merge_vars, dsets)
    dsm.attrs['history'] += f"\n{datetime.now()} xarray.merge(<ALL_VARIABLES>)"

    # fix further_info_url
    fi_urls = set(dsm.attrs['further_info_url'].split('\n'))
    dsm.attrs['further_info_url'] = '\n'.join(fi_urls)

    # rechunk
    chunks = {'time': 'auto'}
    if 'member_id' in dsm.dims:
        chunks.update({'member_id': 1})
    if 'time_climatology' in dsm.dims:
        chunks.update({'time_climatology': 1})
    return dsm.chunk(chunks)


def combine_files(files):
    """Produce a list of xarray datasets from ESGF search output.

    Parameters
    ----------
    files : pandas.DataFrame
        Output from ``esgf_search``

    Returns
    -------
    all_dsets : dict
        Dictionary of xarray datsets. Keys are dataset IDs.
    """

    # fields which define a single dataset
    dataset_fields = ['institution_id', 'source_id', 'experiment_id', 'table_id', 'grid_label']
    all_dsets = {}
    for dset_keys, dset_files in tqdm(files.groupby(dataset_fields), desc='dataset'):
        dset_id = '.'.join(dset_keys)
        all_member_dsets = []
        all_member_ids = []

        # first build a nested list of delayed datasets
        for var_id, var_files in dset_files.groupby('variable_id'):
            member_dsets = []
            member_ids = []
            for m_id, m_files in var_files.groupby('member_id'):
                member_ids.append(m_id)
                member_dsets.append([open_dataset_delayed(url) for url in m_files.OPENDAP_url])
            all_member_dsets.append(member_dsets)
            all_member_ids.append(member_ids)

        # now compute them all in parallel
        all_member_dsets_c = dask.compute(*all_member_dsets, retries=5)

        # and merge them
        var_dsets = [concat_ensembles([concat_timesteps(time_dsets) for time_dsets in member_dsets],
                                      member_ids)
                 for member_dsets, member_ids in zip(
                     tqdm(all_member_dsets_c, desc='ensemble', leave=False), all_member_ids)]
        ds = merge_recursive(tqdm(var_dsets, desc='variables', leave=False))
        all_dsets[dset_id] = ds
    return all_dsets
