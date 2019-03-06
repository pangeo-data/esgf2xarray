from .. import search
import pytest

# This is far from a comprehensive test suite...
# Just some basic checks that things work. Can't validate against specific data
# because data is changing.

columns = \
       ['dataset_id', 'id', 'version', 'activity_drs', 'activity_id',
       'branch_method', 'cf_standard_name', 'checksum', 'checksum_type',
       'citation_url', 'data_node', 'data_specs_version',
       'dataset_id_template_', 'directory_format_template_', 'experiment_id',
       'experiment_title', 'frequency', 'further_info_url', 'grid',
       'grid_label', 'index_node', 'instance_id', 'institution_id', 'latest',
       'master_id', 'member_id', 'mip_era', 'model_cohort',
       'nominal_resolution', 'pid', 'product', 'project', 'realm', 'replica',
       'size', 'source_id', 'source_type', 'sub_experiment_id', 'table_id',
       'timestamp', 'title', 'tracking_id', 'type', 'variable', 'variable_id',
       'variable_long_name', 'variable_units', 'variant_label', '_version_',
       'retracted', '_timestamp', 'score',  'HTTPServer_url', 'OPENDAP_url',
       'GridFTP_url', 'Globus_url']


@pytest.fixture(scope="module")
def query(request):
    # a basic query
    return dict(mip_era='CMIP6', activity_drs='CMIP', variable="ts",
                table_id='Amon', institution_id='NASA-GISS', experiment_id='amip')

@pytest.mark.parametrize('page_size', [10, 15])
def test_esgf_search_page_size(query, page_size):
    res = search.esgf_search(verbose=True, page_size=page_size, **query)
    assert list(res.columns) == columns

@pytest.mark.parametrize('delayed', [True, False])
def test_esgf_search_delayed(query, delayed):
    res = search.esgf_search(verbose=True, delayed=delayed, **query)
    assert list(res.columns) == columns
