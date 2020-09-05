from pkg_resources import Requirement, resource_filename
from src.features.Preprocessor import Preprocessor
from src.features.FbmktplacePreprocessor import FbmktplacePreprocessor
import pandas as pd

path = resource_filename(Requirement.parse("foobar-database"),"tests/fbmktplace_london_homemadefood.json")
fbmkplace_preprocessor = FbmktplacePreprocessor(path)

fbmkplace_preprocessor.print_parameters()

fbmkplace_df = fbmkplace_preprocessor.get_dataframe()

def test_dataframe():
    assert isinstance(fbmkplace_df, pd.DataFrame)

def test_dataframe_columns():
    assert list(fbmkplace_df.columns) == ['PlatformBusinessName', 'PlatformBusinessAddress', 'PlatformPostCode', 'PlatformName', 'PlatformFHRSID', 'PlatformRatingValue', 'PlatformRating', 'PlatformTags', 'PlatformDescription', 'ScrapingSearch', 'ScrapingTime']
