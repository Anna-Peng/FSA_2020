from pkg_resources import Requirement, resource_filename
from src.features.Preprocessor import Preprocessor
from src.features.KukdPreprocessor import KukdPreprocessor
import pandas as pd

path = resource_filename(Requirement.parse("foobar-database"),"tests/kukd_E16AW.json")
kukd_preprocessor = KukdPreprocessor(path)

kukd_preprocessor.print_parameters()

kukd_df = kukd_preprocessor.get_dataframe()

def test_dataframe():
    assert isinstance(kukd_df, pd.DataFrame)

def test_dataframe_columns():
    assert list(kukd_df.columns) == ['PlatformBusinessName', 'PlatformBusinessAddress', 'PlatformPostCode', 'PlatformName', 'PlatformFHRSID', 'PlatformRatingValue', 'PlatformRating', 'PlatformTags', 'PlatformDescription', 'ScrapingSearch', 'ScrapingTime']

