from pkg_resources import Requirement, resource_filename
from src.features.Preprocessor import Preprocessor
from src.features.DeliverooPreprocessor import DeliverooPreprocessor
import pandas as pd

path = resource_filename(Requirement.parse("foobar-database"),"tests/deliveroo_E16AW.json")
deliveroo_preprocessor = DeliverooPreprocessor(path)

deliveroo_df = deliveroo_preprocessor.get_dataframe()

def test_dataframe():
    assert isinstance(deliveroo_df, pd.DataFrame)

def test_dataframe_columns():
    assert list(deliveroo_df.columns) == ['PlatformBusinessName', 'PlatformBusinessAddress', 'PlatformPostCode', 'PlatformName', 'PlatformFHRSID', 'PlatformRatingValue', 'PlatformRating', 'PlatformTags', 'PlatformDescription', 'ScrapingSearch', 'ScrapingTime']
