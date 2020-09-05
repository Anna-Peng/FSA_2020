from src.data.FHRSWrapper import FHRSWrapper
from os import path, makedirs
import json

wrapper = FHRSWrapper()

def test_get_businesstypes():
    decoded_response = wrapper.get_businesstypes()
    assert isinstance(decoded_response, dict)

def test_get_establishments_id():
    fhrsid=1088584
    decoded_response = wrapper.get_establishments_id(fhrsid)
    assert isinstance(decoded_response, dict)

def test_get_establishments():
    name='Pizza Haven'
    address='E70NQ'
    decoded_response = wrapper.get_establishments(name=name, address=address)
    assert isinstance(decoded_response, dict)

def test_get_outward_postcode():
    postcode='E70NQ'
    decoded_response = wrapper.get_outward_postcode(postcode)
    assert isinstance(decoded_response, dict)
