#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import os
import json
import xml.etree.ElementTree as ET
from collections import OrderedDict

"""Converts CBERS-4 scene metadata (xml) to stac item"""

def get_keys_from_cbers(cbers_metadata):
    """
    Input:
    cbers_metadata(sting): CBERS metadata file location
    Output:
    dict: Dictionary with stac relevant information
    """

    nsp = {'x': 'http://www.gisplan.com.br/xmlsat'}
    metadata = dict()

    #match = re.match(r'.*_(?P<camera>\w+)_(?P<ymd>\d{8})_.*', cbers_metadata)
    #assert match
    #ymd = match.group('ymd')

    tree = ET.parse(cbers_metadata)
    root = tree.getroot()

    # satellite node information
    satellite = root.find('x:satellite', nsp)
    metadata['sensor'] = satellite.find('x:instrument', nsp).text
    metadata['number'] = satellite.find('x:number', nsp).text

    # image node information
    image = root.find('x:image', nsp)
    metadata['path'] = image.find('x:path', nsp).text
    metadata['row'] = image.find('x:row', nsp).text
    metadata['processing_level'] = image.find('x:level', nsp).text
    metadata['vertical_pixel_size'] = image.find('x:verticalPixelSize', nsp).text
    metadata['horizontal_pixel_size'] = image.find('x:horizontalPixelSize', nsp).text
    
    imagedata = image.find('x:imageData', nsp)
    metadata['ul_lat'] = imagedata.find('x:UL', nsp).find('x:latitude', nsp).text
    metadata['ul_lon'] = imagedata.find('x:UL', nsp).find('x:longitude', nsp).text
    metadata['ur_lat'] = imagedata.find('x:UR', nsp).find('x:latitude', nsp).text
    metadata['ur_lon'] = imagedata.find('x:UR', nsp).find('x:longitude', nsp).text
    metadata['lr_lat'] = imagedata.find('x:LR', nsp).find('x:latitude', nsp).text
    metadata['lr_lon'] = imagedata.find('x:LR', nsp).find('x:longitude', nsp).text
    metadata['ll_lat'] = imagedata.find('x:LL', nsp).find('x:latitude', nsp).text
    metadata['ll_lon'] = imagedata.find('x:LL', nsp).find('x:longitude', nsp).text

    boundingbox = image.find('x:boundingBox', nsp)
    metadata['bb_ul_lat'] = boundingbox.find('x:UL', nsp).find('x:latitude', nsp).text
    metadata['bb_ul_lon'] = boundingbox.find('x:UL', nsp).find('x:longitude', nsp).text
    metadata['bb_ur_lat'] = boundingbox.find('x:UR', nsp).find('x:latitude', nsp).text
    metadata['bb_ur_lon'] = boundingbox.find('x:UR', nsp).find('x:longitude', nsp).text
    metadata['bb_lr_lat'] = boundingbox.find('x:LR', nsp).find('x:latitude', nsp).text
    metadata['bb_lr_lon'] = boundingbox.find('x:LR', nsp).find('x:longitude', nsp).text
    metadata['bb_ll_lat'] = boundingbox.find('x:LL', nsp).find('x:latitude', nsp).text
    metadata['bb_ll_lon'] = boundingbox.find('x:LL', nsp).find('x:longitude', nsp).text

    sun_position = image.find('x:sunPosition', nsp)
    metadata['sun_elevation'] = sun_position.find('x:elevation', nsp).text
    metadata['sun_azimuth'] = sun_position.find('x:sunAzimuth', nsp).text

    # attitude node information
    attitudes = image.find('x:attitudes', nsp)
    for attitude in attitudes.findall('x:attitude', nsp):
        metadata['roll'] = attitude.find('x:roll', nsp).text
        break
    
    # availableBands node information
    available_bands = root.find('x:availableBands', nsp)
    for band in available_bands.findall('x:band', nsp):
        key = 'band_%s_gain' % (band.text)
        metadata[key] = band.attrib['gain']

    # viewing node information
    viewing = root.find('x:viewing', nsp)
    metadata['acquisition_date'] = viewing.find('x:center', nsp).text.replace('T', ' ')
    metadata['acquisition_day'] = metadata['acquisition_date'].split(' ')[0]

    # derived fields
    # example: CBERS4/MUX/071/092/CBERS_4_MUX_20171105_071_092_L2
    metadata['download_url'] = 'CBERS%s/%s/%03d/%03d/%s' % (metadata['number'],
                                                            metadata['sensor'],
                                                            int(metadata['path']),
                                                            int(metadata['row']),
                                                            re.sub(r'_BAND\d+.xml', '',
                                                                   os.path.basename(cbers_metadata)))

    #for band in range(reference_band, reference_band + number_of_bands):
    #    key = 'band_%d_download_url' % (band)
    #    metadata[key] = metadata['download_url'] + '/' + \
    #                    'CBERS_%s_%s_%s_%03d_%03d_L%s_BAND%d.tif' % (metadata['number'],
    #                                                                 metadata['sensor'],
    #                                                                 ymd,
    #                                                                 int(metadata['path']),
    #                                                                 int(metadata['row']),
    #                                                                 metadata['processing_level'],
    #                                                                 band)

    # processing related fields
    # metadata['metadata_processing_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return metadata

def build_stac_item_keys(cbers):
    """
    Builds a STAC item dict based on CBERS metadata

    Input:
    cbers(dict): CBERS metadata
    """

    stac_item = OrderedDict()

    stac_item['id'] = 'CBERS_%s_%s_%s_' \
                      '%03d_%03d_L%s' % (cbers['number'],
                                         cbers['sensor'],
                                         cbers['acquisition_day'].replace('-', ''),
                                         int(cbers['path']),
                                         int(cbers['row']),
                                         cbers['processing_level'])
    stac_item['type'] = 'Feature'
    # Order is lower left lat, lon; upper right lat, lon 
    stac_item['bbox'] = (float(cbers['bb_ll_lat']), float(cbers['bb_ll_lon']),
                         float(cbers['bb_ur_lat']), float(cbers['bb_ur_lon']))

    stac_item['geometry'] = OrderedDict()
    stac_item['geometry']['type'] = 'MultiPolygon'
    stac_item['geometry']['coordinates'] = [[[(float(cbers['ll_lat']),
                                               float(cbers['ll_lon'])),
                                              (float(cbers['lr_lat']),
                                               float(cbers['lr_lon'])),
                                              (float(cbers['ur_lat']),
                                               float(cbers['ur_lon'])),
                                              (float(cbers['ul_lat']),
                                               float(cbers['ul_lon'])),
                                              (float(cbers['ll_lat']),
                                               float(cbers['ll_lon']))]]]

    stac_item['properties'] = OrderedDict()
    datetime = cbers['acquisition_date'].replace(' ', 'T')
    datetime = re.sub('\.\d+', 'Z', datetime)
    stac_item['properties']['datetime'] = datetime
    stac_item['properties']['provider'] = 'INPE'
    # License from INPE's site is
    # https://creativecommons.org/licenses/by-sa/3.0/
    stac_item['properties']['license'] = 'CC-BY-SA-3.0'

    # EO section
    stac_item['properties']['eo:sun_azimuth'] = float(cbers['sun_azimuth'])
    stac_item['properties']['eo:sun_elevation'] = float(cbers['sun_elevation'])
    stac_item['properties']['eo:resolution'] = float(cbers['vertical_pixel_size'])
    stac_item['properties']['eo:off_nadir_angle'] = float(cbers['roll'])
    # Missing fields (not available from CBERS metadata)
    # eo:cloud_cover
    
    return stac_item

def create_json_item(stac_item, filename):
    """
    Dumps STAC item into json file
    Input:
    stac_item(dict): stac item dictionary
    filename(string): ditto
    """

    with open(filename, 'w') as outfile:
        json.dump(stac_item, outfile, indent=2)
        
def convert(cbers_metadata):
    """Generate STAC item from CBERS metadata
    Input:
    cbers_metadata(string): CBERS metadata file location
    """

    meta = get_keys_from_cbers(cbers_metadata)
    
if __name__ == '__main__':
    assert sys.argv == 2
    convert(cbers_metadata=sys.argv[1])
