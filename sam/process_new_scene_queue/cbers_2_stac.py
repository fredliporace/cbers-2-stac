#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Converts CBERS-4 scene metadata (xml) to stac item"""

import sys
import re
import os
import json
import xml.etree.ElementTree as ET
from collections import OrderedDict

def epsg_from_utm_zone(zone):
    """
    Returns the WGS-84 EPSG for a given UTM zone
    Input:
    zone(int): Zone, positive values for North
    Output:
    epsg code(int).
    """

    if zone > 0:
        epsg = 32600 + zone
    else:
        epsg = 32700 - zone
    return epsg

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
    metadata['mission'] = satellite.find('x:name', nsp).text
    metadata['number'] = satellite.find('x:number', nsp).text
    metadata['sensor'] = satellite.find('x:instrument', nsp).text

    # image node information
    image = root.find('x:image', nsp)
    metadata['path'] = image.find('x:path', nsp).text
    metadata['row'] = image.find('x:row', nsp).text
    metadata['processing_level'] = image.find('x:level', nsp).text
    metadata['vertical_pixel_size'] = image.find('x:verticalPixelSize', nsp).text
    metadata['horizontal_pixel_size'] = image.find('x:horizontalPixelSize', nsp).text
    metadata['projection_name'] = image.find('x:projectionName', nsp).text
    metadata['origin_latitude'] = image.find('x:originLatitude', nsp).text
    metadata['origin_longitude'] = image.find('x:originLongitude', nsp).text

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
    metadata['bands'] = list()
    for band in available_bands.findall('x:band', nsp):
        metadata['bands'].append(band.text)
        key = 'band_%s_gain' % (band.text)
        metadata[key] = band.attrib['gain']

    # viewing node information
    viewing = root.find('x:viewing', nsp)
    metadata['acquisition_date'] = viewing.find('x:center', nsp).text.replace('T', ' ')
    metadata['acquisition_day'] = metadata['acquisition_date'].split(' ')[0]

    # derived fields
    metadata['no_level_id'] = 'CBERS_%s_%s_%s_' \
                              '%03d_%03d' % (metadata['number'],
                                             metadata['sensor'],
                                             metadata['acquisition_day'].replace('-', ''),
                                             int(metadata['path']),
                                             int(metadata['row']))

    # example: CBERS4/MUX/071/092/CBERS_4_MUX_20171105_071_092_L2
    metadata['download_url'] = 'CBERS%s/%s/%03d/%03d/%s' % (metadata['number'],
                                                            metadata['sensor'],
                                                            int(metadata['path']),
                                                            int(metadata['row']),
                                                            re.sub(r'_BAND\d+.xml', '',
                                                                   os.path.
                                                                   basename(cbers_metadata)))
    metadata['sat_sensor'] = 'CBERS%s/%s' % (metadata['number'],
                                             metadata['sensor'])
    metadata['meta_file'] = os.path.basename(cbers_metadata)

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

def build_stac_item_keys(cbers, buckets):
    """
    Builds a STAC item dict based on CBERS metadata

    Input:
    cbers(dict): CBERS metadata
    buckets(dict): buckets identification
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
    # Order is lower left lon, lat; upper right lon, lat
    stac_item['bbox'] = (float(cbers['bb_ll_lon']), float(cbers['bb_ll_lat']),
                         float(cbers['bb_ur_lon']), float(cbers['bb_ur_lat']))

    stac_item['geometry'] = OrderedDict()
    stac_item['geometry']['type'] = 'MultiPolygon'
    stac_item['geometry']['coordinates'] = [[[(float(cbers['ll_lon']),
                                               float(cbers['ll_lat'])),
                                              (float(cbers['lr_lon']),
                                               float(cbers['lr_lat'])),
                                              (float(cbers['ur_lon']),
                                               float(cbers['ur_lat'])),
                                              (float(cbers['ul_lon']),
                                               float(cbers['ul_lat'])),
                                              (float(cbers['ll_lon']),
                                               float(cbers['ll_lat']))]]]

    stac_item['properties'] = OrderedDict()
    datetime = cbers['acquisition_date'].replace(' ', 'T')
    datetime = re.sub(r'\.\d+', 'Z', datetime)
    stac_item['properties']['datetime'] = datetime
    stac_item['properties']['provider'] = 'INPE'
    # License from INPE's site is
    # https://creativecommons.org/licenses/by-sa/3.0/
    # Removed from Item since this is defined at catalog level
    # stac_item['properties']['license'] = 'CC-BY-SA-3.0'

    # EO section
    stac_item['properties']['eo:collection'] = 'default'
    stac_item['properties']['eo:sun_azimuth'] = float(cbers['sun_azimuth'])
    stac_item['properties']['eo:sun_elevation'] = float(cbers['sun_elevation'])
    #stac_item['properties']['eo:resolution'] = float(cbers['vertical_pixel_size'])
    stac_item['properties']['eo:off_nadir'] = float(cbers['roll'])
    assert cbers['projection_name'] == 'UTM', \
        'Unsupported projection ' + cbers['projection_name']
    stac_item['properties']['eo:epsg'] = int(epsg_from_utm_zone(int(cbers['origin_longitude'])))
    # Missing fields (not available from CBERS metadata)
    # eo:cloud_cover

    # CBERS section
    stac_item['properties']['cbers:data_type'] = 'L' + cbers['processing_level']
    stac_item['properties']['cbers:path'] = int(cbers['path'])
    stac_item['properties']['cbers:row'] = int(cbers['row'])

    # Links
    meta_prefix = 'https://s3.amazonaws.com/%s/' % (buckets['metadata'])
    main_prefix = 's3://%s/' % (buckets['cog'])
    stac_prefix = 'https://%s.s3.amazonaws.com/' % (buckets['stac'])
    # https://s3.amazonaws.com/cbers-meta-pds/CBERS4/MUX/066/096/CBERS_4_MUX_20170522_066_096_L2/CBERS_4_MUX_20170522_066_096.jpg
    stac_item['links'] = OrderedDict()

    stac_item['links']['self'] = OrderedDict()
    stac_item['links']['self']['rel'] = 'self'
    # Option if Items are organized by path and row
    #stac_item['links'][0]['href'] = meta_prefix + \
    #                                cbers['download_url'] + '/' + stac_item['id'] + '.json'
    # Option if Items are organized in the same camera subdir
    #stac_item['links'][0]['href'] = stac_prefix + \
    #                                cbers['sat_sensor'] + '/' + stac_item['id'] + '.json'
    stac_item['links']['self']['href'] = stac_prefix + \
                                         cbers['sat_sensor'] + '/' + \
                                         "%03d" % (int(cbers['path'])) + \
                                         '/' + "%03d" % (int(cbers['row'])) + '/' + \
                                         stac_item['id'] + '.json'

    stac_item['links']['catalog'] = OrderedDict()
    stac_item['links']['catalog']['rel'] = 'catalog'
    stac_item['links']['catalog']['href'] = stac_prefix + cbers['sat_sensor'] + \
                                            '/' + "%03d" % (int(cbers['path'])) + '/catalog.json'

    # Collection
    collection_id = cbers['mission'] + '_' + \
                    cbers['number'] + '_' + \
                    cbers['sensor'] + '_' + \
                    'L' + cbers['processing_level']
    stac_item['links']['collection'] = OrderedDict()
    stac_item['links']['collection']['rel'] = 'collection'
    stac_item['links']['collection']['href'] = stac_prefix + 'collections/' + \
                                               collection_id + \
                                               '_collection.json'
    stac_item['properties']['c:id'] = collection_id

    # Assets
    stac_item['assets'] = OrderedDict()
    stac_item['assets']['thumbnail'] = OrderedDict()
    stac_item['assets']['thumbnail']['href'] = meta_prefix + \
                                               cbers['download_url'] + '/' + \
                                               cbers['no_level_id'] + '.jpg'
    stac_item['assets']['thumbnail']['type'] = 'jpeg'
    stac_item['assets']['metadata'] = OrderedDict()
    stac_item['assets']['metadata']['href'] = main_prefix + \
                                               cbers['download_url'] + '/' + \
                                               cbers['meta_file']
    stac_item['assets']['metadata']['type'] = 'xml'
    for band in cbers['bands']:
        band_id = "B" + band
        stac_item['assets'][band_id] = OrderedDict()
        stac_item['assets'][band_id]['href'] = main_prefix + \
                                               cbers['download_url'] + '/' + \
                                               stac_item['id'] + '_BAND' + band + '.tif'
        stac_item['assets'][band_id]['type'] = 'GeoTIFF'
        stac_item['assets'][band_id]['format'] = 'COG'
        stac_item['assets'][band_id]['eo_bands'] = [band]

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

def convert_inpe_to_stac(inpe_metadata_filename, stac_metadata_filename,
                         buckets):
    """
    Generate STAC item in stac_metadata from inpe_metadata.

    Input:
    inpe_metadata(string): CBERS metadata (INPE format) file
    stac_metadata(string): STAC item metadata file to be written
    buckets: buckets dictionary

    Return:
    Dictionary based on INPE's metadata
    """

    meta = get_keys_from_cbers(inpe_metadata_filename)
    stac_meta = build_stac_item_keys(meta, buckets)
    create_json_item(stac_meta, stac_metadata_filename)
    return stac_meta

if __name__ == '__main__':
    # Command line arguments
    # inpe_metadata filename (1)
    # stac_metadata filename (2)

    BUCKETS = {
        'metadata':'cbers-meta-pds',
        'cog':'cbers-pds',
        'stac':'cbers-stac'}

    assert sys.argv == 3
    convert_inpe_to_stac(inpe_metadata_filename=sys.argv[1],
                         stac_metadata_filename=sys.argv[2],
                         buckets=BUCKETS)