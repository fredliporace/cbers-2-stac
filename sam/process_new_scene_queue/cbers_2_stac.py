#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Converts CBERS-4 scene metadata (xml) to stac item"""

import sys
import re
import os
import json
import statistics
import xml.etree.ElementTree as ET
from collections import OrderedDict
import utm

from utils import build_absolute_prefix, build_collection_name, \
    CBERS_MISSIONS

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
    dict: Dictionary with stac information
    """

    nsp = {'x': 'http://www.gisplan.com.br/xmlsat'}
    metadata = dict()

    #match = re.match(r'.*_(?P<camera>\w+)_(?P<ymd>\d{8})_.*', cbers_metadata)
    #assert match
    #ymd = match.group('ymd')

    #import nose.tools; nose.tools.set_trace()

    tree = ET.parse(cbers_metadata)
    original_root = tree.getroot()

    # satellite node information, checking for CBERS-04A WFI
    # special case
    left_root = original_root.find('x:leftCamera', nsp)
    rigth_root = None
    if left_root:
        right_root = original_root.find('x:rightCamera', nsp)
        # We use the left camera for fields that are not camera
        # specific or are not used for STAC fields computation
        root = left_root
    else:
        root = original_root

    satellite = root.find('x:satellite', nsp)

    metadata['mission'] = satellite.find('x:name', nsp).text
    metadata['number'] = satellite.find('x:number', nsp).text
    metadata['sensor'] = satellite.find('x:instrument', nsp).text
    metadata['collection'] = build_collection_name(satellite=metadata['mission'],
                                                   mission=metadata['number'],
                                                   camera=metadata['sensor'])

    # image node information
    image = root.find('x:image', nsp)
    metadata['path'] = image.find('x:path', nsp).text
    metadata['row'] = image.find('x:row', nsp).text
    metadata['processing_level'] = image.find('x:level', nsp).text
    metadata['vertical_pixel_size'] = image.find('x:verticalPixelSize',
                                                 nsp).text
    metadata['horizontal_pixel_size'] = image.find('x:horizontalPixelSize',
                                                   nsp).text
    metadata['projection_name'] = image.find('x:projectionName', nsp).text
    metadata['origin_latitude'] = image.find('x:originLatitude', nsp).text
    metadata['origin_longitude'] = image.find('x:originLongitude', nsp).text

    imagedata = image.find('x:imageData', nsp)
    metadata['ul_lat'] = imagedata.find('x:UL',
                                        nsp).find('x:latitude', nsp).text
    metadata['ul_lon'] = imagedata.find('x:UL',
                                        nsp).find('x:longitude', nsp).text
    metadata['ur_lat'] = imagedata.find('x:UR',
                                        nsp).find('x:latitude', nsp).text
    metadata['ur_lon'] = imagedata.find('x:UR',
                                        nsp).find('x:longitude', nsp).text
    metadata['lr_lat'] = imagedata.find('x:LR',
                                        nsp).find('x:latitude', nsp).text
    metadata['lr_lon'] = imagedata.find('x:LR',
                                        nsp).find('x:longitude', nsp).text
    metadata['ll_lat'] = imagedata.find('x:LL',
                                        nsp).find('x:latitude', nsp).text
    metadata['ll_lon'] = imagedata.find('x:LL',
                                        nsp).find('x:longitude', nsp).text
    metadata['ct_lat'] = imagedata.find('x:CT',
                                        nsp).find('x:latitude', nsp).text
    metadata['ct_lon'] = imagedata.find('x:CT',
                                        nsp).find('x:longitude', nsp).text

    boundingbox = image.find('x:boundingBox', nsp)
    metadata['bb_ul_lat'] = boundingbox.find('x:UL',
                                             nsp).find('x:latitude', nsp).text
    metadata['bb_ul_lon'] = boundingbox.find('x:UL',
                                             nsp).find('x:longitude', nsp).text
    metadata['bb_ur_lat'] = boundingbox.find('x:UR',
                                             nsp).find('x:latitude', nsp).text
    metadata['bb_ur_lon'] = boundingbox.find('x:UR',
                                             nsp).find('x:longitude', nsp).text
    metadata['bb_lr_lat'] = boundingbox.find('x:LR',
                                             nsp).find('x:latitude', nsp).text
    metadata['bb_lr_lon'] = boundingbox.find('x:LR',
                                             nsp).find('x:longitude', nsp).text
    metadata['bb_ll_lat'] = boundingbox.find('x:LL',
                                             nsp).find('x:latitude', nsp).text
    metadata['bb_ll_lon'] = boundingbox.find('x:LL',
                                             nsp).find('x:longitude', nsp).text

    sun_position = image.find('x:sunPosition', nsp)
    metadata['sun_elevation'] = sun_position.find('x:elevation', nsp).text
    metadata['sun_azimuth'] = sun_position.find('x:sunAzimuth', nsp).text

    if left_root:
        # Update fields for CB04A WFI special case
        lidata = left_root.find('x:image', nsp).find('x:imageData', nsp)
        ridata = right_root.find('x:image', nsp).find('x:imageData', nsp)
        metadata['ur_lat'] = ridata.find('x:UR',
                                         nsp).find('x:latitude', nsp).text
        metadata['ur_lon'] = ridata.find('x:UR',
                                         nsp).find('x:longitude', nsp).text
        metadata['lr_lat'] = ridata.find('x:LR',
                                         nsp).find('x:latitude', nsp).text
        metadata['lr_lon'] = ridata.find('x:LR',
                                         nsp).find('x:longitude', nsp).text
        metadata['ct_lat'] = \
            str(statistics.mean([float(lidata.find('x:CT',
                                                   nsp).find('x:latitude',
                                                             nsp).text),
                                 float(ridata.find('x:CT',
                                                   nsp).find('x:latitude',
                                                             nsp).text)]))
        metadata['ct_lon'] = \
            str(statistics.mean([float(lidata.find('x:CT',
                                                   nsp).find('x:longitude',
                                                             nsp).text),
                                 float(ridata.find('x:CT',
                                                   nsp).find('x:longitude',
                                                             nsp).text)]))

        spleft = left_root.find('x:image', nsp).find('x:sunPosition', nsp)
        spright = right_root.find('x:image', nsp).find('x:sunPosition', nsp)

        metadata['sun_elevation'] = \
            str(statistics.mean([float(spleft.find('x:elevation', nsp).text),
                                 float(spright.find('x:elevation', nsp).text)]))

        metadata['sun_azimuth'] = \
            str(statistics.mean([float(spleft.find('x:sunAzimuth', nsp).text),
                                 float(spright.find('x:sunAzimuth', nsp).text)]))

        bbleft = left_root.find('x:image', nsp).find('x:boundingBox', nsp)
        bbright = right_root.find('x:image', nsp).find('x:boundingBox', nsp)

        metadata['bb_ll_lat'] = \
            str(min(float(bbleft.find('x:LL',
                                      nsp).find('x:latitude', nsp).text),
                    float(bbright.find('x:LL',
                                       nsp).find('x:latitude', nsp).text)))
        metadata['bb_ll_lon'] = \
            str(min(float(bbleft.find('x:LL',
                                      nsp).find('x:longitude', nsp).text),
                    float(bbright.find('x:LL',
                                       nsp).find('x:longitude', nsp).text)))

        metadata['bb_ur_lat'] = \
            str(max(float(bbleft.find('x:UR',
                                      nsp).find('x:latitude', nsp).text),
                    float(bbright.find('x:UR',
                                       nsp).find('x:latitude', nsp).text)))
        metadata['bb_ur_lon'] = \
            str(max(float(bbleft.find('x:UR',
                                      nsp).find('x:longitude', nsp).text),
                    float(bbright.find('x:UR',
                                       nsp).find('x:longitude', nsp).text)))

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
        metadata[key] = band.attrib.get('gain')

    # viewing node information
    viewing = root.find('x:viewing', nsp)
    metadata['acquisition_date'] = viewing.find('x:center',
                                                nsp).text.replace('T', ' ')
    metadata['acquisition_day'] = metadata['acquisition_date'].split(' ')[0]

    # derived fields
    metadata['no_level_id'] = 'CBERS_%s_%s_%s_' \
                              '%03d_%03d' % (metadata['number'],
                                             metadata['sensor'],
                                             metadata['acquisition_day'].\
                                             replace('-', ''),
                                             int(metadata['path']),
                                             int(metadata['row']))

    # example: CBERS4/MUX/071/092/CBERS_4_MUX_20171105_071_092_L2
    metadata['download_url'] = 'CBERS%s/'\
                               '%s/'\
                               '%03d/%03d/'\
                               '%s' % (metadata['number'],
                                       metadata['sensor'],
                                       int(metadata['path']),
                                       int(metadata['row']),
                                       re.sub(r'_BAND\d+.xml',
                                              '',
                                              os.path.
                                              basename(cbers_metadata)))
    metadata['sat_sensor'] = 'CBERS%s/%s' % (metadata['number'],
                                             metadata['sensor'])
    metadata['sat_number'] = "{}-{}".format(metadata['mission'],
                                            metadata['number'])
    metadata['meta_file'] = os.path.basename(cbers_metadata)

    return metadata

def build_link(rel, href):
    """
    Build a rel, href link object
    """
    link = OrderedDict()
    link['rel'] = rel
    link['href'] = href
    return link

def build_asset(href, sat_number, title=None, asset_type=None, band_id=None,
                properties=None):
    """
    Build a asset entry
    """
    asset = OrderedDict()
    asset['href'] = href
    if title:
        asset['title'] = title
    if asset_type:
        asset['type'] = asset_type
    if properties:
        for key in properties:
            asset[key] = properties[key]
    if band_id is not None:
        eo_band = OrderedDict()
        eo_band['name'] = band_id
        eo_band['common_name'] = CBERS_MISSIONS[sat_number]['band']\
            [band_id]['common_name']
        asset['eo:bands'] = [eo_band]
    return asset

def build_stac_item_keys(cbers, buckets):
    """
    Builds a STAC item dict based on CBERS metadata

    Input:
    cbers(dict): CBERS metadata
    buckets(dict): buckets identification
    """

    stac_item = OrderedDict()

    stac_item['stac_version'] = "1.0.0-beta.2"
    stac_item['stac_extensions'] = ["projection", "view", "eo"]
    stac_item['id'] = 'CBERS_%s_%s_%s_' \
                      '%03d_%03d_L%s' % (cbers['number'],
                                         cbers['sensor'],
                                         cbers['acquisition_day'].\
                                         replace('-', ''),
                                         int(cbers['path']),
                                         int(cbers['row']),
                                         cbers['processing_level'])

    stac_item['type'] = 'Feature'

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

    # Order is lower left lon, lat; upper right lon, lat
    stac_item['bbox'] = (float(cbers['bb_ll_lon']), float(cbers['bb_ll_lat']),
                         float(cbers['bb_ur_lon']), float(cbers['bb_ur_lat']))

    # Collection
    stac_item['collection'] = cbers['collection']

    stac_item['properties'] = OrderedDict()
    datetime = cbers['acquisition_date'].replace(' ', 'T')
    datetime = re.sub(r'\.\d+', 'Z', datetime)
    stac_item['properties']['datetime'] = datetime

    # Common metadata
    stac_item['properties']['platform'] = cbers['sat_number']
    stac_item['properties']['instruments'] = [cbers['sensor']]

    # Links
    meta_prefix = 'https://s3.amazonaws.com/%s/' % (buckets['metadata'])
    main_prefix = 's3://%s/' % (buckets['cog'])
    stac_prefix = 'https://%s.s3.amazonaws.com/' % (buckets['stac'])
    # https://s3.amazonaws.com/cbers-meta-pds/CBERS4/MUX/066/096/CBERS_4_MUX_20170522_066_096_L2/CBERS_4_MUX_20170522_066_096.jpg
    stac_item['links'] = list()

    # links, self
    stac_item['links'].\
        append(build_link('self',
                          build_absolute_prefix(buckets['stac'],
                                                cbers['sat_sensor'],
                                                int(cbers['path']),
                                                int(cbers['row'])) + \
                          stac_item['id'] + '.json'))

    # links, parent
    stac_item['links'].\
        append(build_link('parent',
                          build_absolute_prefix(buckets['stac'],
                                                cbers['sat_sensor'],
                                                int(cbers['path']),
                                                int(cbers['row'])) + \
                          'catalog.json'))

    # link, collection
    stac_item['links'].\
        append(build_link(rel='collection',
                          href=stac_prefix + \
                          cbers['mission'] + \
                          cbers['number'] + \
                          '/' + cbers['sensor'] + '/collection.json'))

    # EO section
    # Missing fields (not available from CBERS metadata)
    # eo:cloud_cover

    # VIEW extension
    stac_item['properties']['view:sun_azimuth'] = float(cbers['sun_azimuth'])
    stac_item['properties']['view:sun_elevation'] = float(cbers['sun_elevation'])
    stac_item['properties']['view:off_nadir'] = abs(float(cbers['roll']))

    # PROJECTION extension
    assert cbers['projection_name'] == 'UTM', \
        'Unsupported projection ' + cbers['projection_name']
    utm_zone = int(utm.from_latlon(float(cbers['ct_lat']),
                                   float(cbers['ct_lon']))[2])
    if float(cbers['ct_lat']) < 0.:
        utm_zone *= -1
    stac_item['properties']\
        ['proj:epsg'] = int(epsg_from_utm_zone(utm_zone))

    # CBERS section
    stac_item['properties']['cbers:data_type'] = 'L' + cbers['processing_level']
    stac_item['properties']['cbers:path'] = int(cbers['path'])
    stac_item['properties']['cbers:row'] = int(cbers['row'])

    # Assets
    stac_item['assets'] = OrderedDict()
    stac_item['assets']\
        ['thumbnail'] = build_asset(meta_prefix + \
                                    cbers['download_url'] + \
                                    '/' + \
                                    cbers['no_level_id'] + '.' + \
                                    CBERS_MISSIONS[cbers['sat_number']]\
                                    ["quicklook"]["extension"],
                                    cbers['sat_number'],
                                    asset_type="image/" + \
                                    CBERS_MISSIONS[cbers['sat_number']]\
                                    ["quicklook"]["type"])

    stac_item['assets']\
        ['metadata'] = build_asset(main_prefix + \
                                   cbers['download_url'] + \
                                   '/' + \
                                   cbers['meta_file'],
                                   cbers['sat_number'],
                                   asset_type="text/xml",
                                   title="INPE original metadata")
    for index, band in enumerate(cbers['bands']):
        band_id = "B" + band
        gsd = CBERS_MISSIONS[cbers['sat_number']]\
            ["band"][band_id].get("gsd")
        if gsd:
            properties = {
                "gsd": gsd
            }
        else:
            properties = None
        stac_item['assets'][band_id] = \
            build_asset(main_prefix + \
                        cbers['download_url'] + '/' + \
                        stac_item['id'] + '_BAND' + band + '.tif',
                        cbers['sat_number'],
                        asset_type="image/tiff; application=geotiff; "\
                        "profile=cloud-optimized",
                        band_id=band_id,
                        properties=properties)
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
    stac_metadata(string): STAC item metadata file to be written, if None then
                           no file is written.
    buckets: buckets dictionary

    Return:
    Dictionary based on INPE's metadata
    """

    meta = get_keys_from_cbers(inpe_metadata_filename)
    stac_meta = build_stac_item_keys(meta, buckets)
    if stac_metadata_filename:
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
