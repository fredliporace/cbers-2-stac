"""utils.py"""

def build_absolute_prefix(bucket, sat_sensor=None, path=None, row=None):
    """Returns the absolute prefix given the bucket/satellite/path/row"""

    prefix = 'https://%s.s3.amazonaws.com' % bucket
    if sat_sensor:
        prefix += '/' + sat_sensor
    if path:
        prefix += '/%03d' % int(path)
    if row:
        prefix += '/%03d' % int(row)
    prefix += '/'
    return prefix
