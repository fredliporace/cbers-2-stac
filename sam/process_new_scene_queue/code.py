"""process_new_scene_queue"""

import os
import re
from multiprocessing import Process
import urllib2
import boto3
import zipfile

# @todo used in other lambda functions, unify
# this is candidate for removal, it is simply a concatenation
def s3_filename(directory, filename, camera):
    """Return complete path to S3 file to be uploaded"""
    match = re.search(r'CBERS_(?P<satno>\d{1})_' + camera + r'_(?P<ymd>\d{8})'
                      r'_(?P<path>\d{3})_(?P<row>\d{3})_L(?P<level>\d{1})_'
                      r'BAND(?P<band>\d+).zip', filename)
    assert match
    return directory + '/' + match.group('ymd') + '/' + filename

def s3_file_size(bucket_name, key):
    """Return file size if key exists in S3, 0 otherwise"""

    s3res = boto3.resource('s3')
    bucket = s3res.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=key))
    exists = objs and objs[0].key == key
    if exists:
        return objs[0].size
    return 0

# @todo used in other lambda functions, unify
# @todo this is now checked in email_parser/code.py, consider removing
# double check
def s3_file_exists(bucket, key):
    """
    True if key exists.
    Works also if object is in Glacier
    """

    s3res = boto3.resource('s3')
    bucket = s3res.Bucket(bucket)
    objs = list(bucket.objects.filter(Prefix=key))
    exists = objs and objs[0].key == key
    return exists

def get_remote_filesize(url_file):
    """
    Output:
    remote filesize in bytes
    """
    uhandle = urllib2.urlopen(url_file)
    return int(uhandle.info()['Content-Length'])

def part_filename(tmpdir, filename, part_no):
    """Build part filename"""
    filename = '%s/%s_part%04d' % (tmpdir, filename, part_no)
    return filename.replace(r'//', r'/')

def download_part(url, filename, tmpdir, part_no, part_size, this_part_size, bucket):
    """
    Download file part

    If bucket is not None the part is downloaded only if not
    already available in S3. Once downloaded the part is uploaded
    to S3.

    Input:
    bucket(dict): keys 'bucket_name', 'bucket_dir', 'camera'
    """

    chunk_size = 1024 * 1024
    furl = url + '/' + filename
    local_part_filename = part_filename(tmpdir, filename, part_no)
    if bucket:
        s3filename = bucket['bucket_dir'] + local_part_filename

    # Nothing to do if part is already downloaded
    if bucket:
        size = s3_file_size(bucket['bucket_name'], s3filename)
        # @todo check if size is expected
        if size:
            # Already downloaded
            return

    # @todo retry on error
    req = urllib2.Request(furl)
    req.headers['Range'] = 'bytes=%d-%d' % (part_no * part_size,
                                            part_no * part_size + this_part_size - 1)
    partfile = urllib2.urlopen(req)
    with open(local_part_filename, 'wb') as output:
        while True:
            data = partfile.read(chunk_size)
            if data:
                output.write(data)
            else:
                break

    # Transfer part to S3
    if bucket:
        upload_to_s3(local_part_filename, s3filename,
                     bucket['bucket_name'])

def download_to_local(url, filename, tmpdir,
                      part_size=None,
                      bucket=None):
    """
    Download file (url+filename) to temporary directory,
    read file in chunks.

    Input:
    part_size(int): if defined file is downloaded in parallel parts of this size
    bucket(dict): keys 'bucket_name', 'bucket_dir', 'camera'
    """

    chunk_size = 1024 * 1024
    furl = url + '/' + filename
    if part_size:
        fsize = get_remote_filesize(furl)
        process_no = ((fsize - 1) // part_size) + 1
        processes = list()
        for process in range(0, process_no):
            if process == process_no - 1:
                this_part_size = ((fsize - 1) % part_size) + 1
            else:
                this_part_size = part_size
            processes.append(Process(target=download_part, args=(url,
                                                                 filename,
                                                                 tmpdir,
                                                                 process,
                                                                 part_size,
                                                                 this_part_size,
                                                                 bucket)))
        for process in processes:
            process.start()
        for process in processes:
            process.join()
        # @todo there is no reason to proceed here if one of the process reported
        # a failure
        with open(tmpdir + '/' + filename, 'wb') as output:
            for process in range(0, process_no):
                # @todo repeated above
                if process == process_no - 1:
                    this_part_size = ((fsize - 1) % part_size) + 1
                else:
                    this_part_size = part_size
                local_part_filename = part_filename(tmpdir, filename, process)
                # File is downloaded from S3 if not local or incorrect size
                if not os.path.isfile(local_part_filename) or \
                   os.path.getsize(local_part_filename) != this_part_size:
                    assert bucket
                    # @todo create a function for that, same rule being used in
                    # download_part()
                    s3filename = bucket['bucket_dir'] + local_part_filename
                    boto3.client('s3').download_file(bucket['bucket_name'], s3filename,
                                                     local_part_filename)
                    assert os.path.getsize(local_part_filename) == this_part_size, \
                        "Part size for %s differ, %d %d" % (local_part_filename,
                                                            os.path.getsize(local_part_filename),
                                                            this_part_size)
                with open(local_part_filename, 'rb') as fpart:
                    while True:
                        data = fpart.read(chunk_size)
                        if data:
                            output.write(data)
                        else:
                            break
                os.remove(local_part_filename)
                if bucket:
                    # @todo delete file only after target is on S3
                    s3filename = bucket['bucket_dir'] + local_part_filename
                    if s3_file_size(bucket['bucket_name'], s3filename):
                        boto3.client('s3').delete_object(Bucket=bucket['bucket_name'],
                                                         Key=s3filename)
        zfile = zipfile.ZipFile(tmpdir + '/' + filename)
        assert zfile.testzip() is None, \
            "%s is not a zip file" % (tmpdir + '/' + filename)
        return

    # @todo unify processing as single part download
    zfile = urllib2.urlopen(furl)
    with open(tmpdir + '/' + filename, 'wb') as output:
        while True:
            data = zfile.read(chunk_size)
            if data:
                output.write(data)
            else:
                break

def upload_to_s3(localfilename, s3filename, bucket_name, delete_after=False):
    """Upload file to s3"""
    s3srv = boto3.resource('s3')
    with open(localfilename, 'rb') as data:
        s3srv.Bucket(bucket_name).put_object(Key=s3filename,
                                             Body=data)
    if delete_after:
        os.remove(localfilename)

def download_to_s3_main(url, filename, tmpdir, bucket_name, bucket_dir,
                        camera, part_size):
    """ Main function
    """
    if not s3_file_exists(bucket_name,
                          s3_filename(bucket_dir, filename, camera)):
        download_to_local(url, filename, tmpdir,
                          part_size=part_size,
                          bucket={'bucket_name':bucket_name,
                                  'bucket_dir':bucket_dir,
                                  'camera':camera})
        s3filename = s3_filename(bucket_dir, filename, camera)
        localfilename = tmpdir + '/' + filename
        upload_to_s3(localfilename, s3filename, bucket_name, delete_after=True)

def handler(event, context):
    """Lambda entry point
    Event keys:
    baseurl: URL of directory where file is located
    filenames: list of filenames to be downloaded
    index: index of filename to be downloaded
    BUCKET: bucket id
    """

    return download_to_s3_main(event['url'], event['filenames'][event['index']],
                               '/tmp', os.environ['BUCKET'], event['download_dir'],
                               event['camera'], int(os.environ['DOWNLOAD_PART_SIZE']))
