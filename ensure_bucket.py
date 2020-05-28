import boto3
from botocore.exceptions import ClientError

_s3 = boto3.resource('s3')

def ensure_athena_bucket():
    existing_bucket = _get_bucket_with_tag('purpose', 'athena')
    if(existing_bucket is None):
        print('No bucket found tagged with purpose athena, creating bucket...')
        return create_athena_bucket()
    print('Athena bucket already exists, returning bucket %s' % (existing_bucket.name))
    return existing_bucket

def create_athena_bucket():
    # x Properties that are desired: Retention time of 1 day
    # x Random name after athena-results-
    # x Private bucket, Athena should be allowed to write to it.
    # x Tagged with purpose Athena
    import uuid

    new_bucket = _s3.Bucket('athena-results-' + uuid.uuid1().hex[0:8])

    location = new_bucket.create(
        ACL = 'private',
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-1'
        },
        ObjectLockEnabledForBucket=False
    )

    print('created bucket %s' % (new_bucket.name))

    set_retention_lifecycle_configuration(new_bucket.name)

    add_purpose_to_bucket(new_bucket.name, 'athena')

def set_retention_lifecycle_configuration(bucket_name):
    print('setting lifecycle configuration for %s' % (bucket_name))
    bucket = _s3.Bucket(bucket_name)
    bucket.LifecycleConfiguration().put(
        LifecycleConfiguration= {
            'Rules': [
                {
                    'Expiration': {
                        'Days': 2
                    },
                    'Filter': {
                        'Prefix': '/'
                    },
                    'ID': 'athena-retention',
                    'Status': 'Enabled',
                    'AbortIncompleteMultipartUpload': {
                        'DaysAfterInitiation': 1
                    }
                }
            ]
        }
    )
    print('created lifecycle configuration for bucket %s' % (bucket.name))



def _get_bucket_with_tag(tagKey, tagValue):
    for bucket in _s3.buckets.all():
        try:
            tags = _s3.BucketTagging(bucket.name)
            for dict in tags.tag_set:
                if dict['Key'] == tagKey and dict['Value'] == tagValue:
                    return bucket
        except:
            e = sys.exc_info()[0]
            print('Failed for bucket %s with exception %s' % (bucket.name, e))


def add_purpose_to_bucket(bucketName, tagValue):
    print('adding purpose tag to bucket %s' % (bucketName))
    tag_set = []
    tagging = _s3.BucketTagging(bucketName)
    try:
        tag_set = tagging.tag_set
    except ClientError:
        pass
    if not contains_purpose(tag_set, 'athena'):
        tag_set.append({"Key": 'purpose', "Value": tagValue})
        tagging.put(Tagging={
                'TagSet': tag_set
            }
        )
        print('added purpose tag "athena" for bucket %s' % (bucket.name))
    else:
        print('purpose already exists! doing nothing.')

def contains_purpose(tag_set, purpose):
    for dict in tag_set:
        if dict['Key'] == 'purpose' and dict['Value'] == purpose:
            return True
    return False

def main():
    bucket = ensure_athena_bucket()
    if bucket is not None:
        print('Bucket found or created!' + bucket.name)
    else:
        print('Bucket still not there, seems like a bug :(')

if __name__ == "__main__":
    main()
