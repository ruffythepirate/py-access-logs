import boto3
s3 = boto3.resource('s3')

def ensure_purpose_bucket(purpose):
    devBlog = get_bucket_with_tag('purpose', purpose)
    if devBlog is None:
        add_tag_to_bucket('dev-blog.net', 'purpose', purpose)

def create_athena_bucket():
    s3.
    print('bam')

def get_bucket_with_tag(tagKey, tagValue):
    for bucket in s3.buckets.all():
        tags = s3.BucketTagging(bucket.name)
        for dict in tags.tag_set:
            if dict['Key'] == tagKey and dict['Value'] == tagValue:
                return bucket

def add_tag_to_bucket(bucketName, tagKey, tagValue):
    tagging = s3.BucketTagging(bucketName)
    tagging.tag_set.append({"Key": tagKey, "Value": tagValue})
    tagging.put(Tagging={
            'TagSet': tagging.tag_set
        }
    )

def main():
    ensure_purpose_bucket('dev-blog.net')
    bucket = get_bucket_with_tag('purpose', 'dev-blog.net')
    if bucket is not None:
        print('found bucket! ' + bucket.name)
    else:
        print('still cant find tagged bucket :(')

if __name__ == "__main__":
    main()
