import os

from ziki_helpers.aws.secrets_manager import get_secret


toast_secrets = get_secret('Toast_API')
for k, v in toast_secrets.items():
    os.environ[k] = str(v)

# For toast-auth, stores the toast_token.json file in S3
os.environ['BUCKET_NAME'] = 'ziki-analytics-config'
os.environ['FILE_NAME'] = 'toast_token.json'