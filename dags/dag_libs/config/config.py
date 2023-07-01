# Batch processing module name
import os

MODULE_NAME = "transfer_to_click"
POOL = "transfer_pool"
VARIABLES_NAME = 'transfer_to_click'
GP_CONNECTION = 'gp_admin'
AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID','minio')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY','minio123')
