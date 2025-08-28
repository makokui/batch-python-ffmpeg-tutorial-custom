# -------------------------------------------------------------------------
#
# THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
# ----------------------------------------------------------------------------------
# The example companies, organizations, products, domain names,
# e-mail addresses, logos, people, places, and events depicted
# herein are fictitious. No association with any real company,
# organization, product, domain name, email address, logo, person,
# places, or events is intended or should be inferred.
# --------------------------------------------------------------------------

# Global constant variables (Azure Storage account/Batch details)

# import "config.py" in "batch_python_tutorial_ffmpeg.py"

# Update the Batch and Storage account credential strings below with the values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.

# Sample config template — copy to config.py and fill in your values
_STORAGE_ACCOUNT_NAME = '<your-storage-account-name>'
_STORAGE_ACCOUNT_KEY = '<your-storage-account-key>'
_BATCH_ACCOUNT_URL = 'https://<your-batch-account>.<region>.batch.azure.com'

_POOL_ID = 'LinuxFfmpegPool'
_DEDICATED_POOL_NODE_COUNT = 0
_LOW_PRIORITY_POOL_NODE_COUNT = 5
_POOL_VM_SIZE = 'STANDARD_A1_v2'
_JOB_ID = 'LinuxFfmpegJob'

# 認証モード: 'AAD' または 'SharedKey' を指定
# ユーザーサブスクリプションモードで AAD を推奨。Batch アカウントが AAD のみ許可の場合は 'AAD' にすること。
_AUTH_MODE = 'AAD'  # or 'SharedKey'
_BATCH_ACCOUNT_NAME = '<your-batch-account-name>'
_BATCH_ACCOUNT_KEY = ''  # when _AUTH_MODE == 'SharedKey', set the key
_BATCH_ACCOUNT_URL = 'https://<your-batch-account>.<region>.batch.azure.com'

# ---------------- User Subscription mode (VNet) settings ----------------
# ユーザーサブスクリプションモードでプールを作成する場合、
# サブネットのリソースIDを指定する必要があります。
# 例:
# _SUBNET_ID = '/subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.Network/virtualNetworks/<VNET>/subnets/<SUBNET>'
_SUBNET_ID = ''  # optional; set when using VNet in User Subscription mode

# ログレベル（任意）: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR'
_LOG_LEVEL = 'INFO'

# ---------------- Run behavior toggles (optional) ----------------
# 起動時に既存ジョブを削除する（推奨: True 固定相当。コード側で常に削除します）
# ジョブはコード側で毎回削除するため、明示フラグは不要です。

# 起動時に既存プールも削除して作り直すか（既定 False: プールは再利用）
_DELETE_EXISTING_POOL_AT_START = True

# 起動時に Storage の input/output コンテナーを削除してから作成するか（既定 False）
_CLEAN_STORAGE_AT_START = False