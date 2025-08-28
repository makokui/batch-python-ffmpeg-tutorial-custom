from __future__ import print_function
import datetime
import io
import os
import sys
import time
import logging
import config

"""
Python 3 専用のため raw_input 互換対応は不要
"""

from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from azure.batch import BatchServiceClient
from azure.batch import models as batchmodels
from azure.batch import batch_auth
from msrest.authentication import BasicTokenAuthentication
from azure.core.exceptions import HttpResponseError

sys.path.append('.')
sys.path.append('..')

# Update the Batch and Storage account credential strings in config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.


def _setup_logger():
    """セットアップ済みロガーを返す。LOG_LEVEL 環境変数 or config._LOG_LEVEL を尊重。"""
    logger = logging.getLogger("batch_ffmpeg")
    if logger.handlers:
        return logger
    # 既定 INFO。env > config の順で解釈
    level_name = os.getenv("LOG_LEVEL") or getattr(config, "_LOG_LEVEL", "INFO")
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    # noisy なライブラリの既定レベルを抑制（必要に応じて）
    logging.getLogger("azure").setLevel(logging.WARNING)
    return logger


log = _setup_logger()


def log_batch_exception(batch_exception):
    """Log details of a Batch exception."""
    log.error("----- Batch exception -----")
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        log.error(batch_exception.error.message.value)
        if batch_exception.error.values:
            for mesg in batch_exception.error.values:
                log.error("%s: %s", mesg.key, mesg.value)
    log.error("----------------------")


class StorageV12:
    """Entra ID + v12 SDK のラッパー（ユーザー委任 SAS を発行）。"""
    def __init__(self, account_name: str, credential: DefaultAzureCredential):
        self.account_name = account_name
        self.account_url = f"https://{account_name}.blob.core.windows.net"
        self.cred = credential
        self.svc = BlobServiceClient(account_url=self.account_url, credential=self.cred)

    def ensure_container(self, container_name: str):
        client = self.svc.get_container_client(container_name)
        try:
            if client.exists():
                log.debug("Container '%s': exists", container_name)
            else:
                self.svc.create_container(container_name)
                log.info("Created container '%s'", container_name)
        except HttpResponseError as e:
            # AAD での RBAC 不足時に 403 AuthorizationFailure となる
            if getattr(e, 'status_code', None) == 403 or 'AuthorizationFailure' in str(e):
                log.error("Insufficient Storage permissions (AuthorizationFailure). Assign 'Storage Blob Data Contributor' or higher at the storage account scope.")
                log.error("For user delegation SAS, 'Microsoft.Storage/storageAccounts/userDelegationKeys/read' permission is also required.")
                log.error("Also verify you're signed in to the correct tenant/subscription (az login / VS Code Azure extension).")
            raise

    def upload_blob_from_path(self, container_name: str, blob_name: str, file_path: str, overwrite=True):
        log.info("Upload: %s -> container '%s' blob '%s'", file_path, container_name, blob_name)
        blob = self.svc.get_blob_client(container=container_name, blob=blob_name)
        with open(file_path, "rb") as f:
            blob.upload_blob(f, overwrite=overwrite)

    def _get_user_delegation_key(self, hours=2):
        now = datetime.datetime.now(datetime.UTC)
        return self.svc.get_user_delegation_key(
            key_start_time=now - datetime.timedelta(minutes=5),
            key_expiry_time=now + datetime.timedelta(hours=hours)
        )

    def make_blob_user_delegation_sas_url(self, container_name: str, blob_name: str,
                                          permissions: BlobSasPermissions,
                                          expiry_hours: int = 2) -> str:
        udk = self._get_user_delegation_key(hours=expiry_hours)
        sas = generate_blob_sas(
            account_name=self.account_name,
            container_name=container_name,
            blob_name=blob_name,
            user_delegation_key=udk,
            permission=permissions,
            expiry=datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=expiry_hours)
        )
        return f"{self.account_url}/{container_name}/{blob_name}?{sas}"

    def make_container_user_delegation_sas_url(self, container_name: str,
                                               permissions: BlobSasPermissions,
                                               expiry_hours: int = 2) -> str:
        udk = self._get_user_delegation_key(hours=expiry_hours)
        sas = generate_container_sas(
            account_name=self.account_name,
            container_name=container_name,
            user_delegation_key=udk,
            permission=permissions,
            expiry=datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=expiry_hours)
        )
        return f"{self.account_url}/{container_name}?{sas}"

    def delete_container_if_exists(self, container_name: str):
        client = self.svc.get_container_client(container_name)
        if client.exists():
            log.info("Deleting container '%s'", container_name)
            self.svc.delete_container(container_name)
        else:
            log.debug("Container '%s' does not exist (skip delete)", container_name)


class AADTokenCredentials(BasicTokenAuthentication):
    """毎リクエスト直前に AAD トークンを更新し、msrest 既定の挙動で送る安全版。"""
    def __init__(self, credential: DefaultAzureCredential, scope: str):
        self.credential = credential
        self.scope = scope
        super().__init__(self._token_dict())  # 初期トークンを設定

    def _token_dict(self):
        t = self.credential.get_token(self.scope)
        return {"access_token": t.token}

    def signed_session(self, session=None):
        # super を呼ぶ前に必ず最新トークンを注入（msrest は self.token を参照する）
        self.token = self._token_dict()
        return super().signed_session(session)


def upload_file_to_container(storage: StorageV12, container_name: str, file_path: str):
    """1) アップロード 2) 読み取りユーザー委任 SAS を返す"""
    blob_name = os.path.basename(file_path)
    storage.upload_blob_from_path(container_name, blob_name, file_path)
    sas_url = storage.make_blob_user_delegation_sas_url(
        container_name, blob_name, permissions=BlobSasPermissions(read=True))
    return batchmodels.ResourceFile(file_path=blob_name, http_url=sas_url)


def get_container_sas_url_for_write(storage: StorageV12, container_name: str):
    """出力用: コンテナーへの書き込みを許可するユーザー委任 SAS URL を返す"""
    perms = BlobSasPermissions(read=True, write=True, add=True, create=True, list=True)
    return storage.make_container_user_delegation_sas_url(container_name, perms)


"""旧 get_container_sas_url は v12+AAD では get_container_sas_url_for_write に置換"""


def _normalize_batch_url(url: str) -> str:
    """Ensure Batch URL has scheme and no trailing slash."""
    if not url or not isinstance(url, str):
        raise ValueError("config._BATCH_ACCOUNT_URL is not set. Specify 'https://<account>.<region>.batch.azure.com'.")
    u = url.strip()
    if not u.lower().startswith("http"):
        u = "https://" + u
    return u.rstrip('/')


def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sky
    """
    log.info("Creating pool '%s'...", pool_id)

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

    # The start task installs ffmpeg on each node from an available repository, using
    # an administrator user identity.

    # 任意の VNet/Subnet を指定（_SUBNET_ID が空なら None）
    net_conf = None
    if getattr(config, '_SUBNET_ID', ''):
        net_conf = batchmodels.NetworkConfiguration(
            subnet_id=config._SUBNET_ID
        )

    # Azure Monitor Agent 拡張
    ama_extension = batchmodels.VMExtension(
        name="AzureMonitorLinuxAgent",
        publisher="Microsoft.Azure.Monitor",
        type="AzureMonitorLinuxAgent",
        type_handler_version="1.0",
        auto_upgrade_minor_version=True,
        settings={}  # 基本は空でOK、DCRに紐づければLAに飛ぶ
    )

    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="Canonical",
                offer="0001-com-ubuntu-server-jammy",
                sku="22_04-lts",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 22.04",
            extensions=[ama_extension]   # ←ここで拡張を追加
        ),
        network_configuration=net_conf,
        vm_size=config._POOL_VM_SIZE,
        target_dedicated_nodes=config._DEDICATED_POOL_NODE_COUNT,
        target_low_priority_nodes=config._LOW_PRIORITY_POOL_NODE_COUNT,
        start_task=batchmodels.StartTask(
            command_line="/bin/bash -c \"apt-get update && apt-get install -y ffmpeg\"",
            wait_for_success=True,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=batchmodels.ElevationLevel.admin)),
        )
    )

    try:
        batch_service_client.pool.add(new_pool)
    except batchmodels.BatchErrorException as e:
        code = getattr(getattr(e, 'error', None), 'code', None)
        if code == 'PoolExists':
            log.info("Pool '%s' already exists. Reusing it.", pool_id)
        else:
            raise


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    log.info("Creating job '%s'...", job_id)

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    try:
        batch_service_client.job.add(job)
    except batchmodels.BatchErrorException as e:
        code = getattr(getattr(e, 'error', None), 'code', None)
        if code == 'JobExists':
            log.info("Job '%s' already exists. Reusing it.", job_id)
        else:
            raise


def add_tasks(batch_service_client, job_id, input_files, output_container_sas_url):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    log.info("Adding %d tasks to job '%s'...", len(input_files), job_id)

    tasks = list()

    for idx, input_file in enumerate(input_files):
        input_file_path = input_file.file_path
        output_file_path = "".join((input_file_path).split('.')[:-1]) + '.mp3'
        command = "/bin/bash -c \"ffmpeg -i {} {} \"".format(
            input_file_path, output_file_path)
        tasks.append(batchmodels.TaskAddParameter(
            id='Task{}'.format(idx),
            command_line=command,
            resource_files=[input_file],
            output_files=[batchmodels.OutputFile(
                file_pattern=output_file_path,
                destination=batchmodels.OutputFileDestination(
                          container=batchmodels.OutputFileBlobContainerDestination(
                              container_url=output_container_sas_url)),
                upload_options=batchmodels.OutputFileUploadOptions(
                    upload_condition=batchmodels.OutputFileUploadCondition.task_success))]
        )
        )
    batch_service_client.task.add_collection(job_id, tasks)


def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout
    log.info("Monitoring tasks until 'Completed' (timeout: %s)", timeout)
    next_log = time.time()
    while datetime.datetime.now() < timeout_expiration:
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            log.info("All tasks reached 'Completed'.")
            return True
        else:
            if time.time() >= next_log:
                remain = (timeout_expiration - datetime.datetime.now()).total_seconds()
                log.debug("Monitoring... incomplete: %d, ~%ds left", len(incomplete_tasks), int(remain))
                next_log = time.time() + 5
            time.sleep(1)

    # Timeout: dump diagnostics before raising
    try:
        dump_batch_diagnostics(batch_service_client, job_id)
    except Exception as diag_err:
        log.warning("Failed to output diagnostics: %s", diag_err)
        raise RuntimeError(f"ERROR: tasks did not reach 'Completed' within timeout: {timeout}")


def dump_batch_diagnostics(batch_service_client, job_id, max_log_bytes=4096):
    """Prints a concise diagnostics report for the job/pool/nodes/tasks.

    - Task states summary and per-task details (exit code, failure info)
    - Tail of stdout/stderr for each task (best-effort)
    - Pool allocation and compute node states, including start task info
    """
    log.info("===== Azure Batch Diagnostics (start) =====")
    # Resolve pool id from job
    job = batch_service_client.job.get(job_id)
    pool_id = None
    if job.pool_info and getattr(job.pool_info, 'pool_id', None):
        pool_id = job.pool_info.pool_id
    log.info("Job: %s; Pool: %s", job_id, pool_id)

    # Tasks overview
    tasks = list(batch_service_client.task.list(job_id))
    by_state = {}
    for t in tasks:
        s = str(t.state)
        by_state[s] = by_state.get(s, 0) + 1
    log.info("Task states: %s", by_state)

    for t in tasks:
        exec_info = getattr(t, 'execution_info', None)
        exit_code = getattr(exec_info, 'exit_code', None) if exec_info else None
        failure = getattr(exec_info, 'failure_info', None) if exec_info else None
        node_info = getattr(t, 'node_info', None)
        node_id = getattr(node_info, 'node_id', None) if node_info else None
        log.info("- Task %s: state=%s, node=%s, exit=%s", t.id, t.state, node_id, exit_code)
        if failure:
            log.warning("  failure: category=%s, code=%s, message=%s", failure.category, failure.code, getattr(failure, 'message', None))
            if getattr(failure, 'details', None):
                for d in failure.details:
                    log.warning("    %s: %s", d.name, d.value)
        # Fetch stdout/stderr (best-effort)
        for fname in ("stdout.txt", "stderr.txt"):
            try:
                stream = io.BytesIO()
                batch_service_client.file.get_from_task(job_id, t.id, fname, stream)
                data = stream.getvalue()
                if not data:
                    continue
                tail = data[-max_log_bytes:]
                log.info("  %s (last %d bytes):\n%s", fname, len(tail), tail.decode(errors='replace'))
            except Exception as _:
                pass

    # Pool / nodes
    if pool_id:
        try:
            pool = batch_service_client.pool.get(pool_id)
            alloc = getattr(pool, 'allocation_state', None)
            log.info("Pool state=%s, allocation=%s, target(ded/low)=%s/%s, current(ded/low)=%s/%s",
                     pool.state, alloc,
                     pool.target_dedicated_nodes, pool.target_low_priority_nodes,
                     pool.current_dedicated_nodes, pool.current_low_priority_nodes)
        except Exception as _:
            pass
        try:
            nodes = list(batch_service_client.compute_node.list(pool_id))
            log.info("Nodes: %d", len(nodes))
            for n in nodes[:50]:
                start_info = getattr(n, 'start_task_information', None)
                st_state = getattr(start_info, 'state', None) if start_info else None
                st_exit = getattr(start_info, 'exit_code', None) if start_info else None
                log.info("- Node %s: state=%s, sched=%s, start_task=%s, start_exit=%s",
                         n.id, n.state, getattr(n, 'scheduling_state', None), st_state, st_exit)
                if start_info and getattr(start_info, 'failure_info', None):
                    fi = start_info.failure_info
                    log.warning("  start_task failure: category=%s, code=%s, message=%s",
                                fi.category, fi.code, getattr(fi, 'message', None))
        except Exception as _:
            pass
    log.info("===== Azure Batch Diagnostics (end) =====")


# ==========================
# Utility: delete-if-exists
# ==========================
def _delete_job_if_exists(batch_client: BatchServiceClient, job_id: str, wait_seconds: int = 60):
    """Delete a job if it exists and wait until it's gone (best-effort)."""
    try:
        log.info("Deleting existing job '%s' if present...", job_id)
        batch_client.job.delete(job_id)
    except batchmodels.BatchErrorException as e:
        code = getattr(getattr(e, 'error', None), 'code', None)
        if code not in ('JobNotFound', 'ResourceNotFound'):
            raise
        return
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        try:
            batch_client.job.get(job_id)
            time.sleep(2)
        except batchmodels.BatchErrorException as e:
            code = getattr(getattr(e, 'error', None), 'code', None)
            if code in ('JobNotFound', 'ResourceNotFound'):
                return
            raise
    log.warning("Job '%s' deletion confirmation did not complete within %ds.", job_id, wait_seconds)


def _delete_pool_if_exists(batch_client: BatchServiceClient, pool_id: str, wait_seconds: int = 120):
    """Delete a pool if it exists and wait until it's gone (best-effort)."""
    try:
        log.info("Deleting existing pool '%s' if present...", pool_id)
        batch_client.pool.delete(pool_id)
    except batchmodels.BatchErrorException as e:
        code = getattr(getattr(e, 'error', None), 'code', None)
        if code not in ('PoolNotFound', 'ResourceNotFound'):
            raise
        return
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        try:
            batch_client.pool.get(pool_id)
            time.sleep(3)
        except batchmodels.BatchErrorException as e:
            code = getattr(getattr(e, 'error', None), 'code', None)
            if code in ('PoolNotFound', 'ResourceNotFound'):
                return
            raise
    log.warning("Pool '%s' deletion confirmation did not complete within %ds.", pool_id, wait_seconds)


if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    log.info('Start time: %s', start_time)

    # Storage (AAD, v12)
    aad_cred = DefaultAzureCredential(exclude_interactive_browser_credential=False)
    storage = StorageV12(config._STORAGE_ACCOUNT_NAME, aad_cred)

    input_container_name = 'input'
    output_container_name = 'output'
    # Optional: clean storage at start
    if getattr(config, '_CLEAN_STORAGE_AT_START', False):
        storage.delete_container_if_exists(input_container_name)
        storage.delete_container_if_exists(output_container_name)
    storage.ensure_container(input_container_name)
    storage.ensure_container(output_container_name)

    # Create a list of all MP4 files in the InputFiles directory.
    input_file_paths = []

    for folder, subs, files in os.walk(os.path.join(sys.path[0], 'InputFiles')):
        for filename in files:
            if filename.endswith(".mp4"):
                input_file_paths.append(os.path.abspath(
                    os.path.join(folder, filename)))

    # Upload the input files. This is the collection of files that are to be processed by the tasks.
    input_files = [
        upload_file_to_container(storage, input_container_name, file_path)
        for file_path in input_file_paths]
    log.info("Input file count: %d", len(input_file_paths))

    # Obtain a shared access signature URL that provides write access to the output
    # container to which the tasks will upload their output.

    output_container_sas_url = get_container_sas_url_for_write(
        storage,
        output_container_name,
    )

    # Create a Batch service client (AAD or SharedKey)
    batch_url = _normalize_batch_url(getattr(config, '_BATCH_ACCOUNT_URL', ''))
    auth_mode = getattr(config, '_AUTH_MODE', 'SharedKey').upper()
    log.info("Batch URL: %s", batch_url)
    log.info("Auth mode: %s", auth_mode)
    if auth_mode == 'AAD':
        # AAD 認証: 毎リクエスト前にトークンを更新する msrest 互換クレデンシャルを使用
        token_creds = AADTokenCredentials(aad_cred, "https://batch.core.windows.net/.default")
        batch_client = BatchServiceClient(token_creds, batch_url=batch_url)
    else:
        # SharedKey 認証
        creds = batch_auth.SharedKeyCredentials(
            config._BATCH_ACCOUNT_NAME,
            config._BATCH_ACCOUNT_KEY
        )
        batch_client = BatchServiceClient(creds, batch_url=batch_url)

    try:
        # Clean start: delete existing resources (pool deletion is optional)
        _delete_job_if_exists(batch_client, config._JOB_ID)
        if getattr(config, '_DELETE_EXISTING_POOL_AT_START', False):
            _delete_pool_if_exists(batch_client, config._POOL_ID)

        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, config._POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, config._JOB_ID, config._POOL_ID)

        # Add the tasks to the job. Pass the input files and a SAS URL
        # to the storage container for output files.
        add_tasks(batch_client, config._JOB_ID,
                  input_files, output_container_sas_url)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client,
                                   config._JOB_ID,
                                   datetime.timedelta(minutes=30))

        log.info("Success: all tasks reached 'Completed' within the timeout.")

    except batchmodels.BatchErrorException as err:
        log_batch_exception(err)
        raise

    # Delete input container in storage
    storage.delete_container_if_exists(input_container_name)

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    log.info('End time: %s', end_time)
    log.info('Elapsed: %s', end_time - start_time)

    # リソースは残します（ジョブ/プールの削除確認は行いません）
    # 終了プロンプトを廃止
