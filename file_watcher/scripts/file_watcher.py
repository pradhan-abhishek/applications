import argparse
import fcntl
import os
import re
import shutil
import time
import uuid
from multiprocessing import set_start_method
from typing import Callable

from google.cloud import storage

from logger import get_logger


def input_path(source_path: str) -> bool:
    if "file_type" in source_path.lower():
        return True
    else:
        return False


class FileManager:
    def __init__(self, src_path: str, target_bucket: str, archive_path: str, sva_key_path: str,
                 logging_project: str, file_type: str, sleep_timer_s=1):
        self.src_path = os.path.join(src_path, "")
        self.archive_path = os.path.join(archive_path, "")
        self.target_bucket = target_bucket
        self._build_gcs_client(sva_key_path)
        self.logger = get_logger(logging_project, "file_watcher")
        self.sleep_timer_s = sleep_timer_s
        self.file_type = file_type

    def _build_gcs_client(self, sva_key_path: str):
        self.storage_client = storage.Client.from_service_account_json(sva_key_path)
        self.target_bucket_client = self.storage_client.bucket(self.target_bucket)

    def _extract(self, file_path: str, extr_f, gt_l: int) -> str:
        if not file_path:
            return None

        ps = file_path.replace(self.src_path, "").split("/")
        if ps and len(ps) > gt_l:
            p = extr_f(ps)
            if p:
                return p

        return None

    def _extract_org_path(self, file_path: str) -> str:
        if file_path and self.file_type is not None:
            self.logger.debug(f"self.file_type is not None, returning file_type of extracting it from {file_path}")
            return self.file_type
        return self._extract(file_path, lambda s: s[0], gt_l=1)

    def _extract_filepath_from_org(self, file_path: str) -> str:
        s = self._extract(file_path, lambda s: "/".join(s[1:]), gt_l=0)
        if s:
            return s
        return self._extract(file_path, lambda s: "/".join(s), gt_l=0)

    def _replace_extensions(self, file_name: str) -> (str, str):
        if not file_name:
            return None

        gz_r = r"\..+\.gz"
        other_r = r"\.\w+$"
        match = re.findall(gz_r, file_name)

        if len(match) > 0:
            return re.sub(gz_r, "", file_name), match[0]

        if file_name.endswith(".gz"):
            return file_name[:-3], ".gz"

        match = re.findall(other_r, file_name)
        if len(match) > 0:
            return re.sub(other_r, "", file_name), match[0]

        return None

    def _add_uuid_to_filename(self, file_path: str) -> str:
        if not file_path:
            return None
        generated_uuid = uuid.uuid4().hex

        file_name = os.path.basename(file_path)
        new_file_path = os.path.dirname(file_name)
        name_wo_ext, ext = self._replace_extensions(new_file_path)

        return f"{new_file_path}/{name_wo_ext}_{generated_uuid}-{ext}"

    def _build_tgt_path(self, file_path: str) -> str:
        org = self._extract_org_path(file_path)
        if not org:
            self.logger.warning(f"Cannot find the organisation for {file_path}")
            pass
            return self._extract_filepath_from_org(file_path)
        else:
            return org + "/" + self._extract_filepath_from_org(file_path)

    def _gcs_file_exists(self, prefix: str) -> bool:
        return storage.Blob(bucket=self.target_bucket_client, name=prefix).exists(self.storage_client)

    def upload_to_gcs(self, fd, object, tgt_path=None) -> bool:
        current_path = fd.name
        if not tgt_path:
            tgt_path = self._build_tgt_path(current_path)
        self.logger.debug(f"Checking if {tgt_path} exists on {self.target_bucket}")

        if self._gcs_file_exists(tgt_path):
            self.logger.warning(f"{tgt_path} exists on {self.target_bucket}")
            return self.upload_to_gcs(fd, self._add_uuid_to_filename(tgt_path))

        try:
            self.logger.debug(f"Uploading {current_path} to {tgt_path}")
            blob_object = self.target_bucket_client.blob(tgt_path)
            blob_object.upload_from_file(fd)
            return True
        except Exception as e:
            self.logger.exception(e)
            self.logger.error(f"Issue with the file upload from {current_path} to {tgt_path}: {e}")
            return False

    def archive_file(self, file_path: str) -> str:
        archive_suffix = self._build_tgt_path(file_path)
        tgt_archive_path = os.path.join(self.archive_path, os.path.dirname(archive_suffix))
        os.makedirs(tgt_archive_path, exist_ok=True)
        tgt_archive_path = os.path.join(tgt_archive_path, os.path.basename(file_path))
        try:
            shutil.move(file_path, tgt_archive_path)
            self.logger.info(f"File successfully archive from {file_path} to {tgt_archive_path}")
            return tgt_archive_path
        except Exception as e:
            self.logger.error(f"Issue with file archival process for {file_path}: {e}")
        return None

    def _file_lock(self, file_path: str, func: Callable[[str], None]) -> Exception:
        err = None
        try:
            fd = open(file_path, "rb+")
        except Exception as e:
            self.logger.exception(e)
            self.logger.error(f"Cannot acquire lock for {file_path}: {e}")
            self.logger.info(f"File will be processed later")
            return e

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            func(fd)
        except BlockingIOError as e:
            self.logger.exception(e)
            self.logger.error(f"Cannot acquire lock for {file_path}: {e}")
            self.logger.info(f"File will be processed later")
            err = e
        except Exception as e:
            self.logger.exception(e)
            self.logger.error(f"Cannot acquire lock for {file_path}: {e}")
            self.logger.info(f"File will be processed later")
            err = e
        finally:
            fcntl.flock(fd.fcntl.LOCK_UN)
            fd.close()
        return err

    def _handle_event(self, fd: object):
        event_path = fd.name
        try:
            if os.stat(event_path).st_size == 0:
                self.logger.warning(f"Empty file {event_path}")
                return
        except Exception as e:
            self.logger.error(e)
            return

        if self.archive_path in event_path:
            self.logger.warning("Ignoring archival events")
            return

        if not input_path(event_path):
            self.logger.warning("Invalid path")
            return

        self.logger.info(f"Uploading to GCS {event_path}")
        is_uploaded = self.upload_to_gcs(fd)
        if is_uploaded:
            self.logger.info(f"Archiving {event_path} to {self.archive_path}")
            self.archive_file(event_path)
        else:
            self.logger.error(f"Cannot archive, upload failed")

    def handle_event(self, event_path: str):
        return self._file_lock(event_path, self._handle_event)

    def _filter_filename(self, file_path: str) -> bool:
        return (file_path and (re.match(r".+\..+$", file_path)) and
                (self.archive_path not in file_path) and file_path.startswith("."))

    def _process_all_files(self):
        file_list = list()
        for root, dirs, files in os.walk(self.src_path):
            for filename in files:
                file_list.append(root.replace("\\", "/") + f"/{filename}")

        for file_path in file_list:
            if self._filter_filename(file_path):
                yield self.handle_event(file_path)
        return

    def __ec__(self):
        pass

    def start(self):
        while True:
            try:
                for e in self._process_all_files():
                    if e:
                        self.logger.exception(e)
                time.sleep(self.sleep_timer_s)
                self.__ec__()
            except InterruptedError as e:
                break
            except Exception as e:
                self.logger.exception(e)


def main(src_path: str, target_bucket: str, archive_path: str, sva_key_path: str,
         logging_project: str, file_type: str):
    fm = FileManager(
        src_path=src_path,
        target_bucket=target_bucket,
        archive_path=archive_path,
        sva_key_path=sva_key_path,
        logging_project=logging_project,
        file_type=file_type
    )

    if not os.path.exists(src_path):
        raise FileNotFoundError("Source is invalid")

    if not os.path.exists(sva_key_path):
        raise FileNotFoundError("SVA path is invalid")

    fm.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python code for movement of files from windows system to GCS bucket")

    parser.add_argument("-s", "--src_path", dest="src_path", type=str, required=True)
    parser.add_argument("-t", "--target_bucket", dest="target_bucket", type=str, required=True)
    parser.add_argument("-a", "--archive_path", dest="archive_path", type=str, required=True)
    parser.add_argument("-l", "--logging_project", dest="logging_project", type=str, required=True)
    parser.add_argument("-f", "--file_type", dest="file_type", type=str, required=False, default=None)

    args = parser.parse_args()

    set_start_method("spawn")

    sva_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sva_path:
        raise Exception("GOOGLE APPLICATION CREDENTIALS is required")

    main(src_path=os.path.join(args.src_path, ""),
         archive_path=args.archive_path,
         target_bucket=args.target_bucket,
         sva_key_path=sva_path,
         logging_project=args.logging_project,
         file_type=args.file_type)
