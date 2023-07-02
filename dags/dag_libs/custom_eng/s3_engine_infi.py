from infi.clickhouse_orm import Engine
from dataclasses import dataclass
from typing import Optional, List
import logging


@dataclass
class S3Config:
    path: str
    format: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    compression: Optional[str] = None
    SETTINGS: Optional[List[str]] = None

    def get_config_str(self) -> str:
        result = f"""('{self.path}'"""
        if self.aws_access_key_id is not None:
            result += f""", '{self.aws_access_key_id}', '{self.aws_secret_access_key}'"""
        result += f""", '{self.format}'"""
        if self.compression is not None:
            result += f""", '{self.compression}'"""
        result += ") "
        setting_result = ""
        if self.SETTINGS:
            setting_result = ", ".join(self.SETTINGS)
            setting_result = 'SETTINGS ' + setting_result
        return result + setting_result


class S3(Engine):
    def __init__(self, config: S3Config):
        self.config = config
        super(S3, self).__init__()

    def create_table_sql(self, db):

        name = self.__class__.__name__ + self.config.get_config_str()
        logging.info(name)
        return name
