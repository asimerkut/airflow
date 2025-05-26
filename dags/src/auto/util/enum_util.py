from enum import IntEnum, StrEnum


class NodeStateEnum(IntEnum):
    start = 0
    confg = 1
    exect = 2
    error = 3


class PropTypeEnum(StrEnum):
    str = "str"
    int = "int"
    bool = "bool"
    float = "float"
    json = "json"
    array = "array"
    object = "object"


class FsTypeEnum(StrEnum):
    lfs = "lfs"
    s3fs = "s3fs"


class DataFrameEnum(StrEnum):
    pandas = "pandas"
    spark = "spark"


class ConnectorEnum(StrEnum):
    filesys_connector = "filesys_connector"
    rdbms_connector = "rdbms_connector"


class NodePortEnum(StrEnum):
    CON = "CON"  # Connection
    DAT = "DAT"  # Data
    VAR = "VAR"  # Variable
    MOD = "MOD"  # Model
    IMS = "IMS"  # Image/Svg
    HTM = "HTM"  # HTML


class NodePlaceEnum(StrEnum):
    StartNode = "StartNode"
    FinishNode = "FinishNode"
    ConnNode = "ConnNode"


class KeyEnum(StrEnum):
    SLOCA = "SLOCA"
    SGRUP = "SGRUP"
    SFEAT = "SFEAT"
    SCLAS = "SCLAS"
    SMODL = "SMODL"
    STURU = "STURU"
    STERM = "STERM"

    ELOCA = "ELOCA"
    EGRUP = "EGRUP"
    EFEAT = "EFEAT"
    ECLAS = "ECLAS"
    EMODL = "EMODL"
    ETURU = "ETURU"
    ETERM = "ETERM"
