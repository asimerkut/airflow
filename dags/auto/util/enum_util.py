class FsTypeEnum:
    lfs = "lfs"
    s3fs = "s3fs"


class LibEnum:
    LibAi = "LibAi"
    LibIoRdbms = "LibIoRdbms"
    LibIoFilesys = "LibIoFilesys"
    LibDim = "LibDim"
    LibIoUser = "LibIoUser"
    LibEtl = "LibEtl"
    LibStat = "LibStat"
    LibSystem = "LibSystem"
    LibCustom = "LibCustom"
    LibVis = "LibVis"
    LibConnector = "LibConnector"


class DataFrameEnum:
    pandas = "pandas"
    spark = "spark"


class ConnectorEnum:
    filesys_connector = "filesys_connector"
    rdbms_connector = "rdbms_connector"


class NodePortEnum:
    CON = "CON"  # Connection
    DAT = "DAT"  # Data
    VAR = "VAR"  # Variable
    MOD = "MOD"  # Model
    IMP = "IMP"  # Image/Png
    IMS = "IMS"  # Image/Svg
    HTM = "HTM"  # HTML


class NodeStateEnum:
    start = 0
    confg = 1
    exect = 2
    error = 3


class NodePlaceEnum:
    StartNode = "StartNode"
    FinishNode = "FinishNode"
    ConnNode = "ConnNode"


class KeyEnum:
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
