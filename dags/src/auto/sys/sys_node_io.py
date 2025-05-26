import base64
import enum
import json as jsonutil
import pickle
from datetime import datetime as dt
from pathlib import Path
import pandas as pd

import src.auto as auto
import src.auto.env as env
from src.auto.sys import sys_io
from src.auto.util.enum_util import NodePortEnum


def node_write_output(flow_id, node_no, port_no, port_arr, data):
    port_enum: enum
    if port_no == 0:
        port_enum = NodePortEnum.VAR
    else:
        port_str:str = port_arr[port_no][0]
        port_enum = NodePortEnum[port_str]
    match port_enum:
        case NodePortEnum.DAT:  # Data Frame
            __write_data(flow_id, node_no, port_no, data, port_enum)
        case NodePortEnum.MOD:  # Blob Bin
            __write_blob(flow_id, node_no, port_no, data, port_enum)
        case NodePortEnum.VAR | NodePortEnum.HTM | NodePortEnum.IMS:  # Text File
            __write_text(flow_id, node_no, port_no, data, port_enum)
    pass


def __read_data(flow_id, component_port, page_number=None):
    if component_port[0] is None and component_port[1] is None:
        return None
    node_out_id = str(component_port[0]) +"."+ str(component_port[1]).zfill(2)
    node_path = sys_io.get_node_path(flow_id, str(node_out_id).split(".")[0])
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_row = node_path + port_path + env._rows_dat
    pdf = sys_io.read_file_df(file_row, page_number)
    return pdf


def __write_data(flow_id, node_no, port_no, pdf, port_enum:enum.Enum):
    if pdf is None or port_no == 0:
        return

    node_out_id = str(node_no) +"."+ str(port_no).zfill(2)
    node_id = node_out_id.split(".")[0]
    node_path = sys_io.get_node_path(flow_id, node_id)
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    file_row = node_path + port_path + env._rows_dat

    metaData = {
        "source": "write_component_data",
        "flowId": flow_id,
        "outId": node_out_id,
        "portType": port_enum.value,
        "dateTime": dt.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        "count": len(pdf)
    }

    col_names = pdf.columns.tolist()
    col_types = list(pdf.dtypes.astype(str).to_dict().values())

    columns = {
        "names": col_names,
        "types": col_types
    }
    data = {
        'meta_data': metaData,
        'js_column': columns
    }
    data_str = jsonutil.dumps(data)
    sys_io.write_file_txt(file_name=file_head, data=data_str)
    try:
        pdf = pdf.astype({col: "string" for col in pdf.select_dtypes(include=['object']).columns})
        sys_io.write_file_df(file_name=file_row, data=pdf)
    except Exception as e:
        print("to_parquet: ", e)
        raise e

    if Path(file_row).exists():
        print("File Exists: " + file_row)
    else:
        print("File NotFound: " + file_row)
    pass


def __read_blob(flow_id, component_port):
    node_path = sys_io.get_node_path(flow_id, component_port[0])
    node_out_id = str(component_port[0]) +"."+ str(component_port[1]).zfill(2)
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    file_blob = node_path + port_path + env._blob_dat

    headFile = sys_io.read_file_txt(file_head)
    pdfHead = jsonutil.loads(headFile)
    metaData = pdfHead['meta_data']

    blob_data = sys_io.read_file_bin(file_blob)
    if metaData["blobType"] == "str":
        blob_data = blob_data
    return blob_data


def __write_blob(flow_id, node_no, port_no, blob, port_enum:enum.Enum):
    if port_no == 0:
        return

    node_out_id = str(node_no) +"."+ str(port_no).zfill(2)
    node_path = sys_io.get_node_path(flow_id, str(node_out_id).split(".")[0])
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    file_blob = node_path + port_path + env._blob_dat

    blobType = "byte"
    if isinstance(blob, str):
        blobType = "str"
        #blob = blob.encode('utf-8')

    metaData = {
        "source": "write_component_data",
        "flowId": flow_id,
        "outId": node_out_id,
        "portType": port_enum.value,
        "dateTime": dt.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        "blobType": blobType
    }
    data = {
        'meta_data': metaData,
    }
    data_str = jsonutil.dumps(data)
    sys_io.write_file_txt(file_name=file_head, data=data_str)

    if port_enum == NodePortEnum.MOD:
        blob_data = pickle.dumps(blob)
    else:
        blob_data = blob

    sys_io.write_file_blob(file_name=file_blob, data=blob_data, mode="wb")
    pass


def __read_text(flow_id, component_port):
    node_no = int(component_port[0])
    node_out_id = str(component_port[0]) + "."+ str(component_port[1]).zfill(2)
    node_path = sys_io.get_node_path(flow_id, node_no)
    port_path = "/" + str(node_out_id).replace(".", "_")

    # Head (meta veri) dosyasını oku
    file_head = node_path + port_path + env._head_dat
    data_str_head_str = sys_io.read_file_txt(path=file_head)
    data_str_head = jsonutil.loads(data_str_head_str)

    file_vars = node_path + port_path + "_" + data_str_head["meta_data"]["portType"]+".txt"
    data_str_var = sys_io.read_file_txt(path=file_vars)
    return data_str_var


def __write_text(flow_id, node_no, port_no, data, port_enum:enum.Enum):
    node_out_id = str(int(node_no)) + "." + str(port_no).zfill(2)
    node_path = sys_io.get_node_path(flow_id, node_no)
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    head = {
        "meta_data": {
            "portType": port_enum.value
        }
    }
    data_str_head = auto.get_copy_util().to_dictionary(head)
    data_str_head_str = jsonutil.dumps(data_str_head)
    sys_io.write_file_txt(file_name=file_head, data=data_str_head_str)

    file_vars = node_path + port_path + "_" + port_enum.value+".txt"
    sys_io.write_file_txt(file_name=file_vars, data=data)


def get_input_port_args(flow_id:str, def_port_in:list, object_port_in:dict,  # required
                        object_prop:dict|None, con_name: str | None, service_conn): # optional
    lib_call_params = {}
    if service_conn is not None and con_name is not None:  # Connector
        conn_id = object_prop["connector_id"]
        connector_instance = service_conn.conn_get(flow_id, conn_id)
        if connector_instance is None:
            raise Exception("Connector not found or not running : " + str(conn_id))
        lib_call_params[con_name] = connector_instance
    for index in range(len(def_port_in)):
        if not def_port_in[index]:
            continue
        var_port, var_name = def_port_in[index]
        component_port = object_port_in.get("input-" + str(index))
        _input_read = node_read_output(flow_id, component_port, var_port)
        lib_call_params[var_name] = _input_read
    return lib_call_params


def node_read_output(flow_id, component_port, var_port, page_number=None):
    out = None
    match var_port:
        case NodePortEnum.DAT:  # Data Frame
            out = __read_data(flow_id, component_port, page_number)
        case NodePortEnum.MOD:  # Model Bin
            out = pickle.loads(__read_blob(flow_id, component_port))
        case NodePortEnum.VAR | NodePortEnum.HTM | NodePortEnum.IMS:  # Text File
            out = __read_text(flow_id, component_port)
    return out

def replace_nan_with_none(df: pd.DataFrame):
    eligible_cols = df.select_dtypes(include=["object", "float", "category"]).columns
    df[eligible_cols] = df[eligible_cols].astype(object)
    for col in eligible_cols:
        df[col] = df[col].where(pd.notna(df[col]), None)
    return df

def convert_node_out(var_port:str, data):
    out = None
    if data is None:
        return out
    match var_port:
        case NodePortEnum.DAT:  # Data Frame
            if isinstance(data, pd.DataFrame):
                replace_nan_with_none(data)
                out = data.to_dict(orient="split")
                del out['index']
        case NodePortEnum.MOD:  # Model Bin
            out = data
        case NodePortEnum.VAR | NodePortEnum.HTM | NodePortEnum.IMS:  # Text File
            out = data.decode("utf-8")
    return out

def read_node_meta(flow_id, component_port):
    node_path = sys_io.get_node_path(flow_id, component_port[0])
    node_out_id = str(component_port[0]) + "."+ str(component_port[1]).zfill(2)
    port_path = "/"+node_out_id.replace(".","_")
    file_head = node_path + port_path + env._head_dat
    headFile = sys_io.read_file_txt(file_head)
    pdfHead = jsonutil.loads(headFile)
    return pdfHead['meta_data']

def set_output_port_args(flow_id:str, node_no:str, result_tuple:tuple, def_port_out:list, service_conn):
    for index_no in range(len(result_tuple)):
        output = result_tuple[index_no]
        if output is None or index_no >= len(def_port_out):
            continue
        out_port_tuple = (NodePortEnum.VAR,"_")  # out0
        if index_no > 0:
            out_port_tuple = def_port_out[index_no]
        if out_port_tuple[0] == NodePortEnum.CON:
            if service_conn is not None :
                service_conn.conn_add(flow_id, node_no, result_tuple[1])
        else:
            node_write_output(flow_id, node_no, index_no, def_port_out, output)
