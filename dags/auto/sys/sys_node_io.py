import base64
import json as jsonutil
import pickle
from datetime import datetime as dt
from pathlib import Path

import dags.auto as auto
import dags.auto.env as env
from dags.auto.sys import sys_io
from dags.auto.util.enum_util import NodePortEnum


def write_output(flow_id, node_no, port_no, port_arr, data):
    if port_no == 0:
        port_enum = NodePortEnum.VAR
    else:
        port_enum = port_arr[port_no][0]
    match port_enum:
        case NodePortEnum.DAT:  # Data Frame
            write_data(flow_id, node_no, port_no, data, port_enum)
        case NodePortEnum.VAR | NodePortEnum.HTM:  # Text File
            write_text(flow_id, node_no, port_no, data, port_enum)
        case NodePortEnum.MOD | NodePortEnum.IMP | NodePortEnum.IMS:  # Blob Bin
            write_blob(flow_id, node_no, port_no, data, port_enum)
    pass


def read_data(flow_id, component_port):
    if component_port[0] is None and component_port[1] is None:
        return None
    node_out_id = float(component_port[0]) + float(component_port[1] / 100)
    node_path = sys_io.get_node_path(flow_id, str(node_out_id).split(".")[0])
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_row = node_path + port_path + env._rows_dat
    pdf = sys_io.read_file_df(file_row)
    return pdf


def write_data(flow_id, node_no, port_no, pdf, port_enum):
    if pdf is None or port_no == 0:
        return

    node_out_id = str(float(node_no) + float(port_no / 100))
    node_id = node_out_id.split(".")[0]
    node_path = sys_io.get_node_path(flow_id, node_id)
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    file_row = node_path + port_path + env._rows_dat

    metaData = {
        "source": "write_component_data",
        "flowId": flow_id,
        "outId": node_out_id,
        "portType": port_enum,
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


def read_blob(flow_id, component_port):
    node_path = sys_io.get_node_path(flow_id, component_port[0])
    node_out_id = float(component_port[0]) + float((component_port[1] / 100))
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    file_blob = node_path + port_path + env._blob_dat

    headFile = sys_io.read_file_txt(file_head)
    pdfHead = jsonutil.loads(headFile)
    metaData = pdfHead['meta_data']

    blob_data = sys_io.read_file_bin(file_blob)
    if metaData["blobType"] == "str":
        blob_data = blob_data.decode('utf-8')
    return blob_data


def write_blob(flow_id, node_no, port_no, blob, port_enum):
    if port_no == 0:
        return

    node_out_id = str(float(node_no) + float(port_no / 100))
    node_path = sys_io.get_node_path(flow_id, str(node_out_id).split(".")[0])
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    file_blob = node_path + port_path + env._blob_dat

    blobType = "byte"
    if isinstance(blob, str):
        blobType = "str"
        blob = blob.encode('utf-8')

    metaData = {
        "source": "write_component_data",
        "flowId": flow_id,
        "outId": node_out_id,
        "portType": port_enum,
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


def read_text(flow_id, component_port):
    node_no = int(component_port[0])
    node_out_id = "{:.2f}".format(int(component_port[0]) + (component_port[1] / 100))

    node_path = sys_io.get_node_path(flow_id, node_no)
    port_path = "/" + str(node_out_id).replace(".", "_")

    # Head (meta veri) dosyasını oku
    file_head = node_path + port_path + env._head_dat
    data_str_head_str = sys_io.read_file_txt(path=file_head)
    data_str_head = jsonutil.loads(data_str_head_str)

    file_vars = node_path + port_path + env._vars_dat
    data_str_var = sys_io.read_file_txt(path=file_vars)
    return_dict = jsonutil.loads(data_str_var)
    return return_dict


def write_text(flow_id, node_no, port_no, data, port_enum):
    node_out_id = str(int(node_no)) + "." + str(port_no).zfill(2)
    node_path = sys_io.get_node_path(flow_id, node_no)
    port_path = "/" + str(node_out_id).replace(".", "_")
    file_head = node_path + port_path + env._head_dat
    head = {
        "meta_data": {
            "portType": port_enum
        }
    }
    data_str_head = auto.get_copy_util().to_dictionary(head)
    data_str_head_str = jsonutil.dumps(data_str_head)
    sys_io.write_file_txt(file_name=file_head, data=data_str_head_str)

    file_vars = node_path + port_path + env._vars_dat
    data_str_var = auto.get_copy_util().to_dictionary(data)
    data_str_var_str = jsonutil.dumps(data_str_var)
    sys_io.write_file_txt(file_name=file_vars, data=data_str_var_str)


def get_input_port_args(flow_id:str, def_port_in:list, object_port_in:dict, object_prop:dict, connector_field:str, service_conn):
    lib_function_params = {}
    if connector_field is not None:  # Connector
        conn_id = object_prop["connector_id"]
        connector = service_conn.conn_get(flow_id, conn_id)
        lib_function_params[connector_field] = connector
    for index in range(len(def_port_in)):
        if not def_port_in[index]:
            continue
        var_port, var_name = def_port_in[index]
        var_param = object_port_in.get("input-" + str(index))
        _input_read = None
        match var_port:
            case NodePortEnum.DAT:  # Data Frame
                _input_read = read_data(flow_id, var_param)
            case NodePortEnum.VAR | NodePortEnum.HTM:  # Text File
                _input_read = read_text(flow_id, var_param)
            case NodePortEnum.MOD:  # Model Bin
                _input_read = pickle.loads(read_blob(flow_id, var_param))
            case NodePortEnum.IMP:  # Image Png Bin
                _input_read = base64.b64decode(read_blob(flow_id, var_param))
            case NodePortEnum.IMS:  # Image Svg Bin
                _input_read = read_blob(flow_id, var_param)
        lib_function_params[var_name] = _input_read
        # for:def_port_in
    return lib_function_params

def set_output_port_args(flow_id:str, node_no:str, result_tuple:tuple, def_port_out:list, service_conn, base_node):
    for index_no in range(len(result_tuple)):
        output = result_tuple[index_no]
        if output is None or index_no >= len(def_port_out):
            continue
        out_port_tuple = (NodePortEnum.VAR,"_")  # out0
        if index_no > 0:
            out_port_tuple = def_port_out[index_no]
        if out_port_tuple[0] == NodePortEnum.CON:
            base_node.connector_instance = output # if Cmp is Connector
            service_conn.conn_add(flow_id, node_no, base_node.connector_instance)
        else:
            write_output(flow_id, node_no, index_no, def_port_out, output)