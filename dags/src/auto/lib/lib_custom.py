import logging
import traceback
from datetime import datetime

import pandas as pd
import requests
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama.llms import OllamaLLM

from src.auto.util.enum_util import DataFrameEnum

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def py_code(cmp, object_prop, object_vars, **_input_):
    """
    input_1 = _input_.get("input-1") # read input-1
    input_2 = _input_.get("input-2") # read input-2
    _output_.append({"input_cnt_1": len(input_1)}) # write output-1
    _output_.append({"input_cnt_2": len(input_2)}) # write output-2
    """
    _output_ = list()
    _prop_ = object_prop
    _vars_ = object_vars
    pycode = object_prop["code"]
    try:
        exec(pycode)
    except Exception as exc:
        #traceback_str = ''.join(traceback.format_exception(None, exc, exc.__traceback__))
        traceback_str = ''.join(traceback.format_exception_only(type(exc), exc)).strip()
        print(traceback_str)
        raise exc
    _output_ = [_vars_] + _output_
    return tuple(_output_)


def flow_params(cmp, object_prop, object_vars, **_input_):
    merged_vars = {**object_vars, **cmp.params}
    return merged_vars


def var_join(cmp, object_prop, object_vars, **_input_):
    var1 = _input_.get("var1")
    var2 = _input_.get("var2")
    merged_vars = {**object_prop, **var1, **var2}
    return object_vars, merged_vars


def etl_query(cmp, object_prop, object_vars, rdbms_connector):
    query = object_prop["query"]
    df_query = rdbms_connector.execute_query(query)
    return object_vars, df_query


def etl_exec(cmp, object_prop, object_vars, rdbms_connector):
    query = object_prop["query"]
    df_query = rdbms_connector.execute_update(query)
    return object_vars, df_query


def ai_model(cmp, object_prop, object_vars, df):
    lang = object_prop["lang"]
    if lang is None:
        raise Exception("dil alanı boş olamaz")
    if lang != "python":
        raise Exception("unsupported :" + lang)
    df_type = object_prop["df_type"]
    if df_type is None:
        raise Exception("df type boş olamaz: [pandas|spark]")
    if df_type != DataFrameEnum.pandas:
        raise Exception("unsupported :" + df_type)
    function_def = object_prop["function_def"]
    function_name = object_prop["function_name"]
    new_column_name = object_prop["new_column_name"]
    exec(function_def)
    df[new_column_name] = df.apply(eval(function_name), axis=1)
    return object_vars, df


def ws_call(cmp, object_prop, object_vars):
    # Web servisini çağıran genel metod.
    method = object_prop["ws_method"]
    url = object_prop["ws_url"]
    data = object_prop["ws_data"]
    params = object_prop["ws_params"]
    headers = object_prop["ws_headers"]
    timeout = 600
    """
    :param method: HTTP metodu (GET, POST, PUT, DELETE)
    :param url: API'nin URL'si
    :param data: Body'ye gönderilecek JSON verisi (POST/PUT için)
    :param params: URL parametreleri (GET için)
    :param headers: HTTP başlıkları
    :param timeout: İstek zaman aşımı süresi (varsayılan: 600 saniye)
    :return: JSON yanıt veya hata mesajı
    """
    method = method.upper()
    valid_methods = {"GET", "POST", "PUT", "DELETE"}

    if method not in valid_methods:
        logging.error(f"Geçersiz HTTP metodu: {method}")
        return object_vars, {"error": f"Invalid HTTP method: {method}"}
    try:
        logging.info(f"İstek: {method} {url} | Headers: {headers} | Params: {params} | Body: {data}")
        response = requests.request(method, url, json=data, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()  # HTTPError fırlatır (4xx, 5xx hatalarında)
        logging.info(f"Yanıt ({response.status_code}): {response.json()}")
        return object_vars, response.json()  # JSON formatında döndür
    except requests.exceptions.RequestException as e:
        logging.error(f"İstek hatası: {e}")
        return object_vars, {"error": str(e)}


def df_reader(cmp, object_prop, object_vars):
    json_df = object_prop["json_df"]
    pdf = pd.DataFrame(json_df)
    object_vars.update({"json_df": json_df})
    return object_vars, pdf


def llm_call(cmp, object_prop, object_vars):
    question = object_prop["prompt"]
    temperature = object_prop["temperature"]

    print("LLM-Start:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    response_text = get_ollama_response(question, temperature)
    print("LLM-Finish:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    result = {
        'prompt': [question],
        "response": [response_text]
    }
    return object_vars, result


def get_ollama_response(cmp, question, temperature):
    template = """
        Question: {question}
        Answer: Let's think step by step. Return in Turkish Language.
    """
    prompt = ChatPromptTemplate.from_template(template)
    model = OllamaLLM(model="llama3.1", temperature=temperature)
    chain = prompt | model
    response_text = chain.invoke({"question": question})
    return response_text

def table_creator(cmp, object_prop, object_vars):
    df1 = pd.DataFrame(columns=object_prop["column_names"], data=object_prop["table_data"])
    for i, column in enumerate(df1.columns):
        df1[column].astype(object_prop["column_types"][i])
    return object_vars, df1


def user_test(cmp, object_prop, object_vars):
    return object_vars