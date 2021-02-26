import datetime
import math
import os
import re
import tools_util as util
import psycopg2

APPLICATION_DEF_PATH = "/opt/nsn/ngdb/ifw/lib/application/utils" \
                       "/application_definition.sh"
specification_dict = {}
kpi_formula_dict = {}
index_kpi_list = []
reg_expression_enclosed_hash = "(\\#)([A-Za-z0-9_\\-.]+)(\\#)"
reg_expression_prefixed_hash = "(\\#)([A-Za-z0-9_\\-.]+)"
interval_type_map = {"HOUR": "1", "DAY": "2", "WEEK": "3", "MONTH": "4"}
spec_formula_query = "select indicatorspec_id, pi_spec_name, " \
                     "derivationmethod, derivationalgorithm from " \
                     "sairepo.perf_indi_spec where id in (select " \
                     "perfindicatorspecid from " \
                     "sairepo.perf_spec_attributes_use where perfspecid " \
                     "= (select id from sairepo.perf_spec where specid " \
                     "='SPEC' and version = (select max(version) from " \
                     "sairepo.deployment_dictionary where object_id " \
                     "='SPEC')) and perfindicatorspecid is not null and " \
                     "intervalid=INTERVAL);"
kpi_formula_query = "select kpi_name, formula from sairepo.kpi_formula " \
                    "where interval_id=INTERVAL and kpi_id in (" \
                    "KPI_ID_LIST)"
usage_formula_query = "select kpi_name,kpi_definition,average_value from " \
                      "saidata.es_usage_kpi_avg_value_1 where " \
                      "interval_id='INTERVAL'"
entity_parameters_query = "select variable_name, query, default_value," \
                          "target_db from " \
                          "sairepo.entity_dynamic_parameters;"


def hash_replace_query(yaml, lower_bound, upper_bound, job_name):
    return _start_hash_replacement(yaml, lower_bound, upper_bound,
                                   job_name)


def hash_replace_udf(output_yaml, job_name):
    if "table_udf_mapping" in output_yaml.keys():
        for key in output_yaml["table_udf_mapping"].keys():
            for inner_key in output_yaml["table_udf_mapping"][key].keys():
                query = util.get_new_lines_replaced_query(
                        output_yaml['table_udf_mapping'][key][
                            inner_key])
                output_yaml['table_udf_mapping'][key][
                    inner_key] = replace_hash(query, job_name)
    return output_yaml


def _start_hash_replacement(output_yaml, lower_bound, upper_bound,
                            job_name):
    if output_yaml['sql']['query']:
        query = util.get_new_lines_replaced_query(output_yaml['sql']['query'])
        output_yaml['sql']['query'] = replace_hash(query, job_name)
        output_yaml['sql']['query'] = replace_lb_ub(
                output_yaml['sql']['query'],
                lower_bound,
                upper_bound)
    return output_yaml

def _get_hash_list(reg_exp, query):
    hash_list = re.findall(reg_exp, query)
    hash_kpi_list = []
    for hash_element in hash_list:
        hash_entry = ''.join(hash_element)
        if hash_entry not in hash_kpi_list:
            hash_kpi_list.append(hash_entry)
    return hash_kpi_list


def _parse_job_name(job_name):
    job_id = job_name.replace("Perf_", "").replace("_AggregateJob",
                                                   "").replace(
            "_ReaggregateJob", "")
    job_split_values = job_id.split('_')
    interval = job_split_values[-1]
    spec_name = job_id.replace("_1_" + interval, "")
    return spec_name, interval


def get_sdk_db_url():
    if os.environ.get('IS_K8S'):
        return os.environ.get('PROJECT_SDK_DB_URL')
    else:
        file = open(APPLICATION_DEF_PATH, 'r')
        exec(file.read().replace(
                "(", "(\"").replace(")", "\")"), globals())
        file.close()
        return project_application_sdk_db_url


def get_sdk_db_param(url):
    param = dict()
    param["PORT"] = url.split(':')[-1].split('/')[0]
    param["HOST"] = url.split('//')[-1].split(':')[0]
    param["DATABASE"] = url.split('/')[-1].split('?')[0]
    return param


def get_cursor(query):
    param = get_sdk_db_param(get_sdk_db_url())
    connection = psycopg2.connect(database=param["DATABASE"],
                                  user="postgres",
                                  password="", host=param["HOST"],
                                  port=param["PORT"])
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor, connection


def set_specification_kpi_formula(spec_name, interval):
    global spec_formula_query
    spec_formula_query = spec_formula_query.replace("SPEC",
                                                    spec_name).replace(
            "INTERVAL", interval)
    cursor, connection = get_cursor(spec_formula_query)
    rows = cursor.fetchall()
    for row in rows:
        formula_dict = {
            'indicator_id': row[0], 'formula': row[2],
            'algorithm': row[3]
            }
        specification_dict[row[1]] = formula_dict
    connection.close()


def _is_kpi_an_index_subtype(kpi_name, formula_value):
    return kpi_name.endswith('_INT') or kpi_name.endswith(
            '_CONT') or kpi_name.endswith(
            '_WEIGHT') or formula_value == '$formula$'


def _get_index_kpis():
    for kpi_name, dict in specification_dict.items():
        if _is_kpi_an_index_subtype(kpi_name, dict['formula']):
            index_kpi_list.append(dict['indicator_id'])
            if (dict['formula'] == '$formula$' and not kpi_name.endswith(
                    '_CONT')):
                index_kpi_list.append(dict['indicator_id'] + '_BUCKET')
        else:
            if kpi_name + '_CONT' in specification_dict.keys():
                index_kpi_list.append(dict['indicator_id'] + '_INT')
            global kpi_formula_dict
            kpi_formula_dict['#' + kpi_name + '#'] = dict['formula']
    index_kpis = index_kpi_list.__str__()
    index_kpis = index_kpis.rstrip(']')
    index_kpis = index_kpis.lstrip('[')
    return index_kpis


def _update_kpi_formula_dict(index_kpi_names, interval):
    global kpi_formula_query
    kpi_formula_query = kpi_formula_query.replace('INTERVAL',
                                                  interval).replace(
            'KPI_ID_LIST', index_kpi_names)
    cursor, connection = get_cursor(kpi_formula_query)
    rows = cursor.fetchall()
    global kpi_formula_dict
    for row in rows:
        kpi_formula_dict['#' + row[0] + '#'] = row[1]
    connection.close()


def _remove_white_spaces(query):
    replaced_query = query.replace("''", "'")
    replaced_query = replaced_query.replace('\n', ' ')
    replaced_query = replaced_query.replace('\t', ' ')
    replaced_query = replaced_query.replace('\r', ' ')
    return replaced_query


def _get_updated_query(hash_kpi_list, query):
    for hash_kpi in hash_kpi_list:
        if hash_kpi in kpi_formula_dict:
            formula = kpi_formula_dict[hash_kpi]
            query = query.replace(hash_kpi, formula)
    return _remove_white_spaces(query)


def _get_usage_kpi_dict(interval):
    usage_kpi_dict = {}
    global usage_formula_query
    usage_formula_query = usage_formula_query.replace('INTERVAL',
                                                      interval)
    cursor, connection = get_cursor(usage_formula_query)
    rows = cursor.fetchall()
    for row in rows:
        usage_prop_dict = {
            'formula': row[1], 'avg_value': row[2]
            }
        usage_kpi_dict[row[0]] = usage_prop_dict
    connection.close()
    return usage_kpi_dict


def _get_usage_kpi_updated_query(query, interval):
    hash_usage_list = _get_hash_list(reg_expression_enclosed_hash, query)
    usage_kpi_dict = _get_usage_kpi_dict(interval)
    for usage_kpi in hash_usage_list:
        if usage_kpi.startswith('#US_BASE_WEIGHT_'):
            usage_kpi_suffix = usage_kpi.replace('#US_BASE_WEIGHT_',
                                                 '').replace('#', '')
            usage_formula_dict = usage_kpi_dict[usage_kpi_suffix]
            query = query.replace(usage_kpi, usage_formula_dict['formula'])
        if usage_kpi.startswith('#AVG_USAGE_'):
            usage_kpi_suffix = usage_kpi.replace('#AVG_USAGE_',
                                                 '').replace(
                    '#', '')
            usage_formula_dict = usage_kpi_dict[usage_kpi_suffix]
            avg_value = usage_formula_dict['avg_value']
            avg_value = 'null' if avg_value is None or len(
                    avg_value) == 0 else avg_value
            query = query.replace(usage_kpi, avg_value)
    return query


def _get_normalized_threshold(tv):
    return 0.001 if tv == 0 else tv


def _get_calculated_coefficient(lt, ut, tv):
    normalized_tv = _get_normalized_threshold(tv)
    return ((math.log((100.0 / normalized_tv) - 0.983) + 4.074542) / (
            ut - lt))


def _replace_coefficients(query):
    hash_coefficient_list = _get_hash_list(reg_expression_enclosed_hash,
                                           query)
    for coefficient in hash_coefficient_list:
        coefficient_without_hash = coefficient.replace('#', '')
        coefficient_array = coefficient_without_hash.split('_')
        coefficient_value = _get_calculated_coefficient(
                float(coefficient_array[1]), float(coefficient_array[2]),
                float(coefficient_array[3])).__str__()
        query = query.replace(coefficient, coefficient_value)
    return query


def _replace_enclosed_hash(query, job_name):
    hash_kpi_list = _get_hash_list(reg_expression_enclosed_hash, query)
    updated_query = query
    if len(hash_kpi_list) != 0:
        spec_name, interval = _parse_job_name(job_name)
        set_specification_kpi_formula(spec_name,
                                      interval_type_map[interval])
        index_kpi_names = _get_index_kpis()
        _update_kpi_formula_dict(index_kpi_names,
                                 interval_type_map[interval])
        updated_query = _get_updated_query(hash_kpi_list, query)
        updated_query = _get_usage_kpi_updated_query(updated_query,
                                                     interval)
    return _replace_coefficients(updated_query)


def _get_entity_parameters_dict():
    cursor, connection = get_cursor(entity_parameters_query)
    rows = cursor.fetchall()
    entity_params_dict = {}
    for row in rows:
        param_prop_dict = {
            'query': row[1], 'default_value': row[2],
            'target_db': row[3]
            }
        entity_params_dict[row[0]] = param_prop_dict
    connection.close()
    return entity_params_dict


def _get_entity_values_postgres(entity_query):
    entity_values = []
    cursor, connection = get_cursor(entity_query)
    rows = cursor.fetchall()
    for row in rows:
        entity_values.append(str(row[0]))
    connection.close()
    return entity_values


def _get_entity_values_dict(entity_params_dict):
    entity_values_dict = {}
    for entity_name, dict in entity_params_dict.items():
        values = []
        if dict['default_value']:
            values.append(dict['default_value'])
        elif dict['target_db'].upper() == 'POSTGRES':
            values = _get_entity_values_postgres(dict['query'])
        entity_values_dict[entity_name] = values
    return entity_values_dict


def _replace_prefixed_hash(updated_query):
    hash_prefixed_list = _get_hash_list(reg_expression_prefixed_hash,
                                        updated_query)
    entity_params_dict = _get_entity_parameters_dict()
    entity_values_dict = _get_entity_values_dict(entity_params_dict)
    for prefixed_entry in hash_prefixed_list:
        if prefixed_entry in entity_values_dict:
            entity_param_value = entity_values_dict[
                prefixed_entry].__str__()
            entity_param_value = entity_param_value.rstrip(']')
            entity_param_value = entity_param_value.lstrip('[')
            entity_param_value = entity_param_value.replace("\'", "")
            updated_query = updated_query.replace(prefixed_entry,
                                                  entity_param_value)
    return updated_query


def replace_hash(query, job_name):
    updated_query = _replace_enclosed_hash(query, job_name)
    return _replace_prefixed_hash(updated_query)


def replace_lb_ub(query, lb, ub):
    lb_date = datetime.datetime.fromtimestamp(float(lb) / 1000.0).strftime(
            "%Y-%m-%d %H:%M:%S.%f")
    ub_date = datetime.datetime.fromtimestamp(float(ub) / 1000.0).strftime(
            "%Y-%m-%d %H:%M:%S.%f")
    query = query.replace("#LOWERBOUND_DATE", lb_date)
    query = query.replace("#UPPERBOUND_DATE", ub_date)
    query = query.replace("#LOWER_BOUND", str(lb))
    query = query.replace("#UPPER_BOUND", str(ub))
    return query
