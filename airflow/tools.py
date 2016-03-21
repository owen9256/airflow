from airflow.operators import BashOperator, SubDagOperator, DummyOperator, PostgresOperator
from airflow.operators import TimeSensor, SqlSensor, HivePartitionSensor, HdfsSensor, HttpSensor
from airflow.models import DAG, Variable
from datetime import datetime, timedelta, time
import json
import ConfigParser,StringIO

def sensor_factory(sid, config, dagdict):
    default_paras = {
        'TimeSensor':{'poke_interval':60,'timeout':60*60*24,'hour':0, 'minute':0},
        'SqlSensor':{'poke_interval':60,'timeout':60*60*24,'sql':'', 'conn_id':''},
        'HivePartitionSensor':{'poke_interval':60*5,'timeout':60*60*24,'table':'', 'partition':'','metastore_conn_id':'','schema':'default'},
        'HdfsSensor':{'poke_interval':60,'timeout':60*60*24,'filepath':'', 'hdfs_conn_id':'hdfs_default'},
        'HttpSensor':{'poke_interval':60,'timeout':60*60*24,'endpoint':'', 'http_conn_id':'http_default','params':None,'headers':None,'response_check':None}
    }
    c = default_paras[config['type']]
    c.update(config)
    sensor_type = c['type']
    dag = dagdict[c['dag_id']]
    if sensor_type == 'TimeSensor':
        target_time = time(c['hour'], c['minute'])
        return TimeSensor(target_time=target_time, task_id = sid, dag=dag, poke_interval=c['poke_interval'], timeout=c['timeout'])

    elif sensor_type == 'SqlSensor':
        return SqlSensor(sql=c['sql'], conn_id = c['conn_id'], task_id = sid, dag=dag,poke_interval=c['poke_interval'], timeout=c['timeout'])

    elif sensor_type == 'HivePartitionSensor':
        return HivePartitionSensor(table = c['table'], partition = c['partition'], schema= c['schema'], metastore_conn_id = c['metastore_conn_id'], task_id = sid, dag = dag, poke_interval=c['poke_interval'], timeout=c['timeout'])

    elif sensor_type == 'HdfsSensor':
        return HdfsSensor(task_id = sid, dag = dag, filepath = c['filepath'], hdfs_conn_id = c['hdfs_conn_id'],poke_interval=c['poke_interval'], timeout=c['timeout'])

    elif sensor_type == 'HttpSensor':
        return HttpSensor(task_id = sid, dag = dag, endpoint = c['endpoint'], http_conn_id = c['http_conn_id'],poke_interval=c['poke_interval'], timeout=c['timeout'])

        
    
def parse_sensor_config(path, dagdict):
    support_sensor_type = ['TimeSensor', 'SqlSensor', 'HivePartitionSensor', 'HdfsSensor', 'HttpSensor']
    required_fields = {
        'TimeSensor':set(['type', 'dag_id', 'hour', 'minute']),
        'SqlSensor':set(['type', 'dag_id', 'sql', 'conn_id']),
        'HivePartitionSensor':set(['type', 'dag_id', 'table','metastore_conn_id','schema','partition']),
        'HdfsSensor':set(['type', 'dag_id', 'filepath', 'hdfs_conn_id']),
        'HttpSensor':set(['type', 'dag_id', 'endpoint', 'http_conn_id'])
    }

    with open(path,'r') as config_file:
        config = json.load(config_file)
    result = {}

    for k,v in config.items():
        option = v.keys()
        if 'type' not in option:  # type always required
            print "no type in %s"%sensor_id
            continue
        sensor_type = v['type']
        if sensor_type not in support_sensor_type:
            print "%s not supported"%sensor_type
            continue  # only some sensor are supported

        if set(option) < required_fields[sensor_type]:  # type always required
            print "required field %s"%str(required_fields[sensor_type])
            continue

        result[k] = sensor_factory(k, v, dagdict)
    return result


def make_task_group(dagname, name, path, pdag, trigger_rule):
    args = {
        'owner': 'deploy',
        'start_date': datetime.strptime((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'), '%Y-%m-%d')
    }
    dag = DAG(dag_id=dagname, default_args=args, schedule_interval=timedelta(hours=24))
    sdOp = SubDagOperator(task_id=name, subdag=dag, dag=pdag ,trigger_rule = trigger_rule)
    start = DummyOperator(task_id=name+'_start', dag=dag)
    end   = DummyOperator(task_id=name+'_end', dag=dag)
    task_dict = {}
    task_dict[name+'_start'] = start
    task_dict[name+'_end'] = end

    config_str = open(path, 'r').read()
    config_arr = config_str.split('[dependency]') # a -> b means data flow from a to b, though not valid ini format, but i think it is better to understand
    config_fp = StringIO.StringIO(config_arr[0])
    config = ConfigParser.RawConfigParser()
    config.readfp(config_fp)

    sections = config.sections()
    #print sections
    for section in sections:
        #print section
        if section != 'dependency':
            options = config.options( section )    
            #print options
            if 'type' not in options or 'cmd' not in options:
                continue
            operator_type = config.get(section, 'type').strip()
            operator_cmd  = config.get(section, 'cmd').strip()
            if operator_type == 'PostgresOperator':
                task =PostgresOperator(
                    task_id=section.strip(),
                    depends_on_past=False,
                    postgres_conn_id='postgres_sha2dw03',
                    sql=operator_cmd,
                    dag=dag)
                task_dict[ section.strip() ] = task
                task.set_downstream(end)
                task.set_upstream(start)
            elif operator_type == 'BashOperator':
                task =BashOperator(
                    task_id=section.strip(),
                    depends_on_past=False,
                    bash_command=operator_cmd,
                    dag=dag)
                task_dict[ section.strip() ] = task
                task.set_downstream(end)
                task.set_upstream(start)
            else:
                print "Error: currently not support %s operator type"%operator_type
                

    if len(config_arr) == 1:
        return (start, end, dag, sdOp, task_dict)

    for line in config_arr[1].split('\n'):
        arr = line.split('->')
        if len(arr) != 2:
            continue
        left_side = arr[0].strip()
        right_side= arr[1].strip()
        if left_side in task_dict.keys() and right_side in task_dict.keys():
            if end in task_dict[left_side].downstream_list:
                task_dict[left_side].downstream_list.remove(end)
            task_dict[left_side].set_downstream( task_dict[right_side] )
            if start in task_dict[right_side].upstream_list:
                task_dict[right_side].upstream_list.remove(start)
            if task_dict[right_side] in start.downstream_list:
                start.downstream_list.remove(task_dict[right_side])
            if task_dict[left_side] in end.upstream_list:
                end.upstream_list.remove(task_dict[left_side])
    return (start, end, dag, sdOp, task_dict)





def make_subdag(base_path, config_path, dag, base_name, sensor_file_path):
    config_str = open(config_path, 'r').read()
    config_arr = config_str.split('[dependency]') # a -> b means data flow from a to b, though not valid ini format, but i think it is better to understand
    config_fp = StringIO.StringIO(config_arr[0])
    config = ConfigParser.RawConfigParser()
    config.readfp(config_fp)
    #print config.sections()
    #return 1,2
    dag_dict = {base_name:dag}
    subdag_op_dict = {}
    task_dict_all = {}
    is_sensor_init = False
    trigger_rule_all = ['all_success', 'all_failed', 'all_done', 'one_success', 'one_failed','dummy']

    sections = config.sections()
    for section in sections:
        if section != 'dependency':
            options = config.options( section )    
            trigger_rule = 'all_success'
            for o in options:
                o = o.strip()
            if 'ini' not in options:
                continue
            if 'trigger_rule' in options:
                rule = config.get(section, 'trigger_rule').strip()
                if rule in trigger_rule_all:
                    trigger_rule = rule
                 
            title = section.strip()
            path = base_path + '/' + config.get(section, 'ini').strip()
            #print title, trigger_rule
            start,end, subdag, sd, td = make_task_group(base_name+'.'+title, title, path, dag , trigger_rule)
            dag_dict[ title ] = subdag
            task_dict_all[ title ] = td
            subdag_op_dict[ title ] = sd

    if len(config_arr) == 1:
        return dag_dict, subdag_op_dict

    for line in config_arr[1].split('\n'):
         if not is_sensor_init:
             if sensor_file_path is not None:
                 sensor_dict = parse_sensor_config(sensor_file_path, dag_dict)
                 is_sensor_init = True
             else:
                 sensor_dict = {}
                 is_sensor_init = True
         strip_line = line.strip()
         arr = strip_line.split('->')
         if len(arr) == 2:
             left_side = arr[0].strip()
             right_side= arr[1].strip()
             if left_side in subdag_op_dict.keys() and right_side in subdag_op_dict.keys():
                 subdag_op_dict[left_side].set_downstream( subdag_op_dict[right_side] )
             elif left_side in sensor_dict.keys() and right_side in subdag_op_dict.keys():
                 sensor_dict[left_side].set_downstream( subdag_op_dict[right_side] )
             elif left_side in sensor_dict.keys():
                 arr = right_side.split('.')
                 dname = arr[0].strip()
                 tname = arr[1].strip()
                 #print dname,tname,task_dict_all.keys()
                 #if dname in task_dict_all.keys(): 
                 #    print task_dict_all[dname].keys()
                 if dname in task_dict_all.keys() and tname in task_dict_all[dname].keys():
                     sensor_dict[left_side].set_downstream( task_dict_all[dname][tname] )
             else:
                 print 'ERROR, %s -> %s not match '%(left_side, right_side)
    return dag_dict, subdag_op_dict
