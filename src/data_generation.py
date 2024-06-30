import random
import pandas as pd
from datetime import datetime, timedelta
import os

def generate_case(tasks, case_id, start_time,min_datapoints,rand=True,connect = True):
    '''
    '''
    case = []
    time = start_time
    case_id=str(case_id)
    case.append(['null','S0',time,'Req',case_id]) #request from the user to S0
    
    num_opts = len([key for key,values in tasks.items() if values[0] == 'opt'])
    
    if rand == True:
        new_tasks = {task:tasks[task] for task in [list(tasks.keys())[i] for i in random.sample(list(range(len(tasks.keys())-num_opts)),random.randint(2, len(tasks.keys())-num_opts))]}
    else:
        new_tasks = tasks
    for task,subtasks in new_tasks.items():
        if subtasks[0] != 'opt':
            server = f"S{list(tasks.keys()).index(task)+1}" #SX
            case.append(['S0',server,time,'Req',case_id])#request from S0 to SX
        if subtasks[0]=='one':
            service_task = random.choice(subtasks[1])
            new_server = f"{server}_{subtasks[1].index(service_task)+1}"
            #Request from SX to SX_Y
            case.append([server, new_server,time,'Req',case_id])
            #Response from SX_Y to SX
            time += timedelta(microseconds=random.randint(subtasks[-1][0], subtasks[-1][1])*100000)
            case.append([new_server,server,time,'Res',case_id]) #1 milisecond= 1000 microseconds
            
            
        elif subtasks[0] == 'rand':
            rand_tasks = random.sample(subtasks[1], random.randint(1, len(subtasks[1])))
            for new_task in rand_tasks:
                new_server = f"{server}_{subtasks[1].index(new_task)+1}"
                case.append([server, new_server,time,'Req',case_id])

                time += timedelta(microseconds=random.randint(subtasks[-1][0], subtasks[-1][1])*100000)
                case.append([new_server,server,time,'Res',case_id])


        elif subtasks[0] == 'all':
            for new_task in subtasks[1]:
                new_server = f"{server}_{subtasks[1].index(new_task)+1}"
                case.append([server, new_server,time,'Req',case_id])
                time += timedelta(microseconds=random.randint(subtasks[-1][0], subtasks[-1][1])*100000)
                case.append([new_server,server,time,'Res',case_id]) 
        
        elif subtasks[0] == 'con' and connect == True:
            service_task = random.choice(subtasks[1])
            new_server = f"{server}_{subtasks[1].index(service_task)+1}"
            case.append([server, new_server,time,'Req',case_id])

            #Opt
            sub_service_task =random.choice(tasks[service_task][1])
            sub_server = f"{new_server}_{list(tasks[service_task][1]).index(sub_service_task)+1}"
            #Req
            case.append([new_server,sub_server,time,'Req',case_id])
            #Res
            time += timedelta(microseconds=random.randint(tasks[service_task][-1][0], tasks[service_task][-1][1])*100000)
            case.append([sub_server,new_server,time,'Res',case_id])
            #Opt end

            case.append([new_server,server,time,'Res',case_id])
        
        elif subtasks[0] == 'opt':
            pass
        
        if subtasks[0] != 'opt':
            case.append([server,'S0',time,'Res',case_id])
    case.append(['S0','null',time,'Res',case_id])#final response from S0 to the user
    return case

def random_time(start_time,end_time):
    delta = end_time - start_time
    return start_time + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def generate_dataset(tasks, min_datapoints,start_time,end_time,random=True,connect = True,file_name = './SGD file.csv'):
    os.makedirs('data', exist_ok=True)
    datapoints = 0
    dataset = []
    user_id = 1
    while datapoints <= min_datapoints:
        time = random_time(start_time,end_time)
        case = generate_case(tasks, user_id, time,min_datapoints, random, connect= connect)
        dataset.extend(case)
        datapoints+= len(case)
        user_id+=1
    
    df = pd.DataFrame(dataset, columns=['from', 'to', 'timestamp', 'type', 'user_id'])
    output_file = "data/SDG_"+file_name+".csv"
    df.to_csv(output_file, index=False)



