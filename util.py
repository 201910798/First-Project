import numpy as np
import sys
import heapq
import random

class DistancePair(object):
    def __init__(self, distance, node):
        self.distance = distance
        self.node = node
    
    def __lt__(self, other):
        return self.distance < other.distance
    def __eq__(self, other):
        return self.distance == other.distance

def dijkstra(topo, start):
    #topo: topology, Class: Topoloy
    #start: Dijkstra's 알고리즘 시작 노드, Class: Node
    #link cost는 1로 통일
    distance = {node: sys.maxsize for node in topo.nodes.keys()}
    distances = {node: sys.maxsize for node in topo.nodes.keys()}
    routes = {node: sys.maxsize for node in topo.nodes.keys()}
    distance[start] = 0
    distances[start] = 0
    routes[start] = start
    queue = []
    
    ##여기에 코드 추가
    queue = [node for node in topo.nodes.keys()]
    # a = 1
    # print(a)
    while queue:
        u = min(distance, key=lambda x: distance[x]) 
        distance.pop(u)
        # print(u)
        queue.remove(u)
        for neighbor in topo.links[u].keys():  
            temp = distances[u] + 1 
            if temp < distances[neighbor]:
                distances[neighbor] = temp
                distance[neighbor] = temp
                routes[neighbor] = u
    # print("----Distance----")
    # print(distance)
    # print("------------------")
    # print("----Routes----")
    # print(routes)
                       
    return distances, routes

def shortest(start, end, routes, path):
    #start, end: 소스와 목적지, class: Node
    #routes: dijkstra function 결과 중 routes
    #path = []

    ##여기에 코드 추가
    while (start != end):
        path.append(end)
        end = routes[end]
    path.append(start)
    
    pass

def string_to_int(name):
    if name[:4]=='host':
        return 1000+int(name[4:])
    else:
        return 10000+int(name[6:])
    

    #slot_scheduler: [1024*64, 4] 크기의 배열, 1024*64는 슬롯의 번호, 4는 flow구분 정보를 담음
    #lcm_prd=1024
    #flow: 플로우 구분을 위한 튜플
    #meta_data[0]=플로우의 주기
    #early_scheduling은 첫번째 슬롯을 할당할 수 있는 가장 작은 번호의 슬롯으로 할당합니다.
    #슬롯 할당은 다음과 같은 순서로 이루어집니다.
    #1. 첫번째 슬롯 번호 할당
    #2. 첫번째 슬롯 번호에서 플로우의 주기*64를 더한 값을 갖는 슬롯번호에 할당
    #3. lcm.prd/meta_data[0]번 2를 반복
    
def early_scheduling(slot_scheduler, lcm_prd, flow, meta_data):
    check = 0
    tmp = []   
    #print("early_scheduling")
    for idx, val in enumerate(slot_scheduler):
        if not any(val):
            check = 1       #scheudling 성공하면 1, 아니면 0, return 값
            offset = idx%64
            phase = idx//64
            prd = meta_data[0]
            temp = 0
            for _ in range((int)(lcm_prd/prd)):
                try: 
                    if any(slot_scheduler[idx+temp]):
                        check = 0
                        break
                    temp += prd * 64
                except IndexError:
                    print("스케줄링 실패")
                    print(f"flow: {flow}")
                    print(f"idx: {idx}")
                    print(f"prd: {prd}")
                    check = 0
                    return check
                
            
            if (check == 1):
                temp = 0
                for _ in range((int)(lcm_prd/prd)):
                    slot_scheduler[idx+temp] = flow
                    tmp.append(idx+temp)
                    temp += prd * 64
                break
        else:
            #print("next flow")
            pass

    return check


def delay_scheduling(slot_scheduler, lcm_prd, flow, meta_data, node, flow_idx):
    check = 0
    tmp = []   
    flow_index = 0 
    # print(f"{node.name}")
    # print(f"flow: {flow}")
    # print(f"flow idx: {flow_idx}")
    t = flow_idx + 2
    for idx, val in enumerate(slot_scheduler):
        if t != 0:
            t = t - 1
            continue
        if not any(val):
            check = 1
            prd = meta_data[0]
            temp = 0
            for _ in range((int)(lcm_prd/prd)):
                try: 
                    if any(slot_scheduler[idx+temp]):
                        check = 0
                        break
                    temp += prd*64
                except IndexError:
                    print("스케줄링 실패")
                    print(f"flow: {flow}")
                    print(f"idx: {idx}")
                    print(f"prd: {prd}")
                    check = 0
                    return check, flow_index
                
            
            if (check == 1):
                flow_index = idx
                temp = 0
                for _ in range((int)(lcm_prd/prd)):
                    slot_scheduler[idx+temp] = flow
                    tmp.append(idx+temp)
                    temp += prd*64
                break
        else:
            #print("next flow")
            pass
            

    return check, flow_index