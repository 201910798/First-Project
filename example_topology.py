import numpy as np
import simpy
import pandas as pd
import util
import random
import networkx as nx
import matplotlib.pyplot as plt

###라우팅, 스케줄링 관련 line 257-285 확인

MACRO_CYCLE = 1024
LCM_PRD = 1024

class Packet:
    def __init__(self, length, src_addr, dest_addr, src_port, dest_port, seq_num, meta_data):
        self.length = length
        self.src_addr = src_addr
        self.dest_addr = dest_addr
        self.src_port = src_port
        self.dest_port = dest_port
        self.seq_num = seq_num
        self.meta_data = meta_data

class Application:
    def __init__(self, env, local_port=0, dest_port=0, src_addr=0, dest_addr=1, min_pkt_len=1000, max_pkt_len=1000, 
                 burst_size=1, service_rate=1000, send_interval=1, start_time=0, stop_time=10000):
        self.local_port = local_port
        self.dest_port = dest_port
        self.src_addr = src_addr
        self.dest_addr = util.string_to_int(dest_addr)
        self.max_pkt_len = max_pkt_len
        self.min_pkt_len = min_pkt_len
        self.burst_size = burst_size
        self.service_rate = service_rate
        self.send_interval = send_interval
        self.start_time = start_time
        self.stop_time = stop_time
        self.queue = simpy.Store(env)
        self.env = env
        self.seq_num = 0

    def start(self):
        self.env.process(self.generate_burst())

    def generate_burst(self):
        yield self.env.timeout(self.start_time)
        while self.env.now<=self.stop_time:
            yield self.env.timeout(self.send_interval)
            if np.random.rand()<0.999:
                k = np.random.randint(1, self.burst_size+1)
                for _ in range(k):
                    p = self.generate_packet()
                    self.queue.put(p)
                    #print(self.env.now, "generate packet", self.seq_num, 'flow', (self.src_addr, self.dest_addr, self.local_port, self.dest_port), 'prd', p.meta_data[0])
                break

    def generate_packet(self):
        length = np.random.randint(self.min_pkt_len, self.max_pkt_len+1)
        packet = Packet(length, self.src_addr, self.dest_addr, self.local_port, self.dest_port, self.seq_num, [self.send_interval, self.env.now])
        self.seq_num += 1
        return packet

class Link(simpy.Store):
    #링크
    #필요 변수: 용량(capacity, bps단위), 물리적 거리(m단위)
    def __init__(self, env, link_capacity=100000000, distance=10):
        super().__init__(env)
        self.link_capacity = link_capacity
        self.distance = distance

class OutputPort:
    def __init__(self, env, node_name, macro_cycle, link):
        self.env = env
        self.node_name = node_name
        self.output_queues = {}
        self.macro_cycle = macro_cycle
        self.slot_scheduler = np.zeros([self.macro_cycle, 4])
        self.link = link
        self.slot_point = 0

    def action(self, meta_data):
        phase = 0
        offset = 1
        return phase, offset
    
    def scheduling(self, flow, meta_data, node, flow_index):
        #print("output_scheduling")
        scheduling_succeed, flow_index = util.delay_scheduling(self.slot_scheduler, LCM_PRD, flow, meta_data, node, flow_index)
        #scheduling_succeed = util.early_scheduling(self.slot_scheduler, LCM_PRD, flow, meta_data)
        return scheduling_succeed, flow_index
        # phase, offset = self.action(meta_data)
        # prd = 2#meta_data[0]
        # self.slot_scheduler[phase*64+offset] = flow
        # while phase*64+prd*64+offset<self.macro_cycle:
        #     self.slot_scheduler[phase*64+64*prd+offset] = flow
        #     prd = prd + meta_data[0]

    def put(self, packet):
        #print("put")
        flow = (packet.src_addr, packet.dest_addr, packet.src_port, packet.dest_port)
        if flow not in self.output_queues:
            self.output_queues.update({flow : simpy.Store(self.env)})
            #util.early_scheduling(self.slot_scheduler, 2, flow, [2])
            #self.scheduling(flow, packet.meta_data)
        self.output_queues[flow].put(packet)

    def forwarding(self):
        #print("forwarding")
        while True:
            flow = tuple(self.slot_scheduler[self.slot_point%self.macro_cycle])
            if any(flow):
                if len(self.output_queues[flow].items):
                    packet = yield self.output_queues[flow].get()
                    #print(self.env.now, self.node_name, "forward", packet.seq_num, 'flow', flow)
                    if self.node_name[:-1]=='host':
                        packet.meta_data[1]=self.env.now
                    yield self.env.timeout(0.015625)
                    self.link.put(packet)
                else:
                    yield self.env.timeout(0.015625)
            else:
                yield self.env.timeout(0.015625)
            self.slot_point = (self.slot_point+1)%self.macro_cycle

class Node:
    def __init__(self, env, name):
        self.env = env
        self.name = name
        self.addr = util.string_to_int(name)
        self.links_in = []
        self.links_out = []
        self.output_port = []
        self.routing_table = {}

    def start(self):
        for idx in range(len(self.output_port)):
            self.env.process(self.output_port[idx].forwarding())

    def add_link(self, link_out, link_in):
        self.links_in.append(link_in)
        self.links_out.append(link_out)
        self.output_port.append(OutputPort(self.env, self.name, MACRO_CYCLE*64, link_out))

    def routing(self, dest_addr, idx):
        if dest_addr not in self.routing_table:
            self.routing_table.update({dest_addr:[idx]})
        elif idx not in self.routing_table[dest_addr]:
            self.routing_table[dest_addr].append(idx)

    def get_route(self, dest_addr):
        return self.routing_table[dest_addr]

class Switch(Node):
    def __init__(self, env, name):
        super().__init__(env, name)

    def start(self):
        super().start()
        for idx in range(len(self.links_in)):
            self.env.process(self.receive_packet(idx))

    def receive_packet(self, idx_in):
        while True:
            packet = yield self.links_in[idx_in].get()
            flow = (packet.src_addr, packet.dest_addr, packet.src_port, packet.dest_port)
            #print(f"Switch packet: {flow}")
            idx_list = self.get_route(packet.dest_addr)
            for idx in idx_list:
                if flow in self.output_port[idx].output_queues:
                    self.output_port[idx].put(packet)
                    break
            #queue_idx = self.get_route(packet.dest_addr)
            #self.output_port[queue_idx[0]].put(packet)
            #print(self.name+" in")
    
class Host(Node):
    def __init__(self, env, name, applications=None):
        super().__init__(env, name)
        self.applications = applications if applications is not None else []
        self.queue = simpy.Store(env)
        self.delay_dict = {}

    def start(self):
        super().start()
        for app in self.applications:
            app.start()
            self.env.process(self.send_packet(app))
        for idx in range(len(self.links_in)):
            self.env.process(self.receive_packet(idx))

    def send_packet(self, app):
        while True:
            packet = yield app.queue.get()
            flow = (packet.src_addr, packet.dest_addr, packet.src_port, packet.dest_port)
            idx_list = self.get_route(packet.dest_addr)
            for idx in idx_list:
                if flow in self.output_port[idx].output_queues:
                    self.output_port[idx].put(packet)
                    break
            #print(self.name+" out")

    def receive_packet(self, idx):
        while True:
            p = yield self.links_in[idx].get()
            flow = (p.src_addr, p.dest_addr, p.src_port, p.dest_port)
            #print(f"Host packet: {flow}")
            delay = self.env.now-p.meta_data[1]
            self.delay_dict.update({flow: delay})
            del p
            #print(self.name+" in")

    def add_app(self, app):
        self.applications.append(app)
        if self.env.now:
            '''flow = (app.src_addr, app.dest_addr, app.local_port, app.dest_port)
            idx_list = self.get_route(app.dest_addr)
            idx = random.choice(idx_list)
            if flow not in self.output_port[idx].output_queues:
                self.output_port[idx].output_queues.update({flow : simpy.Store(self.env)})
                self.output_port[idx].scheduling(flow, [app.send_interval])'''
            app.start()
            self.env.process(self.send_packet(app))

class Topology:
    def __init__(self, env, topology_table=[]):
        self.env = env
        self.topology_table = topology_table
        self.nodes = {}
        self.links = {}
        #self.flow_index = 0

    def add_node(self, node):
        if type(node) == list:
            for node_ in node:
                if node_ not in self.nodes:
                    self.nodes.update({node_ : node_})
                    self.links.update({node_ : {}})
        elif node not in self.nodes:
            self.nodes.update({node : node})
            self.links.update({node : {}})

    def add_link(self, node1, node2):
        if node1 not in self.nodes:
            self.nodes.update({node1 : node1})
            self.links.update({node1 : {}})
        if node2 not in self.nodes:
            self.nodes.update({node2 : node2})
            self.links.update({node2 : {}})
        if node2 not in self.links[node1]:
            link1 = Link(self.env)
            link2 = Link(self.env)
            self.links[node1][node2] = link1
            self.links[node2][node1] = link2
            self.nodes[node1].add_link(link1, link2)
            self.nodes[node2].add_link(link2, link1)

    def add_app(self, host, app):
        self.routing()
        flow_index = -2
        k = self.scheduling(host, app, flow_index)
        if k:
            host.add_app(app)
        return k

    def routing(self):
        #print("routing")
        for node in self.nodes:
            _, routes = util.dijkstra(self, node)
            # print("-------routes-------")
            # print(routes)
            # print("--------------------")
            for other in self.nodes:
                if other!=node and type(other)==Host:
                    path = []
                    util.shortest(node, other, routes, path)
                    path = path[::-1]
                    # print("----Path----")
                    # print(path)
                    # print("--------------------")
                    # print("----Link_out----")
                    # print(node.links_out)
                    # print("------------------")
                    # print("----host_Link----")
                    # print(self.links[node])
                    # print("------------------")
                    k = node.links_out.index(self.links[node][path[1]])
                    node.routing(path[-1].addr, k)

    def scheduling(self, node, app, flow_index):
        #print("topo_scheduling")
        #print(f"node name: {node.name}")
        #print(f"node.addr: {node.addr}")
        #print(f"dest.addr: {app.dest_addr}")
        if node.addr!=app.dest_addr:
            #print("if_1")
            flow = (app.src_addr, app.dest_addr, app.local_port, app.dest_port)
            idx_list = node.get_route(app.dest_addr)
            idx = random.choice(idx_list)
            if flow not in node.output_port[idx].output_queues:
                #print("if_2")
                k, flow_index = node.output_port[idx].scheduling(flow, [app.send_interval], node, flow_index)
                if not k:
                    #print("if_3")
                    return k
                node.output_port[idx].output_queues.update({flow : simpy.Store(self.env)})
            for key, val in self.links[node].items():
                #print("for")
                if node.output_port[idx].link == val:
                    #print("if_4")
                    break
            i = self.scheduling(key, app, flow_index)
            return i
        else:
            return 1

    def start(self):
        print("start")
        self.routing()
        for node in self.nodes:
            #if type(node) == Host and len(node.applications):
            #    self.scheduling(node)
            node.start()
    
    def make_topology(self, topology_table):
        node_dict = {}
        print("make_topology")
        for name in topology_table.columns:
            if name[:4] == 'host':
                node = Host(self.env, name)
            else:
                node = Switch(self.env, name)
            self.add_node(node)
            node_dict.update({name : node})
        for name in topology_table.columns:
            for name2 in topology_table.columns:
                if topology_table[name][name2]:
                    self.add_link(node_dict[name], node_dict[name2])
        return node_dict


if __name__=="__main__":
    pd.DataFrame()

    env = simpy.Environment()

    flow_prd_list = [2, 4, 8, 16, 32, 64]

    data = {
        'host1' : [0, 1, 1, 0, 0, 0, 0, 0, 0, 0],
        'switch1' : [1, 0, 1, 1, 0, 0, 0, 0, 0, 0],
        'switch2' : [1, 1, 0, 0, 1, 0, 0, 0, 0, 0],
        'switch3' : [0, 1, 0, 0, 1, 1, 0, 0, 0, 0],
        'switch4' : [0, 0, 1, 1, 0, 0, 1, 0, 0, 0],
        'switch5' : [0, 0, 0, 1, 0, 0, 1, 1, 0, 0],
        'switch6' : [0, 0, 0, 0, 1, 1, 0, 0, 1, 0],
        'switch7' : [0, 0, 0, 0, 0, 1, 0, 0, 1, 1],
        'switch8' : [0, 0, 0, 0, 0, 0, 1, 1, 0, 1],
        'host2' : [0, 0, 0, 0, 0, 0, 0, 1, 1, 0]
        }
    # data = {
    #     'host1' : [0, 1, 0, 0],
    #     'switch1' : [1, 0, 1, 0],
    #     'switch2' : [0, 1, 0, 1],
    #     'host2' : [0, 0, 1, 0]
    #     }
    index_name = data.keys()
    df = pd.DataFrame(data, index=index_name)

    topo = Topology(env)
    node_dict = topo.make_topology(df)
    for i in range(10000):
        prd = random.choice(flow_prd_list)
        k = topo.add_app(node_dict['host1'], Application(env, local_port=i, dest_port=i, dest_addr="host2", send_interval=prd))
        if not k:
            break

    # graph = nx.from_pandas_adjacency(df)
    # nx.draw(graph, with_labels=True)
    # plt.show()

    topo.start()
    print(f"1: {len(node_dict['host1'].applications)}")
    env.run(until=1000)
    print(f"2: {len(node_dict['host2'].delay_dict)}")
    num = 0
    delay_sum = 0
    for flow, delay in node_dict['host2'].delay_dict.items():
        delay_sum += delay
        #print(f"flow: {flow}, delay: {delay}")
        if delay<512:
            num += 1

    print(f"delay_avg: {delay_sum/num}")
    print(f"num {num}")
    # print(f"switch7 routing table: {node_dict['switch7'].routing_table}")
    # print(f"switch8 routing table: {node_dict['switch8'].routing_table}")
    print(f"4: {node_dict['host1'].output_port[0].slot_scheduler[64*0:64*1]}")
    #print(f"5: {node_dict['host1'].output_port[0].slot_scheduler[64*1023:64*1024]}")
    # print(f"6: {node_dict['switch3'].output_port[1].slot_scheduler[64*0:64*1]}")
    print(f"7: {node_dict['switch1'].output_port[2].slot_scheduler[64*0:64*1]}")
    print(f"8: {node_dict['switch3'].output_port[2].slot_scheduler[64*0:64*1]}")


    # final = 0
    # for i in range(10):
    #     topo.start()
    #     #print(f"1: {len(node_dict['host1'].applications)}")
    #     env.run(until=1000)
    #     #print(f"2: {len(node_dict['host2'].delay_dict)}")
    #     num = 0
    #     delay_sum = 0
    #     for flow, delay in node_dict['host2'].delay_dict.items():
    #         delay_sum += delay
    #         #print(f"flow: {flow}, delay: {delay}")
    #         if delay<512:
    #             num += 1
    #     final += delay_sum/num
    #     # print(f"delay_avg: {delay_sum/num}")
    #     # print(f"num {num}")
    #     # # print(f"switch7 routing table: {node_dict['switch7'].routing_table}")
    #     # # print(f"switch8 routing table: {node_dict['switch8'].routing_table}")
    #     # print(f"4: {node_dict['host1'].output_port[0].slot_scheduler[64*0:64*1]}")
    #     # print(f"5: {node_dict['host1'].output_port[0].slot_scheduler[64*1023:64*1024]}")
    #     # # print(f"6: {node_dict['switch3'].output_port[1].slot_scheduler[64*0:64*1]}")
    #     # print(f"7: {node_dict['switch3'].output_port[2].slot_scheduler[64*0:64*1]}")
    #     # print(f"8: {node_dict['switch3'].output_port[2].slot_scheduler[64*1023:64*1024]}")

    #print(f"final: {final}")



    '''df = pd.read_excel("c:/Users/User/code/TTDeep/cev.xlsx", engine='openpyxl', index_col=0)

    topo = Topology(env)S
    node_dict = topo.make_topology(df)
    host_list = list(filter(lambda x: x[:4]=='host', df.columns))
    switch_list = list(filter(lambda x: x[:5]=='switch', df.columns))

    topo.routing()
    for i in range(10000):
        prd = random.choice(flow_prd_list)
        host1, host2 = random.sample(host_list, 2)
        k = topo.add_app(node_dict[host1], Application(env, local_port=i, dest_port=i, dest_addr=host2, send_interval=prd))
        if not k:
            break
    print(i)
    #print(node_dict['host1'].output_port[0].slot_scheduler[0:64])

    topo.start()
    env.run(until=10000)
    num = 0
    for host_name in host_list:
        for flow, delay in node_dict[host_name].delay_dict.items():
            if delay<512:
                num += 1
    print(num)
    
    graph = nx.from_pandas_adjacency(df)
    #host: 7, 5, 3, 6, 4, 6
    #switch: 2, 2, 2, 2, 3
    pos = {}
    for i in range(0,7):
        pos.update({'host'+str(i+1):[0, i+5]})
    for i in range(7,12):
        pos.update({'host'+str(i+1):[20, i-7+5]})
    for i in range(12,15):
        pos.update({'host'+str(i+1):[40, i-12+5]})
    for i in range(15,21):
        pos.update({'host'+str(i+1):[60, i-15+5]})
    for i in range(21,25):
        pos.update({'host'+str(i+1):[80, i-21+5]})
    for i in range(25,31):
        pos.update({'host'+str(i+1):[100, i-25+5]})
    
    for i in range(0,2):
        pos.update({'switch'+str(i+1):[10, i]})
    for i in range(2,4):
        if i%2:
            k=0.5
        else:
            k=-0.5
        pos.update({'switch'+str(i+1):[30, i-2+k]})
    for i in range(4,6):
        pos.update({'switch'+str(i+1):[50, i-4]})
    for i in range(6,8):
        pos.update({'switch'+str(i+1):[70, i-6]})
    for i in range(8,10):
        if i%2:
            k=0.5
        else:
            k=-0.5
        pos.update({'switch'+str(i+1):[90, i-8+k]})
    for i in range(10,13):
        pos.update({'switch'+str(i+1):[110, i-10-0.5]})

    nx.draw(graph, pos, with_labels=True)
    plt.show()'''