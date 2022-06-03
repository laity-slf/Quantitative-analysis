import csv
#这里假定节点顺序必须为PO AO T
from datetime import datetime
import random
import numpy as np
import pandas as pd
from numba import jit
#导入两个csv

class my_stream():
    def __init__(self) -> None:
        self.stream=[]
        self.index=0
        self.T = []
        self.temppo={}
        self.tempao={}
        self.fakepo={}
        #储存的是索引
        self.last_T_index = 0
    #这是验证模块
    def val(self,order_data,trade_data):
        PO= {}
        AO= {}
        m_PO={}
        m_T={}
        oindex=0
        tindex=0
        min_tindex = -1
        max_oindex = -1
        for i in range(0,self.index):
            if self.stream[i][0]=='PO' :
                if self.stream[i][7]=='A':
                    y=self.stream[i][1:11]
                    PO.update({self.stream[i][3]:i})
                    if y!=order_data[oindex]:
                        #错误信息
                        m_PO[oindex]=y
                #TODO 如果流的信息多会不会报错？？拟解决方案：流的数量比实际多的话直接报错，少的话走下面的匹配分支
                oindex+=1
                if oindex==len(order_data):
                    max_oindex = i
            #用先后来判断 i
            elif self.stream[i][0]=='AO':
                AO.update({self.stream[i][3]:i})
            else:
                if min_tindex<0:min_tindex=i
                a=[self.stream[i][1]]+self.stream[i][11:]
                if a!=self.T[tindex]:
                    m_T[tindex]=a
                tindex+=1
                if self.stream[i][-2]=='B':
                    aono=self.stream[i][-5]
                    pono=self.stream[i][-4]
                elif self.stream[i][-2]=='S':
                    aono = self.stream[i][-4]
                    pono = self.stream[i][-5]
                else:
                    continue
                s=AO[aono]
                d=PO[pono]
            ##检查 对于每个T是否有PO->AO->T
                if not (i>s>d):
                    print(self.stream[i])
                    print("not po-ao-t")
                    return "not po-ao-t"
    ## 匹配；流中信息可能少于实际信息，因此要数量匹配
        #打印错误匹配消息（流->实际）
        if  m_PO or m_T:
                for k,y in m_PO.items():
                    print("PO中 第{}行不匹配{}".format(k,y))
                for k,y in m_T.items():
                    print("T中 第{}行不匹配{}".format(k,y))
                return "mismatch"
        # print('stream中的PO和T 均匹配')          
        #数量对比
        if oindex != len(order_data):
            print("the size of PO(%d)not match order(%d)"%oindex%len(order_data))
            return 
        if tindex != len(self.T):
            print("the size of Trade(%d) not match tick(%d)"%tindex%len(order_data))
            return
        # print("the same size")
        print("po与order T和tick 完全匹配 ")
    ## 排除po po po po ... po ao t ao t：找到最小的t与最后一个po对比
        if min_tindex>max_oindex:
            print("PO 前部聚集")
            return "PO 前部聚集"
        print("no problem")


    def find_last_T_PO(self):
        ftag = -1
        index=-1
        for i in range(self.index-1,-1,-1):
            if self.stream[i][0]=='PO' and self.stream[i][-1]=='T':
                index=i
                break
            if self.stream[i][-1]=='F':
                ftag=1
        return ftag,index

    # 找到前一个插入的T或者T po
    def get_lastT(self,index):
        for i in range(index-1,-1,-1):
            if self.stream[i][0]=='T' or (self.stream[i][0]=='PO' and self.stream[i][-1]=='T'):
                return i 

    #更新 （l,r）之间的 po index 和 last_T_index
    def update_poindex(self,l=None,r=None,n=1,insert_type='T'):
        if not l:l =0
        if not r:r=self.index
        for i in range(l,r):
            if self.stream[i][0]=='PO' and self.stream[i][7]=='A':
                self.temppo[self.stream[i][3]][0] +=n
                if insert_type=='T':
                    self.temppo[self.stream[i][3]][1]= self.last_T_index

    def get_tfpo(self,po):
        return self.stream[self.temppo[po][0]][-1]=='T' 
        # for i in range(self.index-1,-1,-1):
        #             if po == self.stream[i][3] and self.stream[i][0]=='PO':
        #                 if  self.stream[i][-1]=='T':
        #                     return True
        #                 else:
        #                     return False
        # return False

    def move_ao(self,ao,index):
        assert self.tempao[ao][0][0]<index
        tmp = self.tempao[ao][0][0]
        #move ao
        self.stream.insert(index+1,self.stream[tmp])
        self.stream.pop(tmp)
        n = len(self.tempao[ao])
        num = 0
        for i in range(n):
            #只插入比index小的T
            if self.tempao[ao][i][1]<index:
                temp=self.tempao[ao][i][1]- 1 - i
                #move T
                self.stream.insert(index+1,self.stream[temp])
                self.stream.pop(temp)
                num +=1
            else:
                break
        # 先 po 后 ao
        #update po index
        i = tmp
        while i < index:
            if self.stream[i][0]=='PO' and self.stream[i][7]=='A':
                if self.stream[i][-1]=='F':
                    self.temppo[self.stream[i][3]][1]=self.get_lastT(i)
                # 第一个ao-t一定会减去 
                for j in range(1,num+1):
                    if j==num:self.temppo[self.stream[i][3]][0] -= (j+1);break
                    if self.temppo[self.stream[i][3]][0] < self.tempao[ao][j][1]:
                        self.temppo[self.stream[i][3]][0] -= (j+1)
                        break
            i+=1
        #update ao index
        for i in range(n):
            if i <num:
                self.tempao[ao][i][1] = index - num+i+1
            self.tempao[ao][i][0] = index -num
        print('ao moved %d'%index)

    def output(self):
    
        rename=['type','SecurityID(order)','TransactTime(order)','OrderNo(order)','Price(order)','Balance(order)','OrderBSFlag(order)','OrdType(order)',
                'OrderIndex(order)','ChannelNo(order)','BizIndex(order)','TradeTime(tick)','TradePrice(tick)','TradeQty(tick)','TradeAmount(tick)',
                'BuyNo(tick)','SellNo(tick)','TradeIndex(tick)','TradeBSFlag(tick)','BizIndex(tick)','Tag_po']
        data = pd.DataFrame(self.stream,columns=rename)
        data.to_csv('1.csv')

    def binary_search(self,alist, item):
        n = len(alist)

        if n > 0:
            mid = n // 2  # 找到中间值
            if alist[mid][2] == item:
                return 1  # 证明中间值就是要找的值
            elif alist[mid][2] < item:
                return self.binary_search(alist[mid + 1:], item)  # 在中间值的右侧是目标值，按照递归的思想进行调用函数
            else:
                return self.binary_search(alist[:mid], item)  # 在中间值的左侧是目标值，按照递归的思想进行调用函数
        return -1


    def insert_for_order(self,addline):
        #order处理
        if addline[6]=='D':
            if addline[2]==10117929:
                y=1
            tag,index = self.find_last_T_PO()
            # insert max(index,self.last_T_index)+1
            self.stream.insert(max(index,self.last_T_index)+1,
                ['PO', addline[0], addline[1], addline[2], addline[3], addline[4], addline[5], addline[6], addline[7],
                 addline[8], addline[9], 0, 0, 0, 0, 0, 0, 0, 0, 0,'T'])
                 #撤单不更新 temppo，订单不需要
            g=self.stream[-1]
            self.index += 1
        else:
            if addline[2] in self.fakepo:
                for i in range(self.index-1,-1,-1):
                #找到了预设的fake PO，更新
                    if addline[2] == self.stream[i][3] and self.stream[i][0]=='PO':
                        q=self.stream[i]
                        self.stream.pop(i)
                        break
            tag,index = self.find_last_T_PO()
        #PO不匹配，直接插在后面因为在外部已经用了归并保证时序
            if tag==-1:
                self.stream.append(['PO',addline[0],addline[1],addline[2],addline[3],addline[4],addline[5],addline[6],addline[7],addline[8]
                                ,addline[9],0,0,0,0,0,0,0,0,0,'T'])
                self.temppo[addline[2]]=[self.index,self.last_T_index]
                self.index += 1
            else:
                if index < self.temppo[addline[2]][1]: 
                    self.stream.insert(self.temppo[addline[2]][1]+1,['PO',addline[0],addline[1],addline[2],addline[3],addline[4],addline[5],addline[6],addline[7],addline[8]
                                        ,addline[9],0,0,0,0,0,0,0,0,0,'T'])
                    # 更新索引
                    self.update_poindex(l=self.temppo[addline[2]][1]+2,r=self.temppo[addline[2]][0],n=1,insert_type='PO')
                    self.temppo[addline[2]]=[self.temppo[addline[2]][1]+1,0]
                else:
                    self.stream.insert(index+1,['PO',addline[0],addline[1],addline[2],addline[3],addline[4],addline[5],addline[6],addline[7],addline[8]
                                        ,addline[9],0,0,0,0,0,0,0,0,0,'T'])
                    # 更新索引
                    self.update_poindex(l=index+2,r=self.temppo[addline[2]][0],n=1,insert_type='PO')
                    self.temppo[addline[2]]=[index+1,0]
                self.index += 1

    def insert_for_trade(self,addline):
        act_match=-1
        pass_match=-1
        T_index = -1

        if addline[9]=='N':
            return 1
        if addline[9]=='S':
            self.T.append(addline[0:8]+addline[9:])
            # pass_match = self.binary_search(oderlist, addline[5])
            # #TODO 1.先找到对应的PO 然后active 从po开始寻找对应的ao？？？
            if addline[5] in self.temppo:
                #TODO 根据上一个T的插入位置，从后面去查找
                #生成插入范围区间
                l=-1;r=-1
                # for i in range(self.index-1,-1,-1):
                #     if addline[5] == self.stream[i][3] and self.stream[i][0]=='PO' and self.stream[i][7]=='A':
                #         pass_match = i
                #         break
                pass_match=self.temppo[addline[5]][0]
                pass_match = max(self.last_T_index,pass_match)
                T_index = pass_match
                while T_index <self.index:
                    if self.stream[T_index][0]=='PO' and self.stream[T_index][6]=='B' and \
                        self.stream[T_index][7]=='A' and addline[2]<=self.stream[T_index][4]:
                            if addline[2]==self.stream[T_index][4]:
                                pass_match=T_index
                                T_index+=1
                                continue
                            if  self.stream[T_index][-1]=='F' and self.get_tfpo(addline[5]):    
                                _,ltindex = self.find_last_T_PO()
                                if ltindex >= self.last_T_index:
                                    l = pass_match
                                    r = ltindex
                                else:
                                    l = self.last_T_index
                                    r = T_index
                            else:
                                l = pass_match
                                r = T_index
                            break
                    T_index+=1
                #TODO 在范围内随机生成ao,t 插入的位置
                # np.random 参数的输出是 [l,r)，用random.randint
                if l< 0 or r <0: 
                    T_index = random.randint(pass_match+1,T_index)
                    # T_index  = pass_match+1
                else:
                    T_index = random.randint(l+1,r)
                    # T_index = l+1

            if addline[6] in self.tempao:
                act_match=self.tempao[addline[6]][0][0]
                # for i in range(self.index-1,-1,-1):
                #     # # act macth pass match
                #     if addline[6] == self.stream[i][3] and self.stream[i][0]=='AO':
                #         act_match = i
                #         break

            if act_match >= 0 and pass_match >= 0:
                # 更新主动卖
                self.tempao[addline[6]].append([self.tempao[addline[6]][0][0],T_index])

                self.stream[act_match][5] += addline[3]
                self.stream.insert(T_index,['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.last_T_index = T_index#索引，下同
                self.index += 1
                # 更新 po 索引
                self.update_poindex(l=T_index+1,n=1,insert_type='T')
                # 移动 ao
                if self.tempao[addline[6]][0][0] < self.temppo[addline[5]][0]:
                    self.move_ao(addline[6],self.temppo[addline[5]][0])

            if act_match >= 0 and pass_match < 0:
                # 主动单找到被动单延迟
                self.tempao[addline[6]].append([self.tempao[addline[6]][0][0],self.index+1])
                q = self.stream[act_match]

                self.stream[act_match][5] += addline[3]
                self.stream.append(['PO', addline[0], 0, addline[5], addline[2], addline[3], 0
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.fakepo[addline[5]]=1
                #上一次插入的 T
                self.temppo[addline[5]] = [self.index,self.last_T_index]
                # last_T 更新 
                self.last_T_index = self.index + 1
                self.index += 2
                # 移动 ao
                if self.tempao[addline[6]][0][0] < self.temppo[addline[5]][0]:
                    self.move_ao(addline[6],self.temppo[addline[5]][0])

            if act_match < 0 and pass_match >= 0:
                #TODO新建AO插在两个po中间 根据NBBO原则
                #保存 ao-T 对
                self.tempao[addline[6]] = [[T_index,T_index+1]]
                self.stream.insert(T_index,['AO', addline[0], addline[1], addline[6], addline[2], addline[3], 'S'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                self.stream.insert(T_index+1,['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.last_T_index = T_index + 1
                self.index += 2

                self.update_poindex(l=T_index+1,n=2,insert_type='T')

            if act_match < 0 and pass_match < 0:
                # 保存 ao-T 对
                self.tempao[addline[6]]= [[self.index + 1,self.index + 2]]
                self.stream.append(['PO', addline[0], 0, addline[5], addline[2], addline[3], 'B'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.stream.append(['AO', addline[0], addline[1], addline[6], addline[2], addline[3], 'S'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.fakepo[addline[5]] = 1
                self.temppo[addline[5]] = [self.index,self.last_T_index]
                self.last_T_index = self.index + 2
                self.index += 3

        if addline[9]=='B':
            self.T.append(addline[0:8]+addline[9:])
            if addline[6] in self.temppo:
                # pass_match= self.last_T_index
                #TODO 匹配插入的T_index
                l=-1;r=-1
                # for i in range(self.index-1,-1,-1):
                #     if addline[6] == self.stream[i][3] and self.stream[i][0]=='PO' and self.stream[i][7]=='A':
                #         pass_match = i
                #         break
                pass_match=self.temppo[addline[6]][0]

                pass_match = max(self.last_T_index,pass_match)
                T_index = pass_match
                while T_index <self.index:
                    if self.stream[T_index][0]=='PO' and self.stream[T_index][6]=='S' and \
                        self.stream[T_index][7]=='A'and addline[2]>=self.stream[T_index][4]:
                            if addline[2]==self.stream[T_index][4]:
                                pass_match=T_index
                                T_index+=1
                                continue
                            if  self.stream[T_index][-1]=='F' and self.get_tfpo(addline[6]):    
                                _,ltindex = self.find_last_T_PO()
                                if ltindex >= self.last_T_index:
                                    l = pass_match
                                    r = ltindex
                                else:
                                    l = self.last_T_index
                                    r = T_index
                            else:
                                l = pass_match
                                r = T_index
                            break
                    T_index+=1
                #TODO 在范围内随机生成ao,t 插入的位置
                # np.random 参数的输出是 [l,r)，用random.randint
                if l <0 and r <0: 
                    # T_index = random.randint(pass_match+1,T_index)
                    T_index = pass_match+1
                else:
                    # T_index = random.randint(l+1,r)
                    T_index = l+1

            if addline[5] in self.tempao:
                act_match=self.tempao[addline[5]][0][0]
                # for i in range(self.index-1,-1,-1):
                #     if addline[5] == self.stream[i][3] and self.stream[i][0]=='AO':
                #         act_match=i
                #         break

            if act_match>=0 and pass_match>=0:
                #更新主动
                #多个ao，假设有新的po进来，这边要移动到新的po后面即：pop后插入到T_index前
                self.tempao[addline[5]].append([self.tempao[addline[5]][0][0],T_index])

                # 由于插入的时候匹配前面的po可能会跳过ao
                self.stream[act_match][5]+=addline[3]
                self.stream.insert(T_index,['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.last_T_index = T_index#索引，下同
                self.index += 1
                # 更新插入后的 po 索引
                self.update_poindex(l=T_index+1,n=1,insert_type='T')
                # 移动 ao
                if self.tempao[addline[5]][0][0] < self.temppo[addline[6]][0]:
                    self.move_ao(addline[5],self.temppo[addline[6]][0])
            if act_match>=0 and pass_match<0:
                #主动单找到被动单延迟
                q=self.stream[act_match]
                self.tempao[addline[5]].append([self.tempao[addline[5]][0][0],self.index+1])

                self.stream[act_match][5] += addline[3]
                self.stream.append(['PO', addline[0], 0, addline[6], addline[2], addline[3], 0
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                #上一次插入的 T，这次的po实际是在T的前面
                self.temppo[addline[6]]= [self.index,self.last_T_index]
                # last_T 更新
                self.last_T_index = self.index + 1
                self.index += 2
                # 移动 ao
                if self.tempao[addline[5]][0][0] < self.temppo[addline[6]][0]:
                    self.move_ao(addline[5],self.temppo[addline[6]][0])

            if act_match<0 and pass_match>=0:
                #如果之前有多个 ao 订单 对应，调整
                self.tempao[addline[5]] = [[T_index,T_index+1]]
                self.stream.insert(T_index,['AO',addline[0],addline[1],addline[5],addline[2],addline[3],'B'
                                                ,0,0,0,0,0,0,0,0,0,0,0,0,0])
                self.stream.insert(T_index+1,['T',addline[0],0,0,0,0,0,0,0,0,0,addline[1],addline[2],addline[3]
                                    ,addline[4],addline[5],addline[6],addline[7],addline[9],addline[10]])
                self.last_T_index = T_index + 1
                self.index += 2
                # 对应的po索引和last_T_index修改
                self.update_poindex(l=T_index+1,n=2,insert_type='T')

            if act_match<0 and pass_match<0:
                self.tempao[addline[5]] = [[self.index + 1,self.index+2]]
                self.stream.append(['PO', addline[0], 0, addline[6], addline[2], addline[3], 0
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.stream.append(['AO', addline[0], addline[1], addline[5], addline[2], addline[3], 'B'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])

                self.temppo[addline[6]] = [self.index,self.last_T_index]
                self.last_T_index = self.index + 2
                self.index += 3

with open('o1.csv') as oder_file,open('t1.csv') as trade_file:
# with open('o.csv') as oder_file, open('t.csv') as trade_file:
    oder_csv = pd.read_csv(oder_file)
    trade_csv=pd.read_csv(trade_file)
    order_data=np.array(oder_csv).tolist()
    trade_data=np.array(trade_csv).tolist()
    oder_index=0
    trade_index=0
    mergelist=[]
    my_stream=my_stream()
    t1 = datetime.now()
    while oder_index<len(order_data) and trade_index<len(trade_data):
        if order_data[oder_index][1]<=trade_data[trade_index][1]:
            #进来一条order单
            pno=order_data[oder_index][2]
            if order_data[oder_index][6]=='A':
                my_stream.temppo[pno]=1
            my_stream.insert_for_order(order_data[oder_index])
            oder_index+=1
        else:
            my_stream.insert_for_trade(trade_data[trade_index])
            trade_index+=1
    #mergelist=mergelist+order_data[oder_index:]+trade_data[trade_index:]
    for i in trade_data[trade_index:]:
        my_stream.insert_for_trade(i)
    for j in order_data[oder_index:]:
        my_stream.insert_for_order(j)
    t2 = datetime.now()
    print("Time cost = ", (t2 - t1))
    my_stream.val(order_data,trade_data)
    my_stream.output()
    