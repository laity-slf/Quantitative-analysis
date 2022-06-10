from audioop import add
import csv
#这里假定节点顺序必须为PO AO T
from datetime import datetime

import numpy as np
import pandas as pd
from numba import jit
from torch import index_select
#导入两个csv
def compare_order(a,b):
    dic={1:'B',2:'S','1':'B','2':'S'}
    if a[2]!=b[2] or a[3]!=b[5] or a[5]!=b[0] or a[6]!=dic[b[4]] or a[9]!=b[-4]:
        return 0
    else:
        if a[4]!=b[-5] and b[-5]!=0:
            return 0
    return 1
def compare_tick(a,b):
    if a[11]!=b[-1] or a[12]!=b[3] or a[13]!=b[5] or a[14]!=b[-2] or a[15]!=b[1] or a[16]!=b[6]:
        print("tick 值不匹配")
        return 0
    return 1

class my_stream():
    stream=[]
    index=0
    T = []
    temppo={}
    tempD={}
    typetrue={}
    #上一个插入T的index
    last_T_index = 0
    def val(self,oderlist):
        oderdict={}
        oder = 0
        tdex = 0
        for i in range(self.index):

            if (self.stream[i][0]=='AO' or self.stream[i][0]=='PO') :
                if self.stream[i][7]=='A':
                    a=self.stream[i]
                    b=oderlist[oder]
                    s=compare_order(a,b)
                    if s==0:
                        print("oder error!")
                        return 0
                    oderdict[self.stream[i][3]]=1
                    oder += 1
                else:
                    g=self.stream[i]
                    if self.stream[i][3] not in oderdict:
                        print("D单不在oder之后")
                        return 0

            else:
                if self.stream[i][-7] not in oderdict or self.stream[i][-6] not in oderdict:
                    print("poao不在T之后")
                    return 0
                a=self.stream[i]
                b=self.T[tdex]
                s=compare_tick(a,b)
                if s==0:
                    print("tick值不匹配")
                    return 0
                tdex+=1
        print("noproblem")



    def find_last_T_PO(self,dtag):
        ftag = -1
        index=-1
        low = self.index - 50 if self.index - 50 > -1 else -1
        if dtag==1:
            low=-1
        for i in range(self.index-1,low,-1):
            if self.stream[i][-2]=='T':
                index=i
                break
            if self.stream[i][-2]=='F':
                ftag=1
        return ftag,index

    def output(self):

        rename=['type','SecurityID(order)','TransactTime(order)','OrderNo(order)','Price(order)','Balance(order)','OrderBSFlag(order)','OrdType(order)',
                'OrderIndex(order)','ChannelNo(order)','BizIndex(order)','TradeTime(tick)','TradePrice(tick)','TradeQty(tick)','TradeAmount(tick)',
                'BuyNo(tick)','SellNo(tick)','TradeIndex(tick)','TradeBSFlag(tick)','BizIndex(tick)','Tag_po','OrdType2(order)']
        data = pd.DataFrame(self.stream,columns=rename)
        data.to_csv('1.csv')

    def update_poindex(self,l =0,r =0,n=1,insert_type='T'):
        if not l:l =0
        if not r:r=self.index
        for i in range(l,r):
            if self.stream[i][0]!='T' and self.stream[i][7]=='A':
                self.temppo[self.stream[i][3]][0] +=n
                if insert_type=='T':
                    self.temppo[self.stream[i][3]][1]= self.last_T_index

    def insert_for_order(self,addline,id):
        #order处理
        dic={1:'B',2:'S','1':'B','2':'S'}
        if addline[5]==302100:
            s=1
        tag,index = self.find_last_T_PO(dtag=0)
        low=self.index-50 if self.index-50>-1 else -1
        poorao='PO'
        if addline[1]=='1' or addline[1]=='U':
            poorao='AO'

        #TODO 为什么不跟之前一样设定一个fake的，加快速度
        r=0
        for i in range(self.index-1,low,-1):
        #找到了预设的fake PO，更新
            if addline[5] == self.stream[i][3]:
                poorao=self.stream[0]
                # tag,index=self.find_last_T_PO(dtag=0)
                q=self.stream[i]
                r=i
                self.stream.pop(i)
                break

    #PO不匹配，直接插在后面因为在外部已经用了归并保证时序
        if tag==-1:
            h=self.stream[12610] if self.index>12610 else 0

            self.stream.append([poorao,id,addline[2],addline[5],addline[8],addline[0],dic[addline[4]],'A',0,addline[9],0
                            ,0,0,0,0,0,0,0,0,0,'T',addline[1]])
            self.temppo[addline[5]]=[self.index,self.last_T_index]
            self.index += 1
            h = self.stream[12610] if self.index > 12610 else 0
        ##TODO ？？？ po不是可能会insert么
        else:
            #保证po向上的时候不会越过之前po对应的上一个T（就不用再次维护前面的ao了）
            index = max(self.temppo[addline[5]][1],index)
            h = self.stream[12610] if self.index > 12610 else 0
            self.stream.insert(index+1,[poorao,id,addline[2],addline[5],addline[8],addline[0],dic[addline[4]],'A',0,addline[9],0
                            ,0,0,0,0,0,0,0,0,0,'T',addline[1]])
            h = self.stream[12610] if self.index > 12610 else 0

            self.temppo[addline[5]]= [index+1,self.last_T_index]
            #TODO 更新索引 
            self.update_poindex(l=index+2,r=r,n=1,insert_type='po')

            self.index += 1

        # tempD 用于保证撤单在真的订单之后
        if addline[5] in self.tempD:
            g=self.tempD[addline[5]]
            self.stream.append(g)
            self.index += 1


    def insert_for_trade(self,addline,id):
        act_match=-1
        pass_match=-1
        T_index=-1
        type = 'B' if addline[1] > addline[6] else 'S'

        if addline[-2]=='4':
            no=max(addline[1],addline[6])
            # 查看当前对应的订单是否已到达，如果为未到达则放入tempD中等待对应po到达。
            if no in self.temppo:
                idx=self.temppo[no][0]
                self.stream[idx][0]='PO'
                self.stream.append(['PO', id, addline[-1], no, 0, 0, type,'D'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'T',0])
                self.index += 1
            else:
                self.tempD[no]=['PO', id, addline[-1], no, 0, 0, type,'D'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'T',0]
        else:

            if type=='S':

                self.T.append(addline[0:8]+addline[9:])
                #pass_match = self.binary_search(oderlist, addline[5])
                if addline[1] in self.temppo:
                    pass_match=1
                    pass_idx=self.temppo[addline[1]][0]
                    #TODO 1.找到下一个 PO 2.生成 lr 的区间 3.插入
                    # 简单的解决方式：找到对应的po，在他之后插入即可，需要设置上一次插入的
                    T_index = max(pass_idx,self.last_T_index)

                if addline[-4] in self.temppo:
                    act_match=1
                    act_idx = self.temppo[addline[-4]][0]
                    T_index = max(act_idx,self.last_T_index)
                if T_index>0:T_index+=1
                if act_match >= 0 and pass_match >= 0:
                    # 更新
                    #TODO 深圳的ao不会对应多个价格么？？？？
                    if act_idx not in self.typetrue:
                        self.stream[act_idx][0]='AO'
                        self.typetrue[act_idx]=1
                        if self.stream[act_idx][-1]==1:
                            self.stream[act_idx][4] = addline[3]
                    if pass_idx not in self.typetrue:
                        self.stream[pass_idx][0] = 'PO'
                        # time = str(self.stream[pass_idx][2])[8:12] + "." + str(self.stream[pass_idx][2])[12:]
                        # if float(time) > 930.1 and float(time) < 1455:
                        #     self.stream[pass_idx][4] = addline[3]
                        self.typetrue[pass_idx]=1
                    self.stream.insert(T_index,['T', id, 0, 0, 0, 0, 0, 0,0, 0, 0, addline[-1], addline[3], addline[5]
                                           , addline[7], addline[1], addline[6], addline[0],type, 0,0,0])
                    self.last_T_index = T_index
                    self.index += 1
                    self.update_poindex(l=T_index+1,n=1,insert_type='T')

                if act_match >= 0 and pass_match < 0:
                    # 主动单找到被动单延迟
                    self.stream[act_idx][0]='AO'
                    self.stream.append(['PO', id, 0, addline[1], 0, 0, 'B','A'
                                           , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F','A'])
                    self.stream.append(['T', id, 0, 0, 0, 0, 0, 0,0, 0, 0, addline[-1], addline[3], addline[5]
                                           , addline[7], addline[1], addline[6], addline[0], type, 0, 0, 0])
                    self.temppo[addline[1]] = [self.index,self.last_T_index]
                    # last_T 更新 
                    self.last_T_index = self.index + 1
                    self.index += 2

                if act_match < 0 and pass_match >= 0:
                    # 新建AO插在?后面
                    # self.stream.insert(pass_match)
                    if pass_idx not in self.typetrue:
                        self.stream[pass_idx][0] = 'PO'
                        # time = str(self.stream[pass_idx][2])[8:12] + "." + str(self.stream[pass_idx][2])[12:]
                        # if float(time) > 930.1 and float(time) < 1455:
                        #     self.stream[pass_idx][4] = addline[3]
                        self.typetrue[pass_idx] = 1
                    self.stream.insert(T_index,['AO', id, 0, addline[-4], 0,0, 'S','A'
                                           , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,'F','A'])
                    self.stream.insert(T_index+1,['T', id, 0, 0, 0, 0, 0, 0, 0, 0, addline[-1], addline[3], addline[5]
                                           , addline[7], addline[1], addline[6], addline[0], type, 0, 0, 0])
                    self.temppo[addline[-4]] = [T_index,self.last_T_index]
                    self.last_T_index = T_index + 1
                    self.index += 2
                    self.update_poindex(l=T_index+1,n=2,insert_type='T')

                if act_match < 0 and pass_match < 0:

                    self.stream.append(['PO', id, 0, addline[1], 0, 0, 'B','A'
                                           , 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 'F','A'])
                    self.stream.append(['AO', id, 0, addline[-4], 0, 0, 'S','A'
                                           , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F','A'])
                    self.stream.append(['T', id, 0, 0, 0, 0, 0, 0, 0,0, 0, addline[-1], addline[3], addline[5]
                                   , addline[7], addline[1], addline[6], addline[0], type, 0, 0, 0])
                    self.temppo[addline[1]] = [self.index,self.last_T_index]
                    self.temppo[addline[-4]] = [self.index+1,self.last_T_index]
                    self.last_T_index  =self.index+2
                    self.index+=3
                    

            if type=='B':

                self.T.append(addline[0:8]+addline[9:])
                if addline[-4] in self.temppo:
                    pass_match=1
                    pass_idx=self.temppo[addline[-4]][0]
                    #TODO 1.找到下一个 PO 2.生成 lr 的区间 3.插入
                    # 简单的解决方式：找到对应的po，在他之后插入即可，需要设置上一次插入的
                    T_index = max(pass_idx,self.last_T_index) 
                if addline[1] in self.temppo:
                    act_match=1
                    act_idx = self.temppo[addline[1]][0]
                    T_index = max(act_idx,self.last_T_index) 
                if T_index>0:T_index+=1

                if act_match>=0 and pass_match>=0:
                    #更新主动
                    if act_idx not in self.typetrue:
                        self.stream[act_idx][0] = 'AO'
                        self.typetrue[act_idx] = 1
                        if self.stream[act_idx][-1]==1:
                            self.stream[act_idx][4] = addline[3]
                    if pass_idx not in self.typetrue:
                        self.stream[pass_idx][0] = 'PO'
                        self.typetrue[pass_idx] = 1

                        # time = str(self.stream[pass_idx][2])[8:12] + "." + str(self.stream[pass_idx][2])[12:]
                        # if float(time) > 930.1 and float(time) < 1455:
                        #     self.stream[pass_idx][4] = addline[3]
                    self.stream.insert(T_index,['T', id, 0, 0, 0, 0, 0,0, 0, 0, 0, addline[-1], addline[3], addline[5]
                                           , addline[7], addline[1], addline[6], addline[0],type, 0,0,0])
                    self.last_T_index = T_index
                    self.index += 1
                    self.update_poindex(l=T_index+1,n=1,insert_type='T')

                if act_match>=0 and pass_match<0:
                    #主动单找到被动单延迟
                    q=self.stream[act_match]
                    self.stream[act_idx][0] = 'AO'
                    self.stream.append(['PO', id, 0, addline[-4], 0, 0, 'S','A'
                                           , 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 'F','A'])
                    self.stream.append(['T', id, 0, 0, 0, 0, 0,0,  0, 0, 0, addline[-1], addline[3], addline[5]
                                           , addline[7], addline[1], addline[6], addline[0], type, 0, 0, 0])
                    self.temppo[addline[-4]] = [self.index,self.last_T_index]
                    # last_T 更新 
                    self.last_T_index = self.index + 1
                    self.index += 2

                if act_match<0 and pass_match>=0:
                    #新建AO插在?后面
                    #self.stream.insert(pass_match)
                    self.stream[pass_idx][0] = 'PO'
                    if pass_idx not in self.typetrue:
                        self.stream[pass_idx][0] = 'PO'
                        self.typetrue[pass_idx] = 1
                        # time = str(self.stream[pass_idx][2])[8:12] + "." + str(self.stream[pass_idx][2])[12:]
                        # if float(time) > 930.1 and float(time) < 1455:
                        #     self.stream[pass_idx][4] = addline[3]
                    self.stream.insert(T_index,['AO', id, 0, addline[1], 0, 0, 'B','A'
                                           , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F','A'])
                    self.stream.insert(T_index+1,['T', id, 0, 0, 0, 0,0, 0, 0, 0, 0, addline[-1], addline[3], addline[5]
                                           , addline[7], addline[1], addline[6], addline[0], type, 0, 0, 0])
                    self.temppo[addline[1]] = [T_index,self.last_T_index]
                    self.last_T_index = T_index + 1
                    self.index += 2
                    self.update_poindex(l=T_index+1,n=2,insert_type='T')

                if act_match<0 and pass_match<0:

                    self.stream.append(['PO', id, 0, addline[-4], 0, 0, 'S','A'
                                           , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F','A'])
                    self.stream.append(['AO', id, 0, addline[1], 0, 0, 'B','A'
                                           , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F','A'])
                    self.stream.append(['T', id, 0, 0, 0, 0, 0, 0,0, 0, 0, 0, addline[-1], addline[3], addline[5]
                                           , addline[7], addline[1], addline[6], addline[0], type, 0, 0, 0])
                    self.temppo[addline[-4]] = [self.index,self.last_T_index]
                    self.temppo[addline[1]] = [self.index+1,self.last_T_index]
                    self.last_T_index  =self.index+2
                    self.index+=3



with open('order_000001.csv') as oder_file,open('tick_000001.csv') as trade_file:
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
        if order_data[oder_index][2]<=trade_data[trade_index][-1]:
            #进来一条order单
            s=order_data[oder_index]
            pno=order_data[oder_index][5]
            #这边有问题，如果是插入那么不是对应当前的index
            # my_stream.temppo[pno]=my_stream.index
            my_stream.insert_for_order(order_data[oder_index],'000002')
            oder_index+=1
        else:
            my_stream.insert_for_trade(trade_data[trade_index],'000002')
            trade_index+=1
    #mergelist=mergelist+order_data[oder_index:]+trade_data[trade_index:]
    for i in trade_data[trade_index:]:
        my_stream.insert_for_trade(i,'000002')
    for j in order_data[oder_index:]:
        my_stream.insert_for_order(j,'000002')
    t2 = datetime.now()
    print("Time cost = ", (t2 - t1))
    my_stream.output()
    my_stream.val(order_data)


