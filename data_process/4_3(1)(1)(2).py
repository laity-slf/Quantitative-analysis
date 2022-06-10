import csv
#这里假定节点顺序必须为PO AO T
from datetime import datetime

import numpy as np
import pandas as pd
from numba import jit
#导入两个csv

class my_stream():
    stream=[]
    index=0
    T = []
    temppo={}
    tempao={}
    fakepo={}

    #这是验证模块
    def val(self,order_data,trade_data):
        PO= {}
        AO= {}
        m_PO={}
        m_T={}
        
        oindex=0
        tindex=0
        for i in range(0,self.index):
            if self.stream[i][0]=='PO':
                y=self.stream[i][1:11]
                PO.update({self.stream[i][3]:i})
                if y!=order_data[oindex]:
                    #错误信息
                    m_PO[oindex]=y
                oindex+=1
            #用先后来判断 i
            elif self.stream[i][0]=='AO':
                AO.update({self.stream[i][3]:i})
            else:
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
                if i>s>d:
                    pass
                else:
                    print(self.stream[i])
                    print("not po-ao-t")
                    return "not po-ao-t"
        #数量对比
        if oindex !=len(order_data):
            print("the size of PO %d  not match order %d"%oindex%len(order_data))
        else:
            print("the same size of PO and order")
        if tindex!=len(self.T):
            print("the size of Trade  not match tick")
        else:
            print("the same size of Trade and tick")
        #打印错误匹配消息
        if  m_PO:
            for k,y in m_PO.items():
                print("PO中 第{}行不匹配{}".format(k,y))
        if  m_T:
            for k,y in m_T.items():
                print("T中 第{}行不匹配{}".format(k,y))
        # indexT=0
        # for s in NT:
        #     if s==self.T[indexT]:
        #         indexT+=1
        #     else:
        #         print(s)
        #         print(self.T[indexT])
        #         return -1
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
            self.stream.insert(index+1,
                ['PO', addline[0], addline[1], addline[2], addline[3], addline[4], addline[5], addline[6], addline[7],
                 addline[8], addline[9], 0, 0, 0, 0, 0, 0, 0, 0, 0,'T'])
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
                self.index += 1
            else:
                self.stream.insert(index+1,['PO',addline[0],addline[1],addline[2],addline[3],addline[4],addline[5],addline[6],addline[7],addline[8]
                                    ,addline[9],0,0,0,0,0,0,0,0,0,'T'])
                self.index += 1
        try:
            if self.stream[1][5]!=1000:
                s=1
        except:
            pass

    def insert_for_trade(self,addline):
        act_match=-1
        pass_match=-1
        if addline[5]==26399:
            s=self.stream[1]
        if addline[9]=='N':
            return 1
        if addline[9]=='S':
            self.T.append(addline[0:8]+addline[9:])
            #pass_match = self.binary_search(oderlist, addline[5])
            if addline[5] in self.temppo:
                pass_match=1
            if addline[6] in self.tempao:
                act_match=1
                for i in range(self.index-1,-1,-1):
                #for i in range(self.index):
                    # act macth pass match
                    # if addline[5] == self.stream[i][3] and self.stream[i][0]=='PO':
                    #     pass_match = i
                    if addline[6] == self.stream[i][3] and self.stream[i][0]=='AO':
                        act_match = i
                        break
            if act_match >= 0 and pass_match >= 0:
                # 更新主动卖
                q = self.stream[act_match]
                y = self.stream[pass_match]
                self.stream[act_match][5] += addline[3]
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.index += 1
                # else:
                    # 如果找到的是PO，说明这是系统成交的单子 一笔PO-S加一笔PO-B，系统撮合而成，那么只需要加入一条T
                # self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                #                        , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                # self.index += 1
            if act_match >= 0 and pass_match < 0:
                # 主动单找到被动单延迟
                q = self.stream[act_match]

                self.stream[act_match][5] += addline[3]
                # 做一个PO插在AO之前
                # self.stream.insert(act_match, ['PO', addline[0], 0, addline[5], addline[2], addline[3], 0
                #     , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,'F'])

                # self.index+=1
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.stream.append(['PO', addline[0], 0, addline[5], addline[2], addline[3], 0
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.fakepo[addline[5]]=1
                self.index += 2
                # else:
                # self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                #                        , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                # self.index += 1
            if act_match < 0 and pass_match >= 0:
                # 新建AO插在?后面
                # self.stream.insert(pass_match)
                self.stream.append(['AO', addline[0], addline[1], addline[6], addline[2], addline[3], 'S'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                self.tempao[addline[6]] = 1
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.index += 2

            if act_match < 0 and pass_match < 0:
                self.stream.append(['AO', addline[0], addline[1], addline[6], addline[2], addline[3], 'S'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                self.tempao[addline[6]]=1
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.stream.append(['PO', addline[0], 0, addline[5], addline[2], addline[3], 'B'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.index += 3
                self.fakepo[addline[5]] = 1
        if addline[9]=='B':
            self.T.append(addline[0:8]+addline[9:])
            if addline[6] in self.temppo:
                pass_match=1
            if addline[5] in self.tempao:
                act_match=1
                for i in range(self.index-1,-1,-1):
                    #act macth pass match
                    # if addline[6] == self.stream[i][3] and self.stream[i][0]=='PO':
                    #     pass_match=i

                    if addline[5] == self.stream[i][3] and self.stream[i][0]=='AO':
                        act_match=i
                        break
            if act_match>=0 and pass_match>=0:
                #更新主动
                q = self.stream[act_match]
                y=self.stream[pass_match]

                    #这里还是需要插入T的
                self.stream[act_match][5]+=addline[3]
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.index += 1
                # else:
                #     #如果找到的是PO，说明这是系统成交的单子 一笔PO-S加一笔PO-B，系统撮合而成，那么只需要加入一条T
                # self.stream.append(['T',addline[0],0,0,0,0,0,0,0,0,0,addline[1],addline[2],addline[3]
                #                 ,addline[4],addline[5],addline[6],addline[7],addline[9],addline[10]])
                # self.index+=1
            if act_match>=0 and pass_match<0:
                #主动单找到被动单延迟
                q=self.stream[act_match]

                self.stream[act_match][5] += addline[3]
                #做一个PO插在AO之前
                # self.stream.insert(act_match,['PO',addline[0],0,addline[6],addline[2],addline[3],0
                #                                 ,0,0,0,0,0,0,0,0,0,0,0,0,0,'F'])
                # self.stream.append(['PO', addline[0], 0, addline[6], addline[2], addline[3], 0
                #     , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                # self.index+=1
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.stream.append(['PO', addline[0], 0, addline[6], addline[2], addline[3], 0
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.index+=2
                self.fakepo[addline[6]] = 1
                # else:
                # self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                #                        , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                # self.index += 1
            if act_match<0 and pass_match>=0:
                #新建AO插在?后面
                #self.stream.insert(pass_match)
                self.stream.append(['AO',addline[0],addline[1],addline[5],addline[2],addline[3],'B'
                                                ,0,0,0,0,0,0,0,0,0,0,0,0,0])
                self.tempao[addline[5]] = 1
                self.stream.append(['T',addline[0],0,0,0,0,0,0,0,0,0,addline[1],addline[2],addline[3]
                                    ,addline[4],addline[5],addline[6],addline[7],addline[9],addline[10]])
                self.index+=2

            if act_match<0 and pass_match<0:
                self.tempao[addline[5]] = 1
                self.stream.append(['AO', addline[0], addline[1], addline[5], addline[2], addline[3], 'B'
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                self.stream.append(['T', addline[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, addline[1], addline[2], addline[3]
                                       , addline[4], addline[5], addline[6], addline[7], addline[9], addline[10]])
                self.stream.append(['PO', addline[0], 0, addline[6], addline[2], addline[3], 0
                                       , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'F'])
                self.index+=3
                self.fakepo[addline[6]] = 1

with open('量化分析/o1.csv') as oder_file,open('量化分析/t1.csv') as trade_file:
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
    