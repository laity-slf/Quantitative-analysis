# Quantitative-analysis

## data_process 

#### data_preprocess_SH.py

*目的*: 将**上海**的tick交易单和po委托单打包成po->ao->t 的数据流，用作进一步的特征提取。  

*难点*:
>
>- 上海trade/order两个数据流时间戳⽆法互相⽐较，且每个流均有可能产⽣堵单情况。
>- 不同于深圳数据，上海的order流不会提供主动单（active order）的信息，只会提供被动单（passive order），⽤户需要通过trade流的成交（trade），推演出主动单的信息。但是当接收到⼀个成交的时候，会有两种可能性，⼀是这个成交是这⽐主动单的所有⾦额(full order, trade_qty=order_qty)，⼆是这个成交是这⽐主动单的⼀部分⾦额(partial order, trade_qty<order_qty)，之后可能还会推剩下的成交过来，但是在这个时间点，⽆法得知是哪种情况。
>- 解决NBBO问题：根据市场交易规则，缩⼩可插⼊范围。例如如果⼀个挂卖单在100元 PO1，⼜有⼀个挂卖单在99元 PO2，这时候来了⼀个100元的主动买T，那么根据NBBO原则，T不可能出现在PO2之后（不然的话交易价格会在99元），那么我们就缩⼩了T可插⼊的范围。
>- 可能会偶尔存在丢包现象。

*解决方案设定*:
  >
  > - 默认订单顺序格式是po->ao->t；T插入一定在上一个T之后（T时间自增）；现实情况存在T订单先到来而po还未到，这个时候要补上po，然后用T和F标记po是否真的到来
  > - po订单的插入根据订撤单标志（'A'和'D'）分为两种情况，撤单直接插入在T与最后一个为T的po单之后（同时满足在T与po之后即取max），订单则需匹配前面是否出现同样的订单po（标记为F的po），无则移动到T与最后一个为T的po单之后，有则先删去匹配的po再移动
  > - ao订单插入分为四种情况，其中ao与po的确定需要配合BSflag:  
    1. po ao 均存在：po不需要更新，ao存在说明之前有订单，只需累加  
    2. po在ao不在：说明是首单，记录数量即可  
    3. po不在，ao不在：po不在说明信息错乱，先补po，无所谓ao，po插入顺序，因为po会在order插入函数中进行更新  
    4. po不在，ao在：po同3，ao同2
  >
#### data_preprocess_SZ.py

*目的*: 将**深圳**的tick交易单和po委托单打包成类似上海的数据流，用作进一步的特征提取。

*难点*:
>type，因为order表里有po也有ao。当OrdType=1的时候，肯定是AO；但是OrdType=2或者U的时候，有可能是AO也可能是PO。对于每一个order单，先检查是否有对应trade或者撤单，如果可以找到的话，那么很好判断是po 还是 ao（如果是撤单的话，肯定是po）；如果找不到的话，那么先统一认定是po。对于每一个trade或者撤单，需要再回去modify order的种类。

*解决方案设定*:
>
>- 对于trade，先去match是否有对应的市价单。如果matched，TradeBSFlag(tick)的方向，由order表决定，不通过tick.BidApplSeqNum和 tick.OfferApplSeqNum大小决定。（所以市价单，最好存一个字典，记录下他们的ApplSeqNum和side(买或者卖)。没有用的市价单需要删除），之后插入的逻辑同上海。
>- 对于order数据点，我们在流⾥中寻找是否有已有的order_id，会有以下N种情况。如果matched，首先把F 的order删掉然后看下stream是否有F 的order，如果没有的话，就append order（有可能会再append一个撤单，请见如果unmatched,证明po来晚了。那么可以建立个字典把这些撤单存下来，等到po来了，再在后面append一个撤掉的order）；如果有F的order，就插到最后一个True的order后面（有可能会再插一个撤单，请见如果unmatched,证明po来晚了。那么可以建立个字典把这些撤单存下来，等到po来了，再在后面append一个撤掉的order）。如果没有matched,看下stream是否有F的order，如果没有的话，就append order；如果有F的order，就插到最后一个True的order后面。