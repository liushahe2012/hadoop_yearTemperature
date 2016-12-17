输入为data文件
数据是时间和温度的记录
其中年份yyyy-MM-dd HH:mm:ss后面跟的是制表符tab

计算出：
1949-1951 年之间，每个温度最高的前K天（例如k=5）

思路：
1.按照年份升序排序，同一年中，再按照温度降序排序。
2.按照年份分组，每个年份对应一个reduce任务。

map输出： key为封装对象，封装年份和温度为一个对象作为Key值，即keyPair类。

目的：
1.练习自定义排序
2.练习自定义分区
3.练习自定义分组
//分组的意义：
参考：http://www.linuxidc.com/Linux/2013-08/88603.htm
//最近看dadoop中关于辅助排序（SecondarySort）的实现，说到了三个东西要设置：1. partioner；2. Key Comparator；
//3. Group Comparator。前两个都比较容易理解，但是关于group的概念我一直理解不了，
//一，有了partioner，所有的key已经放到一个分区了，每个分区对应一个reducer，而且key也可以排序了，
//那么不是实现了整个数据集的全排序了吗？第二，mapper产生的中间结果经过shuffle和sort后，每个key整合成一个记录，
//每次reduce方法调用处理一个记录，但是group的目的是让一次reduce调用处理多条记录，这不是矛盾吗，
//找了好久一直都没找到这个问题的清晰解释。
//后来找到一本书，《Pro Hadoop》，里面有一部分内容详细解释了这个问题，看后终于明白了，和大家分享一下。reduce方法每次是读一条记录，
//读到相应的key，但是处理value集合时，处理完当前记录的values后，还会判断下一条记录是不是和当前的key是不是同一个组，
//如果是的话，会继续读取这些记录的值，而这个记录也会被认为已经处理了，直到记录不是当前组，这次reduce调用才结束，
//这样一次reduce调用就会处理掉一个组中的所有记录，而不仅仅是一条了。
//这个有什么用呢？如果不用分组，那么同一组的记录就要在多次reduce方法中独立处理，那么有些状态数据就要传递了，就会增加复杂度，
//在一次调用中处理的话，这些状态只要用方法内的变量就可以的。比如查找最大值，只要读第一个值就可以了。

参考： http://blog.csdn.net/zuochanxiaoheshang/article/details/8986114
Hadoop是如何进行排序的呢？根据笔者的理解，MapReduce的排序过程分为两个步骤，一个按照Key进行排序；一个是按照Key进行分组。
这两部分分别由SortComparator和GroupingComparator来完成。具体的配置如下面黑体所示：
job.setPartitionerClass(FirstPartitioner.class);
job.setSortComparatorClass(KeyComparator.class);
job.setGroupingComparatorClass(GroupComparator.class);


参考： http://blog.sina.com.cn/s/blog_7581a4c30102veem.html
reduce阶段
1. shuffle阶段
reducer开始fetch所有映射到这个reducer的map输出。

2.1 sort阶段
再次调用job.setSortComparatorClass()设置的key比较函数类对所有数据对排序(因为一个reducer接受多个mappers，需要重新排序)。
2.2 secondary sort阶段
然后开始构造一个key对应的value迭代器。这时就要用到分组，使用jobjob.setGroupingComparatorClass()设置的分组函数类。
只要这个比较器比较的两个key相同，他们就属于同一个组，它们的value放在一个value迭代器，
而这个迭代器的key使用属于同一个组的所有key的第一个key。