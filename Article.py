# -*- coding:utf-8 -*-

"""
社群文章类
abc_user_article
abc_praise_despise
abc_comments
"""

import MySQLdb
from MySQLdb.cursors import DictCursor
import time
import logging
import random

class Article(object):

    conn = MySQLdb.connect('127.0.0.1', 'root', 'root', 'gmian', charset='utf8')
    cursor = conn.cursor(cursorclass=DictCursor)

    def __init__(self,article,randomTime,operator,actCount):
        """
        初始化文章对象
        :param article: 文章详情
        :param randomTime: 操作时间 int
        :param operator: 操作者 {'praise':[UserID,..],'tread':[UserID,..],'message':[UserID,..]}
        :param actCount: 操作次数

        """
        self.article = article
        self.randomTime = randomTime
        self.operator = operator
        self.actCount = actCount

    def __cmp__(self, other):
        return cmp(self.randomTime, other.randomTime)

    @classmethod
    def discuss(cls,id,uid,author_id,content,actCount):
        """
        社群文章 评论
        :param id:  文章ID
        :return:
        """
        sql = 'INSERT INTO abc_comments (uid_from,id_to,uid_to,content,add_time) value (%s,%s,%s,"%s",%s)' % (uid,id,author_id,content,int(time.time()))
        affect = cls.cursor.execute(sql)
        cls.conn.commit()
        logging.debug(u"为Article[%d]添加了一个留言 还剩%d次操作" % (id,actCount))
        return bool(affect)

    @classmethod
    def LikeDislike(cls,id,uid,author_id,value,actCount):
        """
        社群文章 点赞 踩
        :param id:  文章ID
        :param uid : 评价者ID
        :param author_id : 文章作者ID
        :param value: 1赞 0 踩
        :return:
        """
        sql = 'INSERT INTO abc_praise_despise (uid_from,id_to,uid_to,id_value,add_time) value (%s,%s,%s,"%s",%s)' % (
        uid, id, author_id, value, int(time.time()))
        affect = cls.cursor.execute(sql)
        cls.conn.commit()
        values = [u'踩',u'赞']
        logging.debug(u"为Article[%d]添加了一个%s 还剩%d次操作" % (id,values[value],actCount))
        return bool(affect)


    def getMessage(self):
        """
        获取留言信息
        :return:
        """
        return '?'

    def RandomOperation(self):
        """
        随机进行一个操作
        :return:
        """
        if self.actCount < 1:
            return False
        if not self.operator['praise'] and not self.operator['tread'] and not self.operator['message']:
            return False
            # raise Exception(u'都没有人了 让谁操作去？')
        arr = ['praise','praise','praise','praise','praise','praise','praise','tread','tread','message','message','message']
        while True:
            text =  random.choice(arr)
            # if not text:
            #     self.actCount = 0
            #     break
            if not self.operator[text]:
                arr = filter(lambda x: x != text, arr)
            else:
                if text == 'praise':
                    self.LikeDislike(self.article['id'],self.operator[text].pop(),self.article['uid'],1,self.actCount)
                elif text == 'tread':
                    self.LikeDislike(self.article['id'],self.operator[text].pop(),self.article['uid'],0,self.actCount)
                elif text == 'message':
                    self.discuss(self.article['id'],self.operator[text].pop(),self.article['uid'],self.getMessage(),self.actCount)
                self.actCount -= 1 #减少操作次数
                break
        return self.actCount > 0


if __name__ == '__main__':
    pass
    # a = Article('haha3', 3, {'praise':[1,2,3],'tread':[0],'message':[4]})
    # while True:
    #     a.RandomOperation()
    # import Queue
    # a = Queue.PriorityQueue()
    # a.put(Article('haha3',3,'xx'))
    # a.put(Article('haha',1,'x2'))
    # a.put(Article('haha2',2,'x3'))
    # print a.get().randomTime
    # print a.get().randomTime
    # print a.get().randomTime
    # print type(a.get()),'%%'

    # for i in  sorted([,,]):
    #     print i.randomTime