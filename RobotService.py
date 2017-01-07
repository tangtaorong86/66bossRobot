# -*- coding:utf-8 -*-

"""
机器人服务
点赞 点踩 评论等
- 机器人 user_id 缓存

服务运行 ：
初始化 机器人 队列
    循环：
        定时执行：
            查询文章
            根据文章发布时间 生成一个随机的 操作间隔时间 (几十个账号同时操作 谁信啊？)
            随机抽取文章所有人的机器人粉丝 加入到操作者
            根据间隔时间 生成一个Article优先队列
            循环：
                sleep到队列第一个元素的操作时间 唤醒操作(每次只操作一个操作者)  如果操作者不为空  就修改操作时间后 再次加入到队列


"""
import MySQLdb
from MySQLdb.cursors import DictCursor
import time
import Queue
from Article import Article
import  random
import logging


logging.basicConfig(level=logging.DEBUG)


class RobotService(object):

    conn = MySQLdb.connect('127.0.0.1', 'root', 'root', 'gmian', charset='utf8')
    cursor = conn.cursor(cursorclass=DictCursor)

    # Interval_time = 72  #72 小时之前的 不在进行操作
    Interval_time = 7200
    ArticleQueue = Queue.PriorityQueue()
    currentArticles = {}

    def __init__(self):
        self.robots = {}
        self.setRobots()


    def setRobots(self):
        """
         设置机器人
        :return:
        """
        sql = 'SELECT * FROM gm_users WHERE real_name="robots" order by rand()'
        self.cursor.execute(sql)
        for userInfo in self.cursor.fetchall():
            self.robots[userInfo['user_id']] = userInfo
        logging.debug(self.robots)
        logging.debug(u"初始化机器人")


    @classmethod
    def getFans(cls,supplier_id):
        """
        获取这个商家的粉丝 不是粉丝不能操作
        :param supplier_id:
        :return:
        """
        sql = 'SELECT link.user_id,link.tribe_id,t.supplier_id FROM abc_tribe_user_rel AS link LEFT JOIN abc_tribe AS' \
              ' t ON t.tribe_id=link.tribe_id LEFT JOIN gm_users AS u ON u.user_id=link.user_id WHERE t.supplier_id=%d AND u.real_name="robots"' % supplier_id
        cls.cursor.execute(sql)
        return cls.cursor.fetchall()

    @staticmethod
    def getTime(addtime):
        """
        根据文章的添加时间 获取随机 操作时间 和 操作频率
        一小时 一天内 两天内 三天内    时间段？
        :param addtime: 添加时间
        :return:
        """
        interval = int(time.time())  - addtime
        if( interval < 3600):  # 一小时之内发布的    10秒 - 10分钟 之内 进行一次操作
            return  int(time.time()) + random.randint(30, 600)
        elif( interval < 86400): # 一天以内的 10分钟 - 30分钟之内进行一次操作
            return int(time.time()) + random.randint(600, 1800)
        elif(interval < 172800): # 两天以内的 20分钟 - 一小时之内进行一次操作
            return int(time.time()) + random.randint(1200, 3600)
        else:
            return int(time.time()) + random.randint(2400, 3600)

    @classmethod
    def getCurrentArticleIds(cls):
        """
        获取 当前正在操作的 文章ID 避免多次查询/生成操作
        :return:
        """
        ids = []
        for key,value in cls.currentArticles.items():
            if key+cls.Interval_time*3600 <= int(time.time()):
                cls.currentArticles.pop(key) # 操作规定的时间 根本就查不到 不用统计ID了 删除
            else:
                ids.extend(value)
        return ids

    @classmethod
    def articles(cls):
        """
        获取文章
        :return:
        """
        # yield  article
        ids = ','.join([str(s) for s in cls.getCurrentArticleIds()])
        logging.debug(cls.currentArticles)
        logging.debug(u"查询排除：%s" % ids)
        where = ''
        if ids:
            where = " AND id not in (%s)" %  ids
        sql = 'SELECT * FROM abc_user_article WHERE add_time >= %d %s' % (int(time.time()) - cls.Interval_time * 3600 , where)
        cls.cursor.execute(sql)
        return cls.cursor.fetchall()

    @classmethod
    def setArticleQueue(cls):
        """
        生成文章对象 队列
        :return:
        """
        i = j = 0;
        articleIds = []
        for article in cls.articles():
            i+=1;
            articleIds.append(article['id'])
            operator = {'praise':[],'tread':[],'message':[]}
            users = cls.getFans(article['supplier_id'])
            act = 0
            for user in users:
                r = random.randint(0, 200)
                if r <= 100: # 百分之五十的用户是要操作的
                    if r == 0:  #  百分之一的操作用户 是会踩的
                        operator['tread'].append(user['user_id'])
                    elif r <= 5: # 百分之5的操作用户是要留言的
                        operator['message'].append(user['user_id'])
                    else: # 剩下的都是要点赞的
                        operator['praise'].append(user['user_id'])
                    act += 1
            if act:
                nextTime = cls.getTime(article['add_time'])
                logging.debug(u"Article[%d] 共有%d个机器人粉丝 有%d个会进行操作 下次操作时间: %s " \
                              % (article['id'],len(users),act, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nextTime))))
                cls.ArticleQueue.put(Article(article,nextTime , operator,act))
                j += 1
        cls.currentArticles[int(time.time())] = articleIds
        logging.debug(u"共查到%d篇文章 其中%d篇有机器人粉丝并加入到操作队列" % (i,j))

    def supply(self):
        """
        定时去 生成 数据
        :return:
        """
        while True:
            self.setArticleQueue()
            logging.debug(u"数据生成过了 歇会")
            time.sleep(200)

    @classmethod
    def action(cls):
        while not cls.ArticleQueue.empty():
            articleobj = cls.ArticleQueue.get()
            sleepTime = articleobj.article['add_time'] - int(time.time())
            if sleepTime > 0:
                logging.debug(u"Article[%d] 再等%s秒后操作 " % (articleobj.article['id'],sleepTime))
                time.sleep(sleepTime)
            if articleobj.RandomOperation():
                t = cls.getTime(articleobj.article['add_time'])
                articleobj.randomTime = t
                logging.debug(u"Article[%d] 重新加入队列 下次操作时间是 %s" % (articleobj.article['id'],time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))))
                cls.ArticleQueue.put(articleobj)
            else:
                logging.debug(u"Article[%d] 操作完毕退出" % articleobj.article['id'])
        return True

    def run(self):
        """
        数据生成不及时 挂起一段时间 再继续
        :return:
        """
        while self.action():
            logging.debug(u"没数据啦 歇会" )
            time.sleep(100)

if __name__ == '__main__':
    import threading

    service = RobotService()
    threading.Thread(target=service.supply, args=()).start()
    # threading.Thread(target=service.supply, args=()).start()
    # threading.Thread(target=service.run, args=()).start()
    # threading.Thread(target=service.run, args=()).start()
    threading.Thread(target=service.run, args=()).start()
    threading.Thread(target=service.run, args=()).start()

